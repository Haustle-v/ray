// Author: He Su
// Email: yefengshuo.yfs@oceanbase.com
// Create Time: 2025-11-26

#pragma once

#include <mysql/mysql.h>

#include <functional>
#include <memory>
#include <queue>
#include <string>

#include "absl/synchronization/mutex.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/status.h"
#include "ray/core_worker/task_execution/thread_pool.h"


namespace ray {
namespace gcs {

struct MysqlStmtDeleter {
  void operator()(MYSQL_STMT* stmt) const {
        mysql_stmt_close(stmt);
  }
};


constexpr size_t kInitialColumnBufferSize = 4096;  // 4 KB
constexpr std::string_view kRayGcsTableNameInOB = "RAY_GCS";
constexpr std::string_view kOBKeySeparator = "@";

struct OBClientOptions {
  std::string server = "6.12.235.70";
  int port = 2881;
  std::string username = "root@sys";
  std::string password = "YdgmkMzQHygaaU325S84";
  std::string database = "test";
  int connection_pool_size = 12;
  int thread_pool_size = 10;
};

/// Result of an OB operation.
struct OBResult {
  Status status;
  int64_t affected_rows = 0;
  std::vector<std::vector<std::string>> rows;  // Query results

  OBResult() : status(Status::OK()) {}
  OBResult(Status s) : status(std::move(s)) {}
};

using OBCallback = std::function<void(std::shared_ptr<OBResult>)>;


struct OBKey {
  std::string external_storage_namespace;
  std::string table_name;

  std::string TablePrefix() const {
    return absl::StrCat("RAY", external_storage_namespace, kOBKeySeparator, table_name);
  }

  std::string KeyPrefix() const { return absl::StrCat(TablePrefix(), kOBKeySeparator); }

  std::string ComposeFullKey(const std::string &key) const {
    return absl::StrCat(TablePrefix(), key);
  }

  // for AsyncGetKeys
  std::string ComposePrefix(const std::string &key_prefix) const {
    return absl::StrCat(TablePrefix(), key_prefix);
  }

};

// Typed key for concurrency control.
struct OBConcurrencyKey {
  std::string table_name;
  std::string key;

  template <typename H>
  friend H AbslHashValue(H h, const OBConcurrencyKey &k) {
    return H::combine(std::move(h), k.table_name, k.key);
  }
  bool operator==(const OBConcurrencyKey &other) const {
    return table_name == other.table_name && key == other.key;
  }
};

/// Context for managing OceanBase/MySQL connections and executing async operations.
class OBContext {
 public:
  explicit OBContext(instrumented_io_context &io_service);

  ~OBContext();

  /// Initialize the connection pool.
  ///
  /// \param options Connection options.
  /// \return Status indicating success or failure.
  Status Initialize(const OBClientOptions &options);

  /// Execute an async SQL operation.
  ///
  /// \param sql SQL statement (can contain placeholders).
  /// \param bind_params Parameters to bind (empty if no placeholders).
  /// \param callback Callback to invoke with the result.
  void ExecuteAsync(
      const std::string &sql,
      const std::vector<std::pair<std::string, int>> &bind_params,  // (value, type)
      OBCallback callback);

  /// Check if the connection pool is healthy.
  ///
  /// \return Status indicating health.
  Status CheckHealth();

  /// Get the io_service reference.
  instrumented_io_context &io_service() { return io_service_; }

 private:
  /// Create a new MySQL connection.
  ///
  /// \return MYSQL* connection or nullptr on failure.
  MYSQL *CreateConnection();

  /// Acquire a connection from the pool.
  ///
  /// \return MYSQL* connection or nullptr if pool is empty and creation fails.
  MYSQL *AcquireConnection();

  /// Release a connection back to the pool.
  ///
  /// \param conn Connection to release.
  void ReleaseConnection(MYSQL *conn);

  /// Execute a synchronous SQL operation.
  ///
  /// \param conn MySQL connection.
  /// \param sql SQL statement.
  /// \param bind_params Parameters to bind.
  /// \return OBResult with the operation result.
  std::shared_ptr<OBResult> ExecuteSync(
      MYSQL *conn,
      const std::string &sql,
      const std::vector<std::pair<std::string, int>> &bind_params);

  /// Validate and reconnect a connection if needed.
  ///
  /// \param conn Connection to validate.
  /// \return true if connection is valid, false otherwise.
  bool ValidateConnection(MYSQL *conn);

  instrumented_io_context &io_service_;
  OBClientOptions options_;
  std::unique_ptr<core::BoundedExecutor> thread_pool_;

  absl::Mutex pool_mutex_;
  std::queue<MYSQL *> connection_pool_ ABSL_GUARDED_BY(pool_mutex_);
  bool initialized_ ABSL_GUARDED_BY(pool_mutex_) = false;
};

}  // namespace gcs
}  // namespace ray

