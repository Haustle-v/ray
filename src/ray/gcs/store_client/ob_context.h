// Author: He Su
// Email: yefengshuo.yfs@oceanbase.com
// Create Time: 2025-12-3

#pragma once

#include <functional>
#include <memory>
#include <queue>
#include <string>

#include <mysql-cppconn/jdbc/mysql_driver.h>
#include <mysql-cppconn/jdbc/cppconn/connection.h>
#include <mysql-cppconn/jdbc/cppconn/exception.h>
#include <mysql-cppconn/jdbc/cppconn/prepared_statement.h>
#include <mysql-cppconn/jdbc/cppconn/resultset.h>
#include <mysql-cppconn/jdbc/cppconn/resultset_metadata.h>
#include <mysql-cppconn/jdbc/cppconn/statement.h>

#include "absl/synchronization/mutex.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/status.h"
#include "ray/common/asio/io_service_pool.h"


namespace ray {
namespace gcs {

constexpr std::string_view kRayGcsTableNameInOB = "RAY_GCS";
constexpr std::string_view kOBKeySeparator = "@";

struct OBClientOptions {
  std::string server;
  int port;
  std::string username;
  std::string password;
  std::string database;
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
  /// \param is_select Whether the SQL expects a result set.
  /// \param callback Callback to invoke with the result.
  void ExecuteAsync(
      const std::string &sql,
      const std::vector<std::string> &bind_params,
      bool is_select,
      OBCallback callback);

  /// Get the io_service reference.
  instrumented_io_context &io_service() { return io_service_; }

 private:
  /// Create a new MySQL connection.
  ///
  /// \return sql::Connection* connection or nullptr on failure.
  sql::Connection *CreateConnection();

  /// Acquire a connection from the pool.
  ///
  /// \return sql::Connection* connection or nullptr if pool is empty and creation fails.
  sql::Connection *AcquireConnection();

  /// Release a connection back to the pool.
  ///
  /// \param conn Connection to release.
  void ReleaseConnection(sql::Connection *conn);

  /// Execute a synchronous SQL operation.
  ///
  /// \param conn MySQL connection.
  /// \param sql SQL statement.
  /// \param bind_params Parameters to bind.
  /// \param is_select Whether the SQL expects a result set.
  /// \return OBResult with the operation result.
  std::shared_ptr<OBResult> ExecuteSync(
      sql::Connection *conn,
      const std::string &sql,
      const std::vector<std::string> &bind_params,
      bool is_select);


  instrumented_io_context &io_service_;
  OBClientOptions options_;
  sql::ConnectOptionsMap conn_opts_;
  std::unique_ptr<IOServicePool> io_service_pool_;
  sql::Driver *driver_ = nullptr;

  absl::Mutex pool_mutex_;
  std::queue<sql::Connection *> connection_pool_ ABSL_GUARDED_BY(pool_mutex_);
  bool initialized_ ABSL_GUARDED_BY(pool_mutex_) = false;
};

}  // namespace gcs
}  // namespace ray

