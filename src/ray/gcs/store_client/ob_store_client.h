// Author: He Su
// Email: yefengshuo.yfs@oceanbase.com
// Create Time: 2025-11-26

#pragma once

#include <gtest/gtest_prod.h>

#include <memory>
#include <queue>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/synchronization/mutex.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/asio/postable.h"
#include "ray/common/gcs_callback_types.h"
#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/gcs/store_client/ob_context.h"
#include "ray/gcs/store_client/store_client.h"

namespace ray {
namespace gcs {

inline std::ostream &operator<<(std::ostream &os, const OBConcurrencyKey &key) {
  os << "{" << key.table_name << ", " << key.key << "}";
  return os;
}

struct OBCommand {
  std::string table_name;
  std::string sql;
  std::vector<std::string> bind_params;
  bool is_select;
};

// StoreClient using OceanBase/MySQL as persistence backend.
//
// Schema:
// - A single table `RAY_GCS` stores all entries.
// - Column layout: k VARBINARY(65535) PRIMARY KEY, v MEDIUMBLOB.
// - The actual logical table name is encoded into the key prefix.
//
// Consistency:
// - All Put/Get/Delete operations to a same (table, key) pair are serialized.
// - For MultiGet/BatchDelete operations, they are subject to *all* keys in the operation.
class OBStoreClient : public StoreClient {
 public:
  /// Connect to OceanBase. Not thread safe.
  ///
  /// \param io_service The event loop for this client. Must be single threaded.
  /// \param options The options for connecting to OceanBase.
  explicit OBStoreClient(instrumented_io_context &io_service,
                         const OBClientOptions &options);

  void AsyncPut(const std::string &table_name,
                const std::string &key,
                std::string data,
                bool overwrite,
                Postable<void(bool)> callback) override;

  void AsyncGet(const std::string &table_name,
                const std::string &key,
                ToPostable<OptionalItemCallback<std::string>> callback) override;

  void AsyncGetAll(
      const std::string &table_name,
      Postable<void(absl::flat_hash_map<std::string, std::string>)> callback) override;

  void AsyncMultiGet(
      const std::string &table_name,
      const std::vector<std::string> &keys,
      Postable<void(absl::flat_hash_map<std::string, std::string>)> callback) override;

  void AsyncDelete(const std::string &table_name,
                   const std::string &key,
                   Postable<void(bool)> callback) override;

  void AsyncBatchDelete(const std::string &table_name,
                        const std::vector<std::string> &keys,
                        Postable<void(int64_t)> callback) override;

  void AsyncGetNextJobID(Postable<void(int)> callback) override;

  void AsyncGetKeys(const std::string &table_name,
                    const std::string &prefix,
                    Postable<void(std::vector<std::string>)> callback) override;

  void AsyncExists(const std::string &table_name,
                   const std::string &key,
                   Postable<void(bool)> callback) override;

  // Check if OceanBase is available.
  //
  // \param callback The callback that will be called with a Status. OK means healthy.
  void AsyncCheckHealth(Postable<void(Status)> callback);

 private:
  /// Escape LIKE pattern special characters.
  ///
  /// \param pattern The pattern to escape.
  /// \return Escaped pattern.
  std::string EscapeLikePattern(const std::string &pattern) const;

  /// Push a request to the sending queue.
  ///
  /// \param keys The keys impacted by the request.
  /// \param send_request The request to send.
  /// \return The number of queues newly added.
  size_t PushToSendingQueue(const std::vector<OBConcurrencyKey> &keys,
                            const std::function<void()> &send_request)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  /// Take requests from the sending queue.
  ///
  /// \param keys The keys to check for next request.
  /// \return The requests to send.
  std::vector<std::function<void()>> TakeRequestsFromSendingQueue(
      const std::vector<OBConcurrencyKey> &keys) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  /// Send OB command with concurrency control.
  ///
  /// \param keys Used as concurrency key.
  /// \param command OB command to send.
  /// \param callback Callback invoked with the execution result.
  void SendOBCmdWithKeys(std::vector<std::string> keys,
                         OBCommand command,
                         OBCallback callback);

  instrumented_io_context &io_service_;
  OBClientOptions options_;
  std::string external_storage_namespace_;
  std::shared_ptr<OBContext> ob_context_;

  absl::Mutex mu_;
  absl::flat_hash_map<OBConcurrencyKey, std::queue<std::function<void()>>>
      pending_ob_request_by_key_ ABSL_GUARDED_BY(mu_);
  absl::Mutex job_counter_status_mu_;
  bool job_counter_inserted_ = false;
  FRIEND_TEST(OBStoreClientTest, Random);
};

}  // namespace gcs
}  // namespace ray
