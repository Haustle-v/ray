// Author: He Su
// Email: yefengshuo.yfs@oceanbase.com
// Create Time: 2025-11-26

#include "ray/gcs/store_client/ob_store_client.h"

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "ray/common/ray_config.h"
#include "ray/util/container_util.h"
#include "ray/util/logging.h"

namespace ray {
namespace gcs {

OBStoreClient::OBStoreClient(instrumented_io_context &io_service,
                             const OBClientOptions &options)
    : io_service_(io_service),
      options_(options),
      external_storage_namespace_(::RayConfig::instance().external_storage_namespace()) {
  ob_context_ = std::make_shared<OBContext>(io_service);
  RAY_CHECK_OK(ob_context_->Initialize(options_)) << "Failed to initialize OBContext.";
}

void OBStoreClient::AsyncPut(const std::string &table_name,
                             const std::string &key,
                             std::string data,
                             bool overwrite,
                             Postable<void(bool)> callback) {
  OBKey ob_key{external_storage_namespace_, table_name};

  std::string sql;
  if (overwrite) {
    sql = absl::StrCat("REPLACE INTO ", kRayGcsTableNameInOB, " (k, v) VALUES (?, ?)");
  } else {
    sql = absl::StrCat(
        "INSERT IGNORE INTO ", kRayGcsTableNameInOB, " (k, v) VALUES (?, ?)");
  }

  std::vector<std::string> bind_params;
  bind_params.emplace_back(ob_key.ComposeFullKey(key));
  bind_params.emplace_back(std::move(data));

  OBCommand command{table_name, std::move(sql), std::move(bind_params), false};
  OBCallback ob_callback = [callback = std::move(callback)](
                               std::shared_ptr<OBResult> result) mutable {
    std::move(callback).Dispatch("OBStoreClient.AsyncPut", result->affected_rows == 1);
  };

  SendOBCmdWithKeys({key}, std::move(command), std::move(ob_callback));
}

void OBStoreClient::AsyncGet(const std::string &table_name,
                             const std::string &key,
                             ToPostable<OptionalItemCallback<std::string>> callback) {
  OBKey ob_key{external_storage_namespace_, table_name};
  std::string sql =
      absl::StrCat("SELECT v FROM ", kRayGcsTableNameInOB, " WHERE k = ? LIMIT 1");

  std::vector<std::string> bind_params;
  bind_params.emplace_back(ob_key.ComposeFullKey(key));

  OBCommand command{table_name, std::move(sql), std::move(bind_params), true};
  OBCallback ob_callback =
      [callback = std::move(callback)](std::shared_ptr<OBResult> result) mutable {
        std::optional<std::string> value;
        Status status = Status::OK();
        if (!result || !result->status.ok()) {
          status = result ? result->status : Status::IOError("Unknown OB error");
        } else if (!result->rows.empty()) {
          value = std::move(result->rows[0][0]);
        }
        std::move(callback).Dispatch("OBStoreClient.AsyncGet", status, std::move(value));
      };

  SendOBCmdWithKeys({key}, std::move(command), std::move(ob_callback));
}

void OBStoreClient::AsyncMultiGet(
    const std::string &table_name,
    const std::vector<std::string> &keys,
    Postable<void(absl::flat_hash_map<std::string, std::string>)> callback) {
  if (keys.empty()) {
    std::move(callback).Dispatch("OBStoreClient.AsyncMultiGet",
                                 absl::flat_hash_map<std::string, std::string>{});
    return;
  }

  OBKey ob_key{external_storage_namespace_, table_name};

  std::vector<std::string> placeholders(keys.size(), "?");
  std::string in_clause = absl::StrJoin(placeholders, ",");
  std::string sql = absl::StrCat(
      "SELECT k, v FROM ", kRayGcsTableNameInOB, " WHERE k IN (", in_clause, ")");

  std::vector<std::string> bind_params;
  bind_params.reserve(keys.size());
  for (const auto &single_key : keys) {
    bind_params.emplace_back(ob_key.ComposeFullKey(single_key));
  }

  std::vector<std::string> request_keys(keys.begin(), keys.end());
  OBCommand command{table_name, std::move(sql), std::move(bind_params), true};
  SendOBCmdWithKeys(std::move(request_keys),
                    std::move(command),
                    [callback = std::move(callback), table_prefix = ob_key.TablePrefix()](
                        std::shared_ptr<OBResult> result) mutable {
                      absl::flat_hash_map<std::string, std::string> key_value_map;
                      if (result && result->status.ok()) {
                        for (const auto &row : result->rows) {
                          if (row.size() >= 2 && absl::StartsWith(row[0], table_prefix)) {
                            key_value_map[row[0].substr(table_prefix.size())] = row[1];
                          }
                        }
                      }
                      std::move(callback).Dispatch("OBStoreClient.AsyncMultiGet",
                                                   std::move(key_value_map));
                    });
}

void OBStoreClient::AsyncDelete(const std::string &table_name,
                                const std::string &key,
                                Postable<void(bool)> callback) {
  AsyncBatchDelete(table_name, {key}, std::move(callback).TransformArg([](int64_t cnt) {
    return cnt > 0;
  }));
}

void OBStoreClient::AsyncBatchDelete(const std::string &table_name,
                                     const std::vector<std::string> &keys,
                                     Postable<void(int64_t)> callback) {
  if (keys.empty()) {
    std::move(callback).Dispatch("OBStoreClient.AsyncBatchDelete", 0);
    return;
  }

  OBKey ob_key{external_storage_namespace_, table_name};

  std::vector<std::string> placeholders(keys.size(), "?");
  std::string in_clause = absl::StrJoin(placeholders, ",");
  std::string sql =
      absl::StrCat("DELETE FROM ", kRayGcsTableNameInOB, " WHERE k IN (", in_clause, ")");

  std::vector<std::string> bind_params;
  bind_params.reserve(keys.size());
  for (const auto &single_key : keys) {
    bind_params.emplace_back(ob_key.ComposeFullKey(single_key));
  }

  std::vector<std::string> request_keys(keys.begin(), keys.end());
  OBCommand command{table_name, std::move(sql), std::move(bind_params), false};
  SendOBCmdWithKeys(
      std::move(request_keys),
      std::move(command),
      [callback = std::move(callback)](std::shared_ptr<OBResult> result) mutable {
        int64_t deleted = 0;
        if (result && result->status.ok()) {
          deleted = result->affected_rows;
        }
        std::move(callback).Dispatch("OBStoreClient.AsyncBatchDelete", deleted);
      });
}

void OBStoreClient::AsyncExists(const std::string &table_name,
                                const std::string &key,
                                Postable<void(bool)> callback) {
  OBKey ob_key{external_storage_namespace_, table_name};

  std::string sql =
      absl::StrCat("SELECT 1 FROM ", kRayGcsTableNameInOB, " WHERE k = ? LIMIT 1");

  std::vector<std::string> bind_params;
  bind_params.emplace_back(ob_key.ComposeFullKey(key));

  OBCommand command{table_name, std::move(sql), std::move(bind_params), true};
  OBCallback ob_callback =
      [callback = std::move(callback)](std::shared_ptr<OBResult> result) mutable {
        bool exists = result && result->status.ok() && !result->rows.empty();
        std::move(callback).Dispatch("OBStoreClient.AsyncExists", exists);
      };

  SendOBCmdWithKeys({key}, std::move(command), std::move(ob_callback));
}

void OBStoreClient::AsyncGetNextJobID(Postable<void(int)> callback) {
  std::string table_name = "JobCounter";
  std::string key = "counter";
  OBKey job_key{external_storage_namespace_, table_name};
  std::string k = job_key.ComposeFullKey(key);

  std::string insert_sql =
      absl::StrCat("INSERT IGNORE INTO ", kRayGcsTableNameInOB, " (k, v) VALUES (?, ?)");
  std::string update_sql = absl::StrCat(
      "UPDATE ", kRayGcsTableNameInOB, " SET v = CAST(v AS UNSIGNED) + 1 WHERE k = ?");
  std::string select_sql =
      absl::StrCat("SELECT v FROM ", kRayGcsTableNameInOB, " WHERE k = ? LIMIT 1");

  OBCallback insert_callback = [this](std::shared_ptr<OBResult> result) mutable {
    job_counter_inserted_ = true;
  };

  OBCallback update_callback = [this](std::shared_ptr<OBResult> result) mutable {};

  OBCallback select_callback =
      [callback = std::move(callback)](std::shared_ptr<OBResult> result) mutable {
        if (result && result->status.ok() && !result->rows.empty()) {
          int job_id = std::stoi(result->rows[0][0]);
          std::move(callback).Dispatch("OBStoreClient.AsyncGetNextJobID", job_id);
        } else {
          std::move(callback).Dispatch("OBStoreClient.AsyncGetNextJobID", -1);
        }
      };

  {
    // Lock to ensure the insert-update-select is atomic, similar to the "INCR BY" in Redis.
    absl::MutexLock status_lock(&job_counter_status_mu_);
    if (!job_counter_inserted_) {
      std::vector<std::string> insert_params;
      insert_params.emplace_back(k);
      insert_params.emplace_back("0");
      OBCommand insert_cmd{
          job_key.table_name, insert_sql, std::move(insert_params), false};
      SendOBCmdWithKeys({key}, std::move(insert_cmd), std::move(insert_callback));
    }

    std::vector<std::string> update_params;
    update_params.emplace_back(k);
    OBCommand update_cmd{
        job_key.table_name, update_sql, std::move(update_params), false};
    SendOBCmdWithKeys({key}, std::move(update_cmd), update_callback);

    std::vector<std::string> select_params;
    select_params.emplace_back(k);
    OBCommand select_cmd{
        job_key.table_name, select_sql, std::move(select_params), true};
    SendOBCmdWithKeys({key}, std::move(select_cmd), select_callback);
  }
}

std::string OBStoreClient::EscapeLikePattern(const std::string &pattern) const {
  // Escape LIKE special characters such as %, _, and backslash.
  std::string escaped_pattern;
  for (char c : pattern) {
    if (c == '%' || c == '_' || c == '\\') {
      escaped_pattern += '\\';
    }
    escaped_pattern += c;
  }
  return escaped_pattern;
}

void OBStoreClient::AsyncGetKeys(const std::string &table_name,
                                 const std::string &prefix,
                                 Postable<void(std::vector<std::string>)> callback) {
  OBKey ob_key{external_storage_namespace_, table_name};

  std::string escaped_prefix = EscapeLikePattern(prefix);
  std::string sql =
      absl::StrCat("SELECT k FROM ", kRayGcsTableNameInOB, " WHERE k LIKE ?");

  std::vector<std::string> bind_params;
  bind_params.emplace_back(
      absl::StrCat(ob_key.ComposePrefix(std::string(escaped_prefix)), "%"));

  ob_context_->ExecuteAsync(
      sql,
      std::move(bind_params),
      /*is_select=*/true,
      [callback = std::move(callback),
       table_prefix = ob_key.TablePrefix()](std::shared_ptr<OBResult> result) mutable {
        std::vector<std::string> keys;
        if (result && result->status.ok()) {
          for (const auto &row : result->rows) {
            if (!row.empty() && absl::StartsWith(row[0], table_prefix)) {
              keys.push_back(row[0].substr(table_prefix.size()));
            }
          }
        }
        std::move(callback).Dispatch("OBStoreClient.AsyncGetKeys", std::move(keys));
      });
}

void OBStoreClient::AsyncGetAll(
    const std::string &table_name,
    Postable<void(absl::flat_hash_map<std::string, std::string>)> callback) {
  OBKey ob_key{external_storage_namespace_, table_name};
  std::string sql =
      absl::StrCat("SELECT k, v FROM ", kRayGcsTableNameInOB, " WHERE k LIKE ?");

  std::vector<std::string> bind_params;
  bind_params.emplace_back(absl::StrCat(ob_key.TablePrefix(), "%"));

  ob_context_->ExecuteAsync(
      sql,
      std::move(bind_params),
      /*is_select=*/true,
      [callback = std::move(callback),
       table_prefix = ob_key.TablePrefix()](std::shared_ptr<OBResult> result) mutable {
        absl::flat_hash_map<std::string, std::string> key_value_map;
        if (result && result->status.ok()) {
          for (const auto &row : result->rows) {
            if (row.size() >= 2 && absl::StartsWith(row[0], table_prefix)) {
              key_value_map[row[0].substr(table_prefix.size())] = row[1];
            }
          }
        }
        std::move(callback).Dispatch("OBStoreClient.AsyncGetAll",
                                     std::move(key_value_map));
      });
}

size_t OBStoreClient::PushToSendingQueue(const std::vector<OBConcurrencyKey> &keys,
                                         const std::function<void()> &send_request) {
  // Newly added queues, meaning the send_request is at the front of the queue (ready to
  // go).
  size_t queue_added = 0;
  for (const auto &key : keys) {
    // If the key does not exist, 'added' is true, a new queue was created.
    // If the key already exists, 'added' is false and 'op_iter' points to the existing
    // queue.
    auto [op_iter, added] =
        pending_ob_request_by_key_.emplace(key, std::queue<std::function<void()>>());
    if (added) {
      queue_added++;
    }
    if (added) {
      // Optimization: use nullptr as placeholder if queue is new
      op_iter->second.push(nullptr);
    } else {
      op_iter->second.push(send_request);
    }
  }
  return queue_added;
}

std::vector<std::function<void()>> OBStoreClient::TakeRequestsFromSendingQueue(
    const std::vector<OBConcurrencyKey> &keys) {
  std::vector<std::function<void()>> requests_to_send;
  for (const auto &key : keys) {
    auto [op_iter, added] =
        pending_ob_request_by_key_.emplace(key, std::queue<std::function<void()>>());
    RAY_CHECK(added == false) << "Pop from a queue doesn't exist: " << key;
    RAY_CHECK(op_iter->second.front() == nullptr);
    op_iter->second.pop();
    if (op_iter->second.empty()) {
      pending_ob_request_by_key_.erase(op_iter);
    } else {
      requests_to_send.emplace_back(std::move(op_iter->second.front()));
    }
  }
  return requests_to_send;
}

void OBStoreClient::SendOBCmdWithKeys(std::vector<std::string> keys,
                                      OBCommand command,
                                      OBCallback callback) {
  RAY_CHECK(!keys.empty());
  auto concurrency_keys = ray::move_mapped(
      std::move(keys), [table_name = std::move(command.table_name)](std::string &&key) {
        return OBConcurrencyKey{table_name, std::move(key)};
      });

  // The number of keys that's ready for this request
  auto num_ready_keys = std::make_shared<size_t>(0);
  std::function<void()> send_ob = [this,
                                   num_ready_keys,
                                   concurrency_keys,
                                   command = std::move(command),
                                   ob_callback = std::move(callback)]() mutable {
    {
      absl::MutexLock lock(&mu_);
      *num_ready_keys += 1;
      RAY_LOG(INFO) << "Ready keys: " << *num_ready_keys
                    << " / All required keys: " << concurrency_keys.size();
      RAY_CHECK(*num_ready_keys <= concurrency_keys.size());
      if (*num_ready_keys != concurrency_keys.size()) {
        return;
      }
    }
    RAY_LOG(INFO) << *num_ready_keys << " / " << concurrency_keys.size()
                  << " keys are ready, send the request to OB.";
    ob_context_->ExecuteAsync(
        command.sql,
        std::move(command.bind_params),
        command.is_select,
        [this, concurrency_keys, ob_callback = std::move(ob_callback)](
            std::shared_ptr<OBResult> result) mutable {
          if (ob_callback) {
            ob_callback(std::move(result));
          }
          std::vector<std::function<void()>> requests_to_send;
          {
            absl::MutexLock lock(&mu_);
            requests_to_send = TakeRequestsFromSendingQueue(concurrency_keys);
          }
          for (auto &request : requests_to_send) {
            if (request) {
              request();
            }
          }
        });
  };

  {
    absl::MutexLock lock(&mu_);
    auto keys_ready = PushToSendingQueue(concurrency_keys, send_ob);
    *num_ready_keys += keys_ready;
    // If all queues are empty for each key this request depends on
    // we are safe to fire the request immediately.
    if (*num_ready_keys == concurrency_keys.size()) {
      // Set to size-1 so that when send_ob() increments it, it triggers execution.
      *num_ready_keys = concurrency_keys.size() - 1;
    } else {
      send_ob = nullptr;
    }
  }
  if (send_ob) {
    send_ob();
  }
}

void OBStoreClient::AsyncCheckHealth(Postable<void(Status)> callback) {
  // Use a simple SELECT 1 query to check database connectivity
  ob_context_->ExecuteAsync(
      "SELECT 1",
      /*bind_params=*/{},
      /*is_select=*/true,
      [callback = std::move(callback)](std::shared_ptr<OBResult> result) mutable {
        Status status = Status::OK();
        if (!result || !result->status.ok() || result->rows.empty()) {
          status = result ? result->status : Status::IOError("Unknown OB error");
        }
        std::move(callback).Dispatch("OBStoreClient.AsyncCheckHealth", status);
      });
}

}  // namespace gcs
}  // namespace ray
