// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "ray/gcs/store_client/ob_store_client.h"

#include <algorithm>
#include <atomic>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "absl/strings/str_cat.h"
#include "gtest/gtest.h"
#include "ray/common/test_utils.h"
#include "ray/gcs/store_client/tests/store_client_test_base.h"
#include "ray/util/logging.h"
#include "ray/util/path_utils.h"
#include "ray/util/raii.h"

namespace ray {
namespace gcs {

namespace {

// Keep pool sizes small to reduce connection pressure during tests.
OBClientOptions LoadOptions() {
  OBClientOptions opts;
  opts.server = "6.12.235.70";
  opts.port = 2881;
  opts.username = "root@sys";
  opts.password = "YdgmkMzQHygaaU325S84";
  opts.database = "test";
  opts.connection_pool_size = 1;
  opts.thread_pool_size = 1;
  return opts;
}

}  // namespace

class OBStoreClientTest : public StoreClientTestBase {
 public:
  OBStoreClientTest() {
    // Keep the dataset small to speed up OB integration tests.
    key_count_ = 200;
    index_count_ = 20;
    wait_pending_timeout_ = std::chrono::milliseconds(20000);
  }

  ~OBStoreClientTest() override = default;

  void SetUp() override {
    opts_ = LoadOptions();
    ASSERT_TRUE(opts_.has_value()) << "OB connection options are missing";
    StoreClientTestBase::SetUp();
  }

  void TearDown() override { StoreClientTestBase::TearDown(); }

  void InitStoreClient() override {
    auto &io_context = *io_service_pool_->Get();
    store_client_ = std::make_shared<OBStoreClient>(io_context, *opts_);
  }

 protected:
  std::optional<OBClientOptions> opts_;
};

TEST_F(OBStoreClientTest, AsyncPutAndAsyncGet) { TestAsyncPutAndAsyncGet(); }

TEST_F(OBStoreClientTest, AsyncGetAllAndBatchDelete) { TestAsyncGetAllAndBatchDelete(); }

TEST_F(OBStoreClientTest, AsyncMultiGet) {
  Put();

  std::vector<std::string> query_keys;
  const size_t take = std::min<size_t>(keys_.size(), 20);
  query_keys.reserve(take);
  for (size_t i = 0; i < take; ++i) {
    query_keys.push_back(keys_[i].Hex());
  }

  absl::flat_hash_map<std::string, std::string> expected;
  for (const auto &k : query_keys) {
    expected[k] = key_to_value_.at(ActorID::FromHex(k)).SerializeAsString();
  }

  std::atomic<int> pending(1);
  store_client_->AsyncMultiGet(
      table_name_,
      query_keys,
      {[&pending, &expected](absl::flat_hash_map<std::string, std::string> result) {
        ASSERT_EQ(result.size(), expected.size());
        for (const auto &kv : expected) {
          auto it = result.find(kv.first);
          ASSERT_NE(it, result.end());
          ASSERT_EQ(it->second, kv.second);
        }
        --pending;
      },
       *io_service_pool_->Get()});
  ASSERT_TRUE(WaitForCondition([&pending]() { return pending == 0; }, 20000));

  BatchDelete();
}

TEST_F(OBStoreClientTest, OverwriteSemantics) {
  const std::string key = "overwrite-key";
  const std::string v1 = "v1";
  const std::string v2 = "v2";

  std::atomic<int> pending(4);

  store_client_->AsyncPut(
      table_name_,
      key,
      v1,
      /*overwrite=*/false,
      {[&pending](bool inserted) {
        ASSERT_TRUE(inserted);
        --pending;
      },
       *io_service_pool_->Get()});

  store_client_->AsyncPut(
      table_name_,
      key,
      v2,
      /*overwrite=*/false,
      {[&pending](bool inserted) {
        ASSERT_FALSE(inserted);
        --pending;
      },
       *io_service_pool_->Get()});

  store_client_->AsyncGet(
      table_name_,
      key,
      {[&pending, v1](const Status &status, const std::optional<std::string> &result) {
        ASSERT_TRUE(status.ok());
        ASSERT_TRUE(result.has_value());
        ASSERT_EQ(*result, v1);
        --pending;
      },
       *io_service_pool_->Get()});

  store_client_->AsyncDelete(
      table_name_,
      key,
      {[&pending](bool) { --pending; }, *io_service_pool_->Get()});

  ASSERT_TRUE(WaitForCondition([&pending]() { return pending == 0; }, 20000));
}

TEST_F(OBStoreClientTest, GetNextJobIdMonotonic) {
  std::vector<int> results;
  std::atomic<int> pending(3);
  for (int i = 0; i < 3; ++i) {
    store_client_->AsyncGetNextJobID(
        {[&results, &pending](int job_id) {
          results.push_back(job_id);
          --pending;
        },
         *io_service_pool_->Get()});
  }
  ASSERT_TRUE(WaitForCondition([&pending]() { return pending == 0; }, 20000));
  ASSERT_EQ(results.size(), 3u);
  std::sort(results.begin(), results.end());
  for (size_t i = 1; i < results.size(); ++i) {
    ASSERT_GE(results[i], results[i - 1]);
    ASSERT_LE(results[i] - results[i - 1], 1);
  }
}

TEST_F(OBStoreClientTest, CheckHealth) {
  std::atomic<int> pending(1);
  store_client_->AsyncCheckHealth(
      {[&pending](Status status) {
        ASSERT_TRUE(status.ok()) << status.ToString();
        --pending;
      },
       *io_service_pool_->Get()});
  ASSERT_TRUE(WaitForCondition([&pending]() { return pending == 0; }, 20000));
}

}  // namespace gcs
}  // namespace ray

int main(int argc, char **argv) {
  InitShutdownRAII ray_log_shutdown_raii(
      ray::RayLog::StartRayLog,
      ray::RayLog::ShutDownRayLog,
      argv[0],
      ray::RayLogLevel::INFO,
      ray::GetLogFilepathFromDirectory(/*log_dir=*/"", /*app_name=*/argv[0]),
      ray::GetErrLogFilepathFromDirectory(/*log_dir=*/"", /*app_name=*/argv[0]),
      ray::RayLog::GetRayLogRotationMaxBytesOrDefault(),
      ray::RayLog::GetRayLogRotationBackupCountOrDefault());
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

