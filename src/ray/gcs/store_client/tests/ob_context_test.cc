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

#include "ray/gcs/store_client/ob_context.h"

#include <atomic>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "absl/strings/str_cat.h"
#include "gtest/gtest.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/test_utils.h"
#include "ray/util/logging.h"
#include "ray/util/raii.h"
#include "ray/util/time.h"
#include "ray/util/path_utils.h"

namespace ray {
namespace gcs {

namespace {

instrumented_io_context io_service;

std::optional<OBClientOptions> LoadOptions() {
  OBClientOptions opts;
  opts.server = "6.12.235.70";
  opts.port = 2881;
  opts.username = "root@sys";
  opts.password = "YdgmkMzQHygaaU325S84";
  opts.database = "test";
  // Keep pools minimal to reduce connection pressure during test.
  opts.connection_pool_size = 1;
  opts.thread_pool_size = 1;
  return opts;
}

}  // namespace

class OBContextTest : public ::testing::Test {
 protected:
  void SetUp() override {
    opts_ = LoadOptions();
    io_service.restart();
  }

  void TearDown() override {
    io_service.stop();
  }

  std::optional<OBClientOptions> opts_;
};


TEST_F(OBContextTest, ExecuteAsyncCrud) {
  OBContext ctx(io_service);
  RAY_LOG(INFO) << "Init OBContext host=" << opts_->server << " port=" << opts_->port
                << " user=" << opts_->username << " db=" << opts_->database;
  auto init_status = ctx.Initialize(*opts_);
  std::cerr << "[TEST] OBContext Initialize status: " << init_status.ToString() << std::endl;
  RAY_LOG(INFO) << "OBContext Initialize status: " << init_status.ToString();
  ASSERT_TRUE(init_status.ok());

  const std::string key = absl::StrCat("test-key-", current_time_ms());
  const std::string val = "v1";

  std::atomic<int> pending(0);

  auto done = [&]() {
    if (--pending == 0) {
      io_service.stop();
    }
  };

  // Insert / upsert
  pending++;
  ctx.ExecuteAsync(
      absl::StrCat("REPLACE INTO ", kRayGcsTableNameInOB, " (k, v) VALUES (?, ?)"),
      {key, val},
      [done](std::shared_ptr<OBResult> res) {
        ASSERT_TRUE(res) << "res null";
        ASSERT_TRUE(res->status.ok()) << res->status.ToString();
        ASSERT_EQ(res->affected_rows, 1);
        done();
      });

  // Read back
  pending++;
  ctx.ExecuteAsync(
      absl::StrCat("SELECT v FROM ", kRayGcsTableNameInOB, " WHERE k = ?"),
      {key},
      [done, &val](std::shared_ptr<OBResult> res) {
        ASSERT_TRUE(res) << "res null";
        ASSERT_TRUE(res->status.ok()) << res->status.ToString();
        ASSERT_FALSE(res->rows.empty());
        ASSERT_EQ(res->rows[0].at(0), val);
        done();
      });

  // Delete
  pending++;
  ctx.ExecuteAsync(
      absl::StrCat("DELETE FROM ", kRayGcsTableNameInOB, " WHERE k = ?"),
      {key},
      [done](std::shared_ptr<OBResult> res) {
        ASSERT_TRUE(res) << "res null";
        ASSERT_TRUE(res->status.ok()) << res->status.ToString();
        done();
      });

  // Run event loop until all callbacks finish.
  io_service.run();
  ASSERT_EQ(pending.load(), 0);
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

