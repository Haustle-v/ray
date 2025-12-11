// Author: He Su
// Email: yefengshuo.yfs@oceanbase.com
// Create Time: 2025-12-5

#include "ray/gcs/store_client/ob_context.h"

#include <atomic>
#include <memory>
#include <string>
#include <vector>

#include <boost/asio/executor_work_guard.hpp>
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

OBClientOptions LoadOptions() {
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
 public:
  OBContextTest() : work_guard_(boost::asio::make_work_guard(io_service)) {}

 protected:
  void SetUp() override {
    opts_ = LoadOptions();
  }

  void TearDown() override {
    work_guard_.reset();
    io_service.stop();
  }

  OBClientOptions opts_;
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work_guard_;
};


TEST_F(OBContextTest, ExecuteAsyncCRUD) {
  OBContext ctx(io_service);
  RAY_LOG(INFO) << "Init OBContext host=" << opts_.server << " port=" << opts_.port
                << " user=" << opts_.username << " db=" << opts_.database;
  auto init_status = ctx.Initialize(opts_);
  RAY_LOG(INFO) << "OBContext Initialize status: " << init_status.ToString();
  ASSERT_TRUE(init_status.ok());

  const std::string key = absl::StrCat("test-key-", current_time_ms());
  const std::string val_insert = "v1";
  const std::string val_update = "v2";
  const std::string val_replace = "v3";

  std::atomic<int> pending(0);

  auto done = [&]() {
    if (--pending == 0) {
      io_service.stop();
    }
  };

  // Insert
  pending++;
  ctx.ExecuteAsync(
      absl::StrCat("INSERT INTO ", kRayGcsTableNameInOB, " (k, v) VALUES (?, ?)"),
      {key, val_insert},
      false,
      [done, &key, &val_insert](std::shared_ptr<OBResult> res) {
        ASSERT_TRUE(res) << "res null";
        ASSERT_TRUE(res->status.ok()) << res->status.ToString();
        ASSERT_EQ(res->affected_rows, 1) << "affected_rows should be 1 in the insert sql";
        RAY_LOG(INFO) << "[Verified] Insert key=" << key << " val=" << val_insert
                      << " affected_rows=" << res->affected_rows;
        done();
      });
  RAY_LOG(INFO) << "Insert task submitted to io_service";

  // Read after insert
  pending++;
  ctx.ExecuteAsync(
      absl::StrCat("SELECT v FROM ", kRayGcsTableNameInOB, " WHERE k = ?"),
      {key},
      true, 
      [done, &key, &val_insert](std::shared_ptr<OBResult> res) {
        ASSERT_TRUE(res) << "res null";
        ASSERT_TRUE(res->status.ok()) << res->status.ToString();
        ASSERT_FALSE(res->rows.empty()) << "select after insert returned empty rows";
        ASSERT_EQ(res->rows[0].at(0), val_insert) << "select after insert got wrong value";
        RAY_LOG(INFO) << "[Verified] Read after insert key=" << key
                      << " rows=" << res->rows.size()
                      << " first_val=" << (res->rows.empty() ? "" : res->rows[0].at(0));
        done();
      });
  RAY_LOG(INFO) << "Read-after-insert task submitted to io_service";


  // Update
  RAY_LOG(INFO) << "Update key=" << key << " val=" << val_update;
  pending++;
  ctx.ExecuteAsync(
      absl::StrCat("UPDATE ", kRayGcsTableNameInOB, " SET v = ? WHERE k = ?"),
      {val_update, key},
      false,
      [done, &key, &val_update](std::shared_ptr<OBResult> res) {
        ASSERT_TRUE(res) << "res null";
        ASSERT_TRUE(res->status.ok()) << res->status.ToString();
        ASSERT_EQ(res->affected_rows, 1) << "affected_rows should be 1 in the update sql";
        RAY_LOG(INFO) << "[Verified] Update key=" << key << " val=" << val_update
                      << " affected_rows=" << res->affected_rows;
        done();
      });
  RAY_LOG(INFO) << "Update task submitted to io_service";

  // Read after update
  pending++;
  ctx.ExecuteAsync(
      absl::StrCat("SELECT v FROM ", kRayGcsTableNameInOB, " WHERE k = ?"),
      {key},
      true,
      [done, &key, &val_update](std::shared_ptr<OBResult> res) {
        ASSERT_TRUE(res) << "res null";
        ASSERT_TRUE(res->status.ok()) << res->status.ToString();
        ASSERT_FALSE(res->rows.empty()) << "select after update returned empty rows";
        ASSERT_EQ(res->rows[0].at(0), val_update) << "select after update got wrong value";
        RAY_LOG(INFO) << "[Verified] Read after update key=" << key
                      << " rows=" << res->rows.size()
                      << " first_val=" << (res->rows.empty() ? "" : res->rows[0].at(0));
        done();
      });
  RAY_LOG(INFO) << "Read-after-update task submitted to io_service";

  // Replace
  RAY_LOG(INFO) << "Replace key=" << key << " val=" << val_replace;
  pending++;
  ctx.ExecuteAsync(
      absl::StrCat("REPLACE INTO ", kRayGcsTableNameInOB, " (k, v) VALUES (?, ?)"),
      {key, val_replace},
      false,
      [done, &key, &val_replace](std::shared_ptr<OBResult> res) {
        ASSERT_TRUE(res) << "res null";
        ASSERT_TRUE(res->status.ok()) << res->status.ToString();
        ASSERT_EQ(res->affected_rows, 2) << "affected_rows should be 2 in the replace sql";
        RAY_LOG(INFO) << "[Verified] Replace key=" << key << " val=" << val_replace
                      << " affected_rows=" << res->affected_rows;
        done();
      });
  RAY_LOG(INFO) << "Replace task submitted to io_service";

  // Read after replace
  pending++;
  ctx.ExecuteAsync(
      absl::StrCat("SELECT v FROM ", kRayGcsTableNameInOB, " WHERE k = ?"),
      {key},
      true,
      [done, &key, &val_replace](std::shared_ptr<OBResult> res) {
        ASSERT_TRUE(res) << "res null";
        ASSERT_TRUE(res->status.ok()) << res->status.ToString();
        ASSERT_FALSE(res->rows.empty()) << "select after replace returned empty rows";
        ASSERT_EQ(res->rows[0].at(0), val_replace) << "select after replace got wrong value";
        RAY_LOG(INFO) << "[Verified] Read after replace key=" << key
                      << " rows=" << res->rows.size()
                      << " first_val=" << (res->rows.empty() ? "" : res->rows[0].at(0));
        done();
      });
  RAY_LOG(INFO) << "Read-after-replace task submitted to io_service";

  // Delete
  pending++;
  ctx.ExecuteAsync(
      absl::StrCat("DELETE FROM ", kRayGcsTableNameInOB, " WHERE k = ?"),
      {key},
      false,
      [done, &key](std::shared_ptr<OBResult> res) {
        ASSERT_TRUE(res) << "res null";
        ASSERT_TRUE(res->status.ok()) << res->status.ToString();
        ASSERT_EQ(res->affected_rows, 1) << "affected_rows should be 1 in the delete sql";
        RAY_LOG(INFO) << "[Verified] Delete key=" << key
                      << " affected_rows=" << res->affected_rows;
        done();
      });
  RAY_LOG(INFO) << "Delete task submitted to io_service";

  // Verify deleted
  pending++;
  ctx.ExecuteAsync(
      absl::StrCat("SELECT v FROM ", kRayGcsTableNameInOB, " WHERE k = ?"),
      {key},
      true,
      [done, &key](std::shared_ptr<OBResult> res) {
        ASSERT_TRUE(res) << "res null";
        ASSERT_TRUE(res->status.ok()) << res->status.ToString();
        ASSERT_TRUE(res->rows.empty()) << "select after delete should return empty rows";
        RAY_LOG(INFO) << "[Verified] Verify delete key=" << key
                      << " rows=" << res->rows.size();
        done();
      });
  RAY_LOG(INFO) << "Verify-delete task submitted to io_service";

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

