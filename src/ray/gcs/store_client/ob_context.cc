// Author: He Su
// Email: yefengshuo.yfs@oceanbase.com
// Create Time: 2025-12-3

#include "ray/gcs/store_client/ob_context.h"

#include <mysql-cppconn/jdbc/mysql_driver.h>
#include <mysql-cppconn/jdbc/cppconn/exception.h>
#include <mysql-cppconn/jdbc/cppconn/prepared_statement.h>
#include <mysql-cppconn/jdbc/cppconn/resultset.h>
#include <mysql-cppconn/jdbc/cppconn/resultset_metadata.h>
#include <mysql-cppconn/jdbc/cppconn/statement.h>

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "ray/util/logging.h"
#include "absl/strings/str_cat.h"

namespace ray {
namespace gcs {

OBContext::OBContext(instrumented_io_context &io_service) : io_service_(io_service) {
  driver_ = sql::mysql::get_mysql_driver_instance();
  RAY_CHECK(driver_ != nullptr) << "Failed to get MySQL driver instance";
  RAY_LOG(INFO) << "Successfully initialize MySQL driver instance";
}

OBContext::~OBContext() {
  if (thread_pool_) {
    thread_pool_->Stop();
    thread_pool_->Join();
  }

  {
    absl::MutexLock lock(&pool_mutex_);
    while (!connection_pool_.empty()) {
      sql::Connection *conn = connection_pool_.front();
      connection_pool_.pop();
      delete conn;
    }
  }

  if (initialized_) {
    std::unique_ptr<sql::Connection> cleanup_conn(CreateConnection());
    if (cleanup_conn) {
      try {
        std::unique_ptr<sql::Statement> stmt(cleanup_conn->createStatement());
        stmt->execute(absl::StrCat("DROP TABLE IF EXISTS ", kRayGcsTableNameInOB));
      } catch (const sql::SQLException &e) {
        RAY_LOG(WARNING) << "Failed to drop table " << kRayGcsTableNameInOB << ": "
                         << e.what();
      }
    }
  }
}

Status OBContext::Initialize(const OBClientOptions &options) {
  RAY_LOG(INFO) << "Initializing OBContext...";
  absl::MutexLock lock(&pool_mutex_);
  if (initialized_) {
    return Status::OK();
  }

  options_ = options;

  RAY_LOG(INFO) << "Create database connection";
  std::unique_ptr<sql::Connection> schema_conn(CreateConnection());
  if (!schema_conn) {
    RAY_LOG(ERROR) << "Failed to create database connection";
    return Status::IOError("Failed to create database connection");
  }

  auto exec_schema_sql = [&schema_conn](const std::string &sql) -> Status {
    try {
      std::unique_ptr<sql::Statement> stmt(schema_conn->createStatement());
      stmt->execute(sql);
      return Status::OK();
    } catch (const sql::SQLException &e) {
      return Status::IOError(e.what());
    }
  };

  RAY_LOG(INFO) << "Drop table " << kRayGcsTableNameInOB;
  auto status =
      exec_schema_sql(absl::StrCat("DROP TABLE IF EXISTS ", kRayGcsTableNameInOB));
  if (!status.ok()) {
    RAY_LOG(ERROR) << "Failed to drop table " << kRayGcsTableNameInOB << ": "
                   << status.ToString();
    return status;
  }

  RAY_LOG(INFO) << "Create table " << kRayGcsTableNameInOB;
  status = exec_schema_sql(absl::StrCat("CREATE TABLE ",
                                        kRayGcsTableNameInOB,
                                        " (k VARBINARY(65535) NOT NULL PRIMARY KEY, "
                                        "v MEDIUMBLOB)"));
  if (!status.ok()) {
    RAY_LOG(ERROR) << "Failed to create table " << kRayGcsTableNameInOB << ": "
                   << status.ToString();
    return status;
  }

  for (int i = 0; i < options_.connection_pool_size; ++i) {
    sql::Connection *conn = CreateConnection();
    if (!conn) {
      return Status::IOError("Failed to create initial database connection " +
                             std::to_string(i));
    }
    if (!ValidateConnection(conn)) {
      delete conn;
      return Status::IOError("Failed to validate initial database connection " +
                             std::to_string(i));
    }
    connection_pool_.push(conn);
  }

  initialized_ = true;
  RAY_LOG(INFO) << "OBContext initialized with " << options_.connection_pool_size
                << " connections";

  thread_pool_ = std::make_unique<core::BoundedExecutor>(
      options_.thread_pool_size, nullptr, boost::chrono::milliseconds(10000));
  RAY_LOG(INFO) << "OBContext thread pool created with " << options_.thread_pool_size
                << " threads";

  return Status::OK();
}

sql::Connection *OBContext::CreateConnection() {
  if (!driver_) {
    driver_ = sql::mysql::get_mysql_driver_instance();
  }
  try {
    RAY_LOG(INFO) << "Creating database connection with uri=" << absl::StrCat("tcp://", options_.server, ":", options_.port);
    std::string uri = absl::StrCat(options_.server, ":", options_.port);
    sql::Connection *conn =
        driver_->connect(uri, options_.username, options_.password);
    conn->setSchema(options_.database);
    return conn;
  } catch (const sql::SQLException &e) {
    RAY_LOG(ERROR) << "Connection failed: " << e.what()
                   << " (code=" << e.getErrorCode()
                   << ", state=" << e.getSQLState() << ")";
    return nullptr;
  }
}

bool OBContext::ValidateConnection(sql::Connection *conn) {
  if (!conn) {
    return false;
  }

  try {
    if (conn->isClosed()) {
      conn->reconnect();
      conn->setSchema(options_.database);
    }

    std::unique_ptr<sql::Statement> stmt(conn->createStatement());
    stmt->execute("SELECT 1");
    return true;
  } catch (const sql::SQLException &e) {
    RAY_LOG(WARNING) << "Connection validation failed: " << e.what();
    return false;
  }
}

sql::Connection *OBContext::AcquireConnection() {
  absl::MutexLock lock(&pool_mutex_);

  while (!connection_pool_.empty()) {
    sql::Connection *conn = connection_pool_.front();
    connection_pool_.pop();

    if (ValidateConnection(conn)) {
      return conn;
    }

    delete conn;
  }

  sql::Connection *conn = CreateConnection();
  if (ValidateConnection(conn)) {
    return conn;
  }
  delete conn;

  RAY_LOG(ERROR)
      << "Database connection pool is unhealthy, please check the connection status.";
  return nullptr;
}

void OBContext::ReleaseConnection(sql::Connection *conn) {
  if (!conn) {
    return;
  }

  absl::MutexLock lock(&pool_mutex_);
  connection_pool_.push(conn);
}

std::shared_ptr<OBResult> OBContext::ExecuteSync(
    sql::Connection *conn,
    const std::string &sql,
    const std::vector<std::string> &bind_params) {
  auto result = std::make_shared<OBResult>();

  if (!conn) {
    RAY_LOG(ERROR) << "No connection available";
    result->status = Status::IOError("No connection available");
    return result;
  }

  try {
    std::unique_ptr<sql::PreparedStatement> stmt(conn->prepareStatement(sql));
    for (size_t i = 0; i < bind_params.size(); ++i) {
      const auto &value = bind_params[i];
      stmt->setString(static_cast<int>(i + 1),
                      sql::SQLString(value.data(), value.size()));
    }

    bool has_result = stmt->execute();
    int64_t affected = stmt->getUpdateCount();
    result->affected_rows = affected >= 0 ? affected : 0;

    if (!has_result) {
      return result;
    }

    std::unique_ptr<sql::ResultSet> res(stmt->getResultSet());
    if (!res) {
      return result;
    }

    sql::ResultSetMetaData *meta = res->getMetaData();
    int column_count = meta ? meta->getColumnCount() : 0;

    while (res->next()) {
      std::vector<std::string> row;
      row.reserve(column_count);
      for (int col = 1; col <= column_count; ++col) {
        if (res->isNull(col)) {
          row.emplace_back();
        } else {
          row.emplace_back(res->getString(col).asStdString());
        }
      }
      result->rows.push_back(std::move(row));
    }
  } catch (const sql::SQLException &e) {
    result->status = Status::IOError(
        absl::StrCat("MySQL error: ", e.what(), " (code=", e.getErrorCode(),
                     ", state=", e.getSQLState(), ")"));
  } catch (const std::exception &e) {
    result->status = Status::IOError(
        absl::StrCat("Standard exception: ", std::string(e.what())));
  } catch (...) {
    result->status = Status::IOError("Unknown exception in ExecuteSync");
  }

  return result;
}

void OBContext::ExecuteAsync(
    const std::string &sql,
    const std::vector<std::string> &bind_params,
    OBCallback callback) {
  thread_pool_->Post([this,
                      sql,
                      bind_params,
                      callback = std::move(callback)]() mutable {
    sql::Connection *conn = AcquireConnection();
    auto result = ExecuteSync(conn, sql, bind_params);
    ReleaseConnection(conn);

    // Post callback to io_service
    io_service_.post(
        [callback = std::move(callback), result = std::move(result)]() mutable {
          callback(std::move(result));
        },
        "OBContext.ExecuteAsync");
  });
}

}  // namespace gcs
}  // namespace ray

