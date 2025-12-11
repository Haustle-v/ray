// Author: He Su
// Email: yefengshuo.yfs@oceanbase.com
// Create Time: 2025-12-3

#include "ray/gcs/store_client/ob_context.h"

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
  RAY_LOG(INFO) << "Successfully initialize MySQL driver instance: " << driver_;
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

  RAY_LOG(INFO) << "Drop table if exists " << kRayGcsTableNameInOB;
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
                                        " (k VARBINARY(16384) NOT NULL PRIMARY KEY, "
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
    if (!conn->isValid()) {
      delete conn;
      return Status::IOError("Initial database connection is invalid " +
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
    conn_opts_["hostName"] = absl::StrCat(options_.server, ":", options_.port);
    conn_opts_["userName"] = options_.username;
    conn_opts_["password"] = options_.password;
    // Disable OTel in libmysqlcppconn to avoid symbol conflicts with OTel in Ray.
    conn_opts_["OPT_OPENTELEMETRY"] = sql::OTEL_DISABLED;
    sql::Connection* conn = driver_->connect(conn_opts_);
    conn->setSchema(options_.database);
    return conn;
  } catch (const sql::SQLException &e) {
    RAY_LOG(ERROR) << "Connection failed: " << e.what()
                   << " (code=" << e.getErrorCode()
                   << ", state=" << e.getSQLState() << ")";
    return nullptr;
  }
}

sql::Connection *OBContext::AcquireConnection() {
  absl::MutexLock lock(&pool_mutex_);

  while (!connection_pool_.empty()) {
    sql::Connection *conn = connection_pool_.front();
    connection_pool_.pop();

    if (!conn->isValid()) {
      conn->reconnect();
    } 
      
    if (conn->isValid()) {
      return conn;
    } else {
      delete conn;
    }
  }

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
    const std::vector<std::string> &bind_params,
    bool is_select) {
  auto result = std::make_shared<OBResult>();

  if (!conn) {
    RAY_LOG(ERROR) << "No connection available";
    result->status = Status::IOError("No connection available");
    return result;
  }

  try {
    RAY_LOG(INFO) << "Executing SQL: " << sql;
    std::unique_ptr<sql::PreparedStatement> stmt(conn->prepareStatement(sql));
    RAY_LOG(INFO) << "Binding params: " << absl::StrJoin(bind_params, ", ");
    for (size_t i = 0; i < bind_params.size(); ++i) {
      const auto &value = bind_params[i];
      stmt->setString(static_cast<int>(i + 1),
                      sql::SQLString(value.data(), value.size()));
    }

    RAY_LOG(INFO) << "Executing statement...";
    if (!is_select) {
      result->affected_rows = stmt->executeUpdate();
      RAY_LOG(INFO) << "Statement executed as update, affected_rows="
                    << result->affected_rows;
      return result;
    }

    RAY_LOG(INFO) << "Executing query and fetching ResultSet";
    std::unique_ptr<sql::ResultSet> res(stmt->executeQuery());
    RAY_LOG(INFO) << "ResultSet got";
    if (!res) {
      RAY_LOG(WARNING) << "Statement executed with no ResultSet, return";
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
    bool is_select,
    OBCallback callback) {
  thread_pool_->Post([this,
                      sql,
                      bind_params,
                      is_select,
                      callback = std::move(callback)]() mutable {
    RAY_LOG(INFO) << "Acquiring connection";
    sql::Connection *conn = AcquireConnection();
    RAY_LOG(INFO) << "Connection acquired";
    auto result = ExecuteSync(conn, sql, bind_params, is_select);
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

