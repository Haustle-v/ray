// Author: He Su
// Email: yefengshuo.yfs@oceanbase.com
// Create Time: 2025-11-26

#include "ray/gcs/store_client/ob_context_MysqlC.h"

#include <mysql/mysql.h>

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "ray/util/logging.h"
#include "absl/strings/str_cat.h"

namespace ray {
namespace gcs {

OBContext::OBContext(instrumented_io_context &io_service) : io_service_(io_service) {
  // Initialize MySQL library
  if (mysql_library_init(0, nullptr, nullptr) != 0) {
    RAY_LOG(FATAL) << "Failed to initialize MySQL library";
  }
}

OBContext::~OBContext() {
  // Stop thread pool
  if (thread_pool_) {
    thread_pool_->Stop();
    thread_pool_->Join();
  }

  // Close all connections
  {
    absl::MutexLock lock(&pool_mutex_);
    while (!connection_pool_.empty()) {
      MYSQL *conn = connection_pool_.front();
      connection_pool_.pop();
      if (conn) {
        mysql_close(conn);
      }
    }
  }

  if (initialized_) {
    MYSQL *cleanup_conn = CreateConnection();
    if (cleanup_conn) {
      std::string drop_sql =
          absl::StrCat("DROP TABLE IF EXISTS ", kRayGcsTableNameInOB);
      if (mysql_query(cleanup_conn, drop_sql.c_str()) != 0) {
        RAY_LOG(WARNING) << "Failed to drop table " << kRayGcsTableNameInOB << ": "
                         << mysql_error(cleanup_conn);
      }
      mysql_close(cleanup_conn);
    }
  }

  // Cleanup MySQL library
  mysql_library_end();
}

Status OBContext::Initialize(const OBClientOptions &options) {
  absl::MutexLock lock(&pool_mutex_);
  if (initialized_) {
    return Status::OK();
  }

  options_ = options;
  RAY_CHECK(!options_.server.empty()) << "OB server cannot be empty";
  RAY_CHECK(!options_.username.empty()) << "OB username cannot be empty";
  RAY_CHECK(!options_.password.empty()) << "OB password cannot be empty";
  RAY_CHECK(!options_.database.empty()) << "OB database cannot be empty";
  RAY_CHECK(options_.port > 0) << "OB port must be greater than 0";
  RAY_CHECK(options_.connection_pool_size > 0) << "OB connection pool size must be greater than 0";
  RAY_CHECK(options_.thread_pool_size > 0) << "OB thread pool size must be greater than 0";

  auto schema_conn = CreateConnection();
  if (!schema_conn) {
    RAY_LOG(ERROR) << "Failed to create database connection";
    return Status::IOError("Failed to create database connection");
  }

  auto exec_schema_sql = [schema_conn](const std::string &sql) -> Status {
    if (mysql_query(schema_conn, sql.c_str()) != 0) {
      mysql_close(schema_conn);
      return Status::IOError(mysql_error(schema_conn));
    }
    return Status::OK();
  };

  auto status = exec_schema_sql(
      absl::StrCat("DROP TABLE IF EXISTS ", kRayGcsTableNameInOB));
  if (!status.ok()) {
    RAY_LOG(ERROR) << "Failed to drop  table if exists " << kRayGcsTableNameInOB << ": "
                   << status.ToString();
    return status;
  }

  status = exec_schema_sql(absl::StrCat(
      "CREATE TABLE ",
      kRayGcsTableNameInOB,
      " (k VARBINARY(65535) NOT NULL PRIMARY KEY, v MEDIUMBLOB)"));
  if (!status.ok()) {
    RAY_LOG(ERROR) << "Failed to create table " << kRayGcsTableNameInOB << ": "
                   << status.ToString();
    return status;
  }

  mysql_close(schema_conn);

  // Create initial database connections
  for (int i = 0; i < options_.connection_pool_size; ++i) {
    MYSQL *conn = CreateConnection();
    if (!conn) {
      return Status::IOError("Failed to create initial database connection " +
                             std::to_string(i));
    }
    connection_pool_.push(conn);
  }

  // Validate connections
  for (int i = 0; i < options_.connection_pool_size; ++i) {
    MYSQL *conn = connection_pool_.front();
    connection_pool_.pop();
    if (!ValidateConnection(conn)) {
      RAY_LOG(WARNING) << "Failed to validate database connection " << i;
      mysql_close(conn);
      conn = CreateConnection();
      if (!conn) {
        RAY_LOG(ERROR) << "Failed to create replacement database connection " << i;
        return Status::IOError("Failed to create replacement database connection " + std::to_string(i));
      }
      RAY_LOG(INFO) << "Created replacement database connection " << i;
    }
    connection_pool_.push(conn);
  }

  initialized_ = true;
  RAY_LOG(INFO) << "OBContext initialized with " << options_.connection_pool_size
                << " connections";
  
  // Create thread pool for async operations
  thread_pool_ = std::make_unique<core::BoundedExecutor>(
    options_.thread_pool_size, nullptr, boost::chrono::milliseconds(10000));
  RAY_LOG(INFO) << "OBContext thread pool created with " << options_.thread_pool_size
                << " threads";

  return Status::OK();
}

MYSQL *OBContext::CreateConnection() {
  MYSQL *conn = mysql_init(nullptr);
  if (!conn) {
    RAY_LOG(ERROR) << "mysql_init() failed";
    return nullptr;
  }

  // Set connection timeout
  unsigned int timeout = 10;
  mysql_options(conn, MYSQL_OPT_CONNECT_TIMEOUT, &timeout);
  mysql_options(conn, MYSQL_OPT_READ_TIMEOUT, &timeout);
  mysql_options(conn, MYSQL_OPT_WRITE_TIMEOUT, &timeout);

  // Connect to server
  if (!mysql_real_connect(conn,
                          options_.server.c_str(),
                          options_.username.c_str(),
                          options_.password.c_str(),
                          options_.database.c_str(),
                          options_.port,
                          nullptr,
                          0)) {
    RAY_LOG(ERROR) << "Connection failed: " << mysql_error(conn);
    mysql_close(conn);
    return nullptr;
  }

  // Set charset to utf8mb4
  mysql_set_character_set(conn, "utf8mb4");

  return conn;
}

bool OBContext::ValidateConnection(MYSQL *conn) {
  if (!conn) {
    return false;
  }
  // Use mysql_ping to check connection
  if (mysql_ping(conn) != 0) {
    return false;
  }
  return true;
}

MYSQL *OBContext::AcquireConnection() {
  absl::MutexLock lock(&pool_mutex_);

  while (!connection_pool_.empty()) {
    MYSQL *conn = connection_pool_.front();
    connection_pool_.pop();

    if (ValidateConnection(conn)) {
      return conn;
    }

    mysql_close(conn);
  }

  MYSQL *conn = CreateConnection();
  if (ValidateConnection(conn)) {
    return conn;
  }
  mysql_close(conn);
  
  RAY_LOG(ERROR) << "Database connection pool is unhealthy, please check the connection status.";
  return nullptr;
}

void OBContext::ReleaseConnection(MYSQL *conn) {
  if (!conn) {
    return;
  }

  absl::MutexLock lock(&pool_mutex_);
  connection_pool_.push(conn);
}

std::shared_ptr<OBResult> OBContext::ExecuteSync(
    MYSQL *conn,
    const std::string &sql,
    const std::vector<std::pair<std::string, int>> &bind_params) {
  auto result = std::make_shared<OBResult>();

  if (!conn) {
    RAY_LOG(ERROR) << "No connection available";
    result->status = Status::IOError("No connection available");
    return result;
  }

  std::unique_ptr<MYSQL_STMT, MysqlStmtDeleter> stmt(mysql_stmt_init(conn));

  if (!stmt) {
    RAY_LOG(ERROR) << "mysql_stmt_init() failed: " << mysql_error(conn);
    result->status = Status::IOError("mysql_stmt_init() failed: " +
                                     std::string(mysql_error(conn)));
    return result;
  }

  if (mysql_stmt_prepare(stmt.get(), sql.c_str(), sql.length()) != 0) {
    RAY_LOG(ERROR) << "mysql_stmt_prepare() failed: " << mysql_stmt_error(stmt.get());
    result->status = Status::IOError("mysql_stmt_prepare() failed: " +
                                     std::string(mysql_stmt_error(stmt.get())));
    return result;
  }


  if (!bind_params.empty()) {
    std::vector<MYSQL_BIND> binds(bind_params.size());
    memset(binds.data(), 0, sizeof(MYSQL_BIND) * binds.size());

    for (size_t i = 0; i < bind_params.size(); ++i) {
      const auto &[value, type] = bind_params[i];
      binds[i].buffer_type = static_cast<enum_field_types>(type);
      binds[i].buffer = const_cast<char *>(value.c_str());
      binds[i].buffer_length = value.length();
      binds[i].length = &binds[i].buffer_length;
    }

    if (mysql_stmt_bind_param(stmt.get(), binds.data()) != 0) {
      RAY_LOG(ERROR) << "mysql_stmt_bind_param() failed: " << mysql_stmt_error(stmt.get());
      result->status = Status::IOError("mysql_stmt_bind_param() failed: " +
                                       std::string(mysql_stmt_error(stmt.get())));
      return result;
    }
  }

  if (mysql_stmt_execute(stmt.get()) != 0) {
    RAY_LOG(ERROR) << "mysql_stmt_execute() failed: " << mysql_stmt_error(stmt.get());
    result->status = Status::IOError("mysql_stmt_execute() failed: " +
                                     std::string(mysql_stmt_error(stmt.get())));
    return result;
  }

  result->affected_rows = mysql_stmt_affected_rows(stmt.get());

  unsigned int num_fields = mysql_stmt_field_count(stmt.get());
  // If the statement does not return a result set, return immediately
  if (num_fields == 0) {
    return result;
  }

  // Otherwise, store the result set
  if (mysql_stmt_store_result(stmt.get()) != 0) {
    RAY_LOG(ERROR) << "mysql_stmt_store_result() failed: " << mysql_stmt_error(stmt.get());
      result->status = Status::IOError("mysql_stmt_store_result() failed: " +
                                      std::string(mysql_stmt_error(stmt.get())));
      return result;
  }

  std::vector<MYSQL_BIND> result_binds(num_fields);
  std::vector<std::vector<char>> buffers(num_fields);
  std::vector<unsigned long> lengths(num_fields);
  std::unique_ptr<bool[]> is_null(new bool[num_fields]());

  // Initialize the result buffers
  for (unsigned int i = 0; i < num_fields; ++i) {
    memset(&result_binds[i], 0, sizeof(MYSQL_BIND));

    buffers[i].resize(kInitialColumnBufferSize);
    // both columns in table "RAY_GCS" are binary data
    result_binds[i].buffer_type = MYSQL_TYPE_BLOB;
    result_binds[i].buffer = buffers[i].data();
    result_binds[i].buffer_length = buffers[i].size();
    result_binds[i].length = &lengths[i];
    result_binds[i].is_null = &is_null[i];
  }

  // Bind the result buffers to the statement
  if (mysql_stmt_bind_result(stmt.get(), result_binds.data()) != 0) {
    RAY_LOG(ERROR) << "mysql_stmt_bind_result() failed: " << mysql_stmt_error(stmt.get());
    result->status = Status::IOError("mysql_stmt_bind_result() failed: " +
                                     std::string(mysql_stmt_error(stmt.get())));
    return result;
  }

  // A lambda function to ensure the column buffer is large enough, if not, resize it.
  auto ensure_column_buffer = [&](unsigned int col_idx) -> bool {
    // If the column buffer is already large enough, return true
    if (lengths[col_idx] <= result_binds[col_idx].buffer_length) {
      return true;
    }
    // Otherwise, resize the column buffer
    buffers[col_idx].resize(lengths[col_idx]);
    result_binds[col_idx].buffer = buffers[col_idx].data();
    result_binds[col_idx].buffer_length = buffers[col_idx].size();
    // Fetch the oversized column data again after resizing the buffer
    if (mysql_stmt_fetch_column(stmt.get(), &result_binds[col_idx], col_idx, 0) != 0) {
      RAY_LOG(ERROR) << "mysql_stmt_fetch_column() failed: "
                     << mysql_stmt_error(stmt.get());
      result->status = Status::IOError("mysql_stmt_fetch_column() failed: " +
                                       std::string(mysql_stmt_error(stmt.get())));
      return false;
    }
    return true;
  };

  while (true) {
    // Fetch rows
    int fetch_status = mysql_stmt_fetch(stmt.get());
    if (fetch_status == MYSQL_NO_DATA) {
      break;
    }
    
    // The MYSQL_DATA_TRUNCATED will be dealt with in the ensure_column_buffer lambda function,
    // so it's not an error here.
    if (fetch_status != 0 && fetch_status != MYSQL_DATA_TRUNCATED) {
      RAY_LOG(ERROR) << "mysql_stmt_fetch() failed: " << mysql_stmt_error(stmt.get());
      result->status = Status::IOError("mysql_stmt_fetch() failed: " +
                                       std::string(mysql_stmt_error(stmt.get())));
      return result;
    }

    std::vector<std::string> row;
    for (unsigned int i = 0; i < num_fields; ++i) {
      // If the column is null, add an empty string to the row
      if (is_null[i]) {
        row.emplace_back();
      } else {
        // Ensure the column buffer is large enough
        if (!ensure_column_buffer(i)) {
          return result;
        }
        // Add the column data to the row
        row.emplace_back(buffers[i].data(), lengths[i]);
      }
    }
    result->rows.push_back(std::move(row));
  }

  return result;
}

void OBContext::ExecuteAsync(
    const std::string &sql,
    const std::vector<std::pair<std::string, int>> &bind_params,
    OBCallback callback) {
  thread_pool_->Post([this,
                      sql,
                      bind_params,
                      callback = std::move(callback)]() mutable {
    MYSQL *conn = AcquireConnection();
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

// TODO: 仿照redis_store_client.cc中的AsyncCheckHealth实现,并且回头要放在OBStoreCLient中
Status OBContext::CheckHealth() {
  absl::MutexLock lock(&pool_mutex_);
  if (!initialized_ || connection_pool_.empty()) {
    return Status::IOError("OBContext not initialized or no connections available");
  }

  // Test with a simple query
  MYSQL *conn = connection_pool_.front();
  if (!ValidateConnection(conn)) {
    return Status::IOError("Connection health check failed");
  }

  return Status::OK();
}

}  // namespace gcs
}  // namespace ray

