package org.apache.hadoop.hive.rdbms.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import org.apache.hadoop.hive.rdbms.ConfigurationUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBOperation {

  private static final Logger log = LoggerFactory.getLogger(DBOperation.class);

  private DBOperation() {
  }

  public static boolean isTableExist(String tableName, Connection connection) throws SQLException {
    // This return all tables, we use this because it is not db specific, Passing table name doesn't
    // work with every database
    ResultSet tables = connection.getMetaData().getTables(null, null, "%", null);
    while (tables.next()) {
      if (tables.getString(3).equalsIgnoreCase(tableName)) {
        return true;
      }
    }
    tables.close();
    return false;
  }

  public static void createTableIfNotExist(Map<String, String> tableParameters) throws Exception {
    String inputTable = tableParameters.get(DBConfiguration.INPUT_TABLE_NAME_PROPERTY);
    String outputTable = tableParameters.get(DBConfiguration.OUTPUT_TABLE_NAME_PROPERTY);
    String createTableQuery = tableParameters.get(ConfigurationUtils.HIVE_JDBC_TABLE_CREATE_QUERY);
    /**
     * If inputTable=null, then most probably it should be a output table.
     * In input table table must already exist.
     */
    if (inputTable == null && (outputTable != null || createTableQuery != null)) {
      DatabaseProperties dbProperties = new DatabaseProperties();
      dbProperties.setTableName(outputTable);
      dbProperties.setUserName(tableParameters.get(DBConfiguration.USERNAME_PROPERTY));
      dbProperties.setPassword(tableParameters.get(DBConfiguration.PASSWORD_PROPERTY));
      dbProperties.setConnectionUrl(tableParameters.get(DBConfiguration.URL_PROPERTY));
      dbProperties.setDriverClass(tableParameters.get(DBConfiguration.DRIVER_CLASS_PROPERTY));

      if (dbProperties.getTableName() == null) {
        if (log.isDebugEnabled()) {
          log.debug("Extracting Table name from sql query");
        }
        String tableName = ConfigurationUtils.extractingTableNameFromQuery(createTableQuery);
        dbProperties.setTableName(tableName);
      }

      if (dbProperties.getConnectionUrl() == null) {
        throw new IllegalArgumentException("connection url is missing");
      }

      Connection connection = createConnection(dbProperties);
      Statement statement = null;
      try {
        if (!isTableExist(dbProperties.getTableName(), connection)) {
          if (log.isDebugEnabled()) {
            log.debug("Creating table " + dbProperties.getTableName());
          }
          statement = connection.createStatement();
          statement.executeUpdate(createTableQuery);
        }
      } finally {
        closeStatement(statement, log);
        closeConnection(connection, log);
      }
    }
  }

  public static int getTotalCount(String sql, Connection connection) throws SQLException {
    ResultSet resultSet = null;
    PreparedStatement statement = null;
    int noOfRows = 0;
    try {
      statement = connection.prepareStatement(sql);
      resultSet = statement.executeQuery();
      if (resultSet.next()) {
        noOfRows = resultSet.getInt(1);
      } else {
        throw new SQLException("Can't get total rows count using sql " + sql);
      }
    } catch (SQLException e) {
      log.error("Failed to get total row count", e);
    } finally {
      closeResultSet(resultSet, log);
      closeStatement(statement, log);
      closeConnection(connection, log);
    }
    return noOfRows;
  }

  public static void runSQLQueryBeforeDataInsert(Map<String, String> tableParameters)
      throws Exception {
    String sqlQueryBeforeDataInsert = tableParameters
        .get(ConfigurationUtils.HIVE_JDBC_OUTPUT_SQL_QUERY_BEFORE_DATA_INSERT);
    if (sqlQueryBeforeDataInsert != null) {
      runSQLQuery(tableParameters, sqlQueryBeforeDataInsert);
    }
  }

  public static boolean runSQLQuery(Map<String, String> tableParameters, String query)
      throws Exception {
    DatabaseProperties dbProperties = new DatabaseProperties();
    dbProperties.setUserName(tableParameters.get(DBConfiguration.USERNAME_PROPERTY));
    dbProperties.setPassword(tableParameters.get(DBConfiguration.PASSWORD_PROPERTY));
    dbProperties.setConnectionUrl(tableParameters.get(DBConfiguration.URL_PROPERTY));
    dbProperties.setDriverClass(tableParameters.get(DBConfiguration.DRIVER_CLASS_PROPERTY));

    if (dbProperties.getConnectionUrl() == null) {
      throw new IllegalArgumentException("connection url is missing");
    }

    Connection connection = createConnection(dbProperties);
    Statement statement = null;
    try {
      statement = connection.createStatement();
      return statement.execute(query);
    } finally {
      closeStatement(statement, log);
      closeConnection(connection, log);
    }
  }

  public static Connection createConnection(DatabaseProperties dbProperties) throws Exception {
    Class.forName(dbProperties.getDriverClass());
    return DriverManager.getConnection(dbProperties.getConnectionUrl(), dbProperties.getUserName(),
        dbProperties.getPassword());
  }

  public static Connection createConnection(JobConf conf) throws Exception {
    DatabaseProperties dbProperties = ConfigurationUtils.getDbPropertiesObj(conf);
    return createConnection(dbProperties);
  }

  public static void closeConnection(Connection connection, Logger log) {
    if (connection != null) {
      try {
        connection.close();
      } catch (SQLException e) {
        log.error("Failed to close the connection", e);
      }
    }
  }

  public static void closeStatement(Statement statement, Logger log) {
    if (statement != null) {
      try {
        statement.close();
      } catch (SQLException e) {
        log.error("Failed to close the statement", e);
      }
    }
  }

  public static void closeResultSet(ResultSet resultSet, Logger log) {
    if (resultSet != null) {
      try {
        resultSet.close();
      } catch (SQLException e) {
        log.error("Failed to close the result set", e);
      }
    }
  }
}
