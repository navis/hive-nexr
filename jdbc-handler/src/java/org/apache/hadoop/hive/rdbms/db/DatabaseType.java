package org.apache.hadoop.hive.rdbms.db;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import org.apache.hadoop.hive.rdbms.UnsupportedDatabaseException;

public enum DatabaseType {
  H2, ORACLE, SYBASE, POSTGRESQL, PHOENIX, SQLSERVER,
  MYSQL { public char quotation() { return '`'; }
  };

  public char quotation() {
    return '"';
  }

  public StringBuilder quote(StringBuilder query, String entity) {
    query.append(quotation());
    query.append(entity);
    query.append(quotation());
    return query;
  }

  public StringBuilder quote(StringBuilder query, String... entities) {
    for (int i = 0; i < entities.length; i++) {
      if (i > 0) {
        query.append(", ");
      }
      query.append(quotation());
      query.append(entities[i]);
      query.append(quotation());
    }
    return query;
  }

  public static DatabaseType getDatabaseType(DatabaseProperties dbProperties, Connection connection)
      throws UnsupportedDatabaseException {
    if (dbProperties.getConnectionUrl() != null) {
      String databaseName = dbProperties.getConnectionUrl().split(":")[1];
      return getDatabaseType(databaseName);
    }
    return getDatabaseType(connection);
  }

  private static DatabaseType getDatabaseType(Connection connection)
      throws UnsupportedDatabaseException {
    String connectionUrl;
    try {
      DatabaseMetaData metaData = connection.getMetaData();
      connectionUrl = metaData.getURL();
    } catch (SQLException e) {
      throw new UnsupportedDatabaseException("Failed to get connection url from Database", e);
    }
    String[] splits = connectionUrl.split(":");
    String databaseName = splits[1];
    if (databaseName.equals("jtds")) {
      databaseName = splits[2];   // sqlserver or sybase
    }
    return getDatabaseType(databaseName);
  }

  private static DatabaseType getDatabaseType(String databaseName)
      throws UnsupportedDatabaseException {
    DatabaseType databaseType;
    if (databaseName.equalsIgnoreCase("mysql")) {
      databaseType = MYSQL;
    } else if (databaseName.equalsIgnoreCase("sqlserver")) {
      databaseType = SQLSERVER;
    } else if (databaseName.equalsIgnoreCase("oracle")) {
      databaseType = ORACLE;
    } else if (databaseName.equalsIgnoreCase("h2")) {
      databaseType = H2;
    } else if (databaseName.equalsIgnoreCase("postgresql")) {
      databaseType = POSTGRESQL;
    } else if (databaseName.equalsIgnoreCase("sybase")) {
      databaseType = SYBASE;
    } else if (databaseName.equalsIgnoreCase("phoenix")) {
      databaseType = PHOENIX;
    } else {
      throw new UnsupportedDatabaseException("Your database type " + databaseName + " " +
          "is not supported by hive jdbc-handler to fetch results");
    }
    return databaseType;
  }
}
