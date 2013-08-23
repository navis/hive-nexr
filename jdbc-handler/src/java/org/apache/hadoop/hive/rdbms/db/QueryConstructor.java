package org.apache.hadoop.hive.rdbms.db;

import org.apache.hadoop.hive.rdbms.JDBCSplit;

public class QueryConstructor {

  public static String constructInsertQuery(DatabaseProperties dbProperties) {
    DatabaseType type = dbProperties.getDatabaseType();
    String tableName = dbProperties.getTableName();
    String[] fieldNames = dbProperties.getFieldNames();

    StringBuilder query = new StringBuilder();
    query.append("INSERT INTO ");
    query.append(tableName);

    query.append(" (");
    type.quote(query, fieldNames);
    query.append(")");
    query.append(" VALUES (");

    for (int i = 0; i < fieldNames.length; i++) {
      query.append("?");
      if (i != fieldNames.length - 1) {
        query.append(",");
      }
    }
    query.append(")");

    return query.toString();
  }

  public String constructCountQuery(DatabaseProperties dbProperties) {
    StringBuilder query = new StringBuilder();
    query.append("SELECT COUNT(*) FROM ");
    query.append(dbProperties.getTableName());
    return query.toString();
  }

  public static String constructSelectQueryForReading(DatabaseProperties dbProperties,
      JDBCSplit split, int limit) {
    DatabaseType databaseType = dbProperties.getDatabaseType();
    boolean selectAll = split.getStart() < 0 && split.getEnd() < 0;

    String query = null;
    switch (databaseType) {

      case MYSQL:
      case H2:
      case POSTGRESQL:
        query = selectAll ? getQueryForMySql(dbProperties, limit) : getQueryForMySql(dbProperties, split);
        break;
      case ORACLE:
      case TIBERO:
        query = selectAll ? getQueryForOracle(dbProperties, limit) : getQueryForOracle(dbProperties, split);
        break;
      case SQLSERVER:
        query = selectAll ? getQueryForMsSql(dbProperties, limit) : getQueryForMsSql(dbProperties, split);
        break;
      case SYBASE:
        query = selectAll ? getQueryForSybase(dbProperties, limit) : getQueryForSybase(dbProperties, split);
        break;
      case PHOENIX:
        query = getQueryForSelectAll(dbProperties, limit);
        break;
    }
    return query;
  }

  private static String getQueryForMsSql(DatabaseProperties dbProperties, int limit) {
    DatabaseType type = dbProperties.getDatabaseType();
    String tableName = dbProperties.getTableName();
    String[] fieldNames = dbProperties.getFieldNames();

    StringBuilder query = new StringBuilder();
    query.append("SELECT ");
    if (limit > 0) {
      query.append("TOP ");
      query.append(limit);
      query.append(' ');
    }
    type.quote(query, fieldNames);
    query.append(" FROM ");
    query.append(tableName);
    return query.toString();
  }

  /**
   * SELECT * FROM (SELECT *, ROW_NUMBER() OVER (ORDER BY column_name) AS Row FROM table_name) AS tmp WHERE Row > 10
   * AND Row < 20
   */
  private static String getQueryForMsSql(DatabaseProperties dbProperties, JDBCSplit split) {
    DatabaseType type = dbProperties.getDatabaseType();
    String tableName = dbProperties.getTableName();
    String[] fieldNames = dbProperties.getFieldNames();

    StringBuilder query = new StringBuilder();
    query.append("SELECT ");
    type.quote(query, fieldNames);

    query.append(" FROM (");
    query.append("SELECT *, ROW_NUMBER() OVER (ORDER BY ");
    type.quote(query, fieldNames[0]);
    query.append(")");
    query.append(" AS Row FROM ");
    query.append(tableName);
    query.append(") AS tmp WHERE Row >=");
    query.append(split.getStart()).append(" AND Row <");
    query.append(split.getEnd());
        /*
         * query.append("SELECT TOP ").append(split.getLength()).append(" * FROM ( ");
         * query.append("SELECT TOP ").append(split.getEnd()).append(" "); for (int i = 0; i < fieldNames.length; i++) {
         * query.append(fieldNames[i]); if (i != fieldNames.length - 1) { query.append(", "); } }
         * query.append(" FROM "); query.append(dbProperties.getTableName());
         * query.append(" ORDER BY ").append(fieldNames[0]).append(" ASC ").append(")");
         * query.append(" ORDER BY ").append(fieldNames[0]).append(" DESC ").append(")");
         * query.append(" ORDER BY ").append(fieldNames[0]).append(" ASC");
         */
    return query.toString();
  }

  private static String getQueryForMySql(DatabaseProperties dbProperties, int limit) {
    DatabaseType type = dbProperties.getDatabaseType();
    String tableName = dbProperties.getTableName();
    String[] fieldNames = dbProperties.getFieldNames();

    StringBuilder query = new StringBuilder();
    query.append("SELECT ");
    type.quote(query, fieldNames);
    query.append(" FROM ");
    query.append(tableName);
    if (limit > 0) {
      query.append(" LIMIT ");
      query.append(limit);
    }
    return query.toString();
  }

  private static String getQueryForMySql(DatabaseProperties dbProperties, JDBCSplit split) {
    DatabaseType type = dbProperties.getDatabaseType();
    String tableName = dbProperties.getTableName();
    String[] fieldNames = dbProperties.getFieldNames();

    StringBuilder query = new StringBuilder();
    query.append("SELECT ");
    type.quote(query, fieldNames);
    query.append(" FROM ");
    query.append(tableName);
        /* query.append(" AS ").append(dbProperties.getTableName()); //in hsqldb this is necessary */

    query.append(" LIMIT ").append(split.getLength());
    query.append(" OFFSET ").append(split.getStart());
    return query.toString();
  }

  private static String getQueryForOracle(DatabaseProperties dbProperties, int limit) {
    DatabaseType type = dbProperties.getDatabaseType();
    String tableName = dbProperties.getTableName();
    String[] fieldNames = dbProperties.getFieldNames();

    StringBuilder query = new StringBuilder();
    query.append("SELECT ");
    type.quote(query, fieldNames);
    query.append(" FROM ");
    query.append(tableName);
    if (limit > 0) {
      query.append(" WHERE ROWNUM <= ");
      query.append(limit);
    }
    return query.toString();
  }

  /**
   * sample query is like this - SELECT * FROM ( SELECT ROW_NUMBER() OVER(ORDER BY column1) LINENUM, column1, column2
   * FROM MyTable ORDER BY column1 ) WHERE LINENUM BETWEEN 100 AND 200;
   */
  private static String getQueryForOracle(DatabaseProperties dbProperties, JDBCSplit split) {
    DatabaseType type = dbProperties.getDatabaseType();
    String tableName = dbProperties.getTableName();
    String[] fieldNames = dbProperties.getFieldNames();

    StringBuilder query = new StringBuilder();
    query.append("SELECT ");
    type.quote(query, fieldNames);
    query.append(" FROM ( SELECT ROW_NUMBER() OVER( ORDER BY ");
    type.quote(query, fieldNames[0]);
    query.append(" ) LINENUM, ");
    type.quote(query, fieldNames);
    query.append(" FROM ");
    query.append(tableName);
    query.append(" ORDER BY ");
    type.quote(query, fieldNames[0]);
    query.append(" )");
    query.append(" WHERE LINENUM > ");
    query.append(split.getStart());
    query.append(" AND LINENUM <= ");
    query.append(split.getEnd());
    return query.toString();
  }

  private static String getQueryForSybase(DatabaseProperties dbProperties, int limit) {
    DatabaseType type = dbProperties.getDatabaseType();
    String tableName = dbProperties.getTableName();
    String[] fieldNames = dbProperties.getFieldNames();

    StringBuilder query = new StringBuilder();
    query.append("SELECT ");
    if (limit > 0) {
      query.append("TOP ");
      query.append(limit);
      query.append(' ');
    }
    type.quote(query, fieldNames);
    query.append(" FROM ");
    query.append(tableName);
    query.append(" ORDER BY ");
    type.quote(query, fieldNames[0]);
    return query.toString();
  }

  private static String getQueryForSybase(DatabaseProperties dbProperties, JDBCSplit split) {
    DatabaseType type = dbProperties.getDatabaseType();
    String tableName = dbProperties.getTableName();
    String[] fieldNames = dbProperties.getFieldNames();

    StringBuilder query = new StringBuilder();
    query.append("SELECT ");
    type.quote(query, fieldNames);
    query.append(" FROM ");
    query.append(tableName);
    query.append(" WHERE ROWID(");
    query.append(tableName);
    query.append(") >= ");
    query.append(split.getStart() + 1L);
    query.append(" AND ");
    query.append("ROWID(");
    query.append(tableName);
    query.append(") < ");
    query.append(split.getEnd() + 1L);
    return query.toString();
  }

  private static String getQueryForSelectAll(DatabaseProperties dbProperties, int limit) {
    DatabaseType type = dbProperties.getDatabaseType();
    String tableName = dbProperties.getTableName();
    String[] fieldNames = dbProperties.getFieldNames();

    StringBuilder query = new StringBuilder();
    query.append("SELECT");
    query.append(' ');
    type.quote(query, fieldNames);
    query.append(" FROM ");
    query.append(tableName);
    if (limit > 0) {
      query.append(" LIMIT ");
      query.append(limit);
    }
    return query.toString();
  }
}
