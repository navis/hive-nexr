package org.apache.hadoop.hive.rdbms.db;

import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.rdbms.JDBCSplit;

public class QueryConstructor {

  public static String constructInsertQuery(DatabaseProperties dbProperties) {
    DatabaseType type = dbProperties.getDatabaseType();
    String tableName = dbProperties.getTableName();
    String[] fieldNames = dbProperties.getFieldNames();

    StringBuilder query = new StringBuilder();
    if (dbProperties.getDatabaseType() == DatabaseType.PHOENIX) {
      query.append("UPSERT INTO ");
    } else {
      query.append("INSERT INTO ");
    }
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

  public String constructCountQuery(DatabaseProperties dbProperties, ExprNodeDesc filter) {
    StringBuilder query = new StringBuilder();
    query.append("SELECT COUNT(*) FROM ");
    query.append(dbProperties.getTableName());
    if (filter != null) {
      query.append(" WHERE ");
      query.append(filter.getExprString());
    }
    return query.toString();
  }

  public static String constructSelectQueryForReading(DatabaseProperties dbProperties,
      JDBCSplit split, ExprNodeDesc filter, int limit) {
    DatabaseType databaseType = dbProperties.getDatabaseType();
    boolean selectAll = split.getStart() < 0 && split.getEnd() < 0;

    String query = null;
    switch (databaseType) {

      case MYSQL:
      case H2:
      case POSTGRESQL:
        query = selectAll ? getQueryForMySql(dbProperties, filter, limit)
            : getQueryForMySql(dbProperties, filter, split);
        break;
      case ORACLE:
      case TIBERO:
        query = selectAll ? getQueryForOracle(dbProperties, filter, limit)
            : getQueryForOracle(dbProperties, filter, split);
        break;
      case SQLSERVER:
        query = selectAll ? getQueryForMsSql(dbProperties, filter, limit)
            : getQueryForMsSql(dbProperties, filter, split);
        break;
      case SYBASE:
        query = selectAll ? getQueryForSybase(dbProperties, filter, limit)
            : getQueryForSybase(dbProperties, filter, split);
        break;
      case PHOENIX:
        query = getQueryForSelectAll(dbProperties, filter, limit);
        break;
    }
    return query;
  }

  private static String getQueryForMsSql(DatabaseProperties dbProperties,
      ExprNodeDesc filter, int limit) {
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

    if (filter != null) {
      query.append(" WHERE ");
      query.append(filter.getExprString());
    }
    return query.toString();
  }

  /**
   * SELECT * FROM (SELECT *, ROW_NUMBER() OVER (ORDER BY column_name) AS Row FROM table_name) AS tmp WHERE Row > 10
   * AND Row < 20
   */
  private static String getQueryForMsSql(DatabaseProperties dbProperties,
      ExprNodeDesc filter, JDBCSplit split) {
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
    if (filter != null) {
      query.append(" WHERE ");
      query.append(filter.getExprString());
    }

    query.append(") AS tmp WHERE Row >=");
    query.append(split.getStart()).append(" AND Row <");
    query.append(split.getEnd());

    return query.toString();
  }

  private static String getQueryForMySql(DatabaseProperties dbProperties,
      ExprNodeDesc filter, int limit) {
    DatabaseType type = dbProperties.getDatabaseType();
    String tableName = dbProperties.getTableName();
    String[] fieldNames = dbProperties.getFieldNames();

    StringBuilder query = new StringBuilder();
    query.append("SELECT ");
    type.quote(query, fieldNames);
    query.append(" FROM ");
    query.append(tableName);

    if (filter != null) {
      query.append(" WHERE ");
      query.append(filter.getExprString());
    }

    if (limit > 0) {
      query.append(" LIMIT ");
      query.append(limit);
    }
    return query.toString();
  }

  private static String getQueryForMySql(DatabaseProperties dbProperties,
      ExprNodeDesc filter, JDBCSplit split) {
    DatabaseType type = dbProperties.getDatabaseType();
    String tableName = dbProperties.getTableName();
    String[] fieldNames = dbProperties.getFieldNames();

    StringBuilder query = new StringBuilder();
    query.append("SELECT ");
    type.quote(query, fieldNames);
    query.append(" FROM ");
    query.append(tableName);

    if (filter != null) {
      query.append(" WHERE ");
      query.append(filter.getExprString());
    }

    query.append(" LIMIT ").append(split.getLength());
    query.append(" OFFSET ").append(split.getStart());
    return query.toString();
  }

  // S : S W<expr> : S W<ROWNUM> : S (S W<ROWNUM>)X W<ROWNUM>
  private static String getQueryForOracle(DatabaseProperties dbProperties,
      ExprNodeDesc filter, int limit) {
    DatabaseType type = dbProperties.getDatabaseType();
    String tableName = dbProperties.getTableName();
    String[] fieldNames = dbProperties.getFieldNames();

    StringBuilder query = new StringBuilder();
    if (limit > 0 && filter != null) {
      query.append("SELECT ");
      type.quote(query, fieldNames);
      query.append(" FROM ( ");
    }
    query.append("SELECT ");
    type.quote(query, fieldNames);
    query.append(" FROM ");
    query.append(tableName);

    if (filter != null) {
      query.append(" WHERE ");
      query.append(filter.getExprString());
    }

    if (limit > 0 && filter != null) {
      query.append(") X");
    }

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
  private static String getQueryForOracle(DatabaseProperties dbProperties,
      ExprNodeDesc filter, JDBCSplit split) {
    DatabaseType type = dbProperties.getDatabaseType();
    String tableName = dbProperties.getTableName();
    String[] fieldNames = dbProperties.getFieldNames();

    StringBuilder query = new StringBuilder();
    query.append("SELECT ");
    type.quote(query, fieldNames);
    query.append(" FROM ( ");

    query.append("SELECT ");
    type.quote(query, fieldNames);
    query.append(" FROM ");
    query.append(tableName);
    if (filter != null) {
      query.append(" WHERE ");
      query.append(filter.getExprString());
    }
    query.append(" ORDER BY ");
    type.quote(query, fieldNames[0]);
    query.append(" ) X ");

    query.append(" WHERE ROWNUM BETWEEN ");
    query.append(split.getStart());
    query.append(" AND ");
    query.append(split.getEnd());
    return query.toString();
  }

  private static String getQueryForSybase(DatabaseProperties dbProperties,
      ExprNodeDesc filter, int limit) {
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
    if (filter != null) {
      query.append(" WHERE ");
      query.append(filter.getExprString());
    }
    query.append(" ORDER BY ");
    type.quote(query, fieldNames[0]);
    return query.toString();
  }

  private static String getQueryForSybase(DatabaseProperties dbProperties,
      ExprNodeDesc filter, JDBCSplit split) {
    DatabaseType type = dbProperties.getDatabaseType();
    String tableName = dbProperties.getTableName();
    String[] fieldNames = dbProperties.getFieldNames();

    StringBuilder query = new StringBuilder();
    query.append("SELECT ");
    type.quote(query, fieldNames);
    query.append(" FROM (");

    query.append("SELECT ");
    type.quote(query, fieldNames);
    query.append(" FROM ");
    query.append(tableName);
    if (filter != null) {
      query.append(" WHERE ");
      query.append(filter.getExprString());
    }

    query.append(" ) X ");

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

  private static String getQueryForSelectAll(DatabaseProperties dbProperties,
      ExprNodeDesc filter, int limit) {
    DatabaseType type = dbProperties.getDatabaseType();
    String tableName = dbProperties.getTableName();
    String[] fieldNames = dbProperties.getFieldNames();

    StringBuilder query = new StringBuilder();
    query.append("SELECT ");
    type.quote(query, fieldNames);
    query.append(" FROM ");
    query.append(tableName);
    if (filter != null) {
      query.append(" WHERE ");
      query.append(filter.getExprString());
    }
    if (limit > 0) {
      query.append(" LIMIT ");
      query.append(limit);
    }
    return query.toString();
  }
}
