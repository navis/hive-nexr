package org.apache.hadoop.hive.phoenix;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormatBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.phoenix.compile.ColumnProjector;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.schema.PDataType;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;

public class PhoenixUtils {

  public static QueryPlan getPlan(Configuration conf, String query) throws SQLException {
    return getPlan(getURL(conf), query);
  }

  public static QueryPlan getPlan(String url, String query) throws SQLException {
    PhoenixConnection connection = getConnection(url);
    try {
      PhoenixPreparedStatement pstmt = (PhoenixPreparedStatement) connection.prepareStatement(query);
      QueryPlan queryPlan = pstmt.compileQuery();
      queryPlan = connection.getQueryServices().getOptimizer().optimize(pstmt, queryPlan);
      return queryPlan;
    } finally {
      connection.close();
    }
  }

  public static RowSchema toSchema(String alias, RowProjector projector) {
    ArrayList<ColumnInfo> columns = new ArrayList<ColumnInfo>();
    for (ColumnProjector column : projector.getColumnProjectors()) {
      PDataType type = column.getExpression().getDataType();
      ColumnInfo columnInfo = new ColumnInfo(column.getName(), toHiveType(type), alias, false);
      columnInfo.setAlias(column.getName());
      columns.add(columnInfo);
    }
    return new RowSchema(columns);
  }

  public static TypeInfo toHiveType(PDataType type) {
    switch (type) {
      case VARCHAR:
        return TypeInfoFactory.stringTypeInfo;
      case BOOLEAN:
        return TypeInfoFactory.booleanTypeInfo;
      case TINYINT:
        return TypeInfoFactory.byteTypeInfo;
      case SMALLINT:
        return TypeInfoFactory.shortTypeInfo;
      case INTEGER:
        return TypeInfoFactory.intTypeInfo;
      case FLOAT:
        return TypeInfoFactory.floatTypeInfo;
      case DOUBLE:
        return TypeInfoFactory.doubleTypeInfo;
    }
    throw new UnsupportedOperationException("Unsupported type " + type);
  }

  private static String getURL(Configuration conf) throws SQLException {
    String url = conf.get(DBConfiguration.URL_PROPERTY);
    if (url == null || !new PhoenixDriver().acceptsURL(url)) {
      throw new SQLException("Invalid connection url " + url);
    }
    return url;
  }

  public static PhoenixConnection getConnection(Configuration conf) throws SQLException {
    return getConnection(getURL(conf));
  }

  public static PhoenixConnection getConnection(String url) throws SQLException {
    return (PhoenixConnection) DriverManager.getConnection("jdbc:phoenix:localhost:2181");
  }

  public static class DummyTableInputFormat extends TableInputFormatBase {
    public void setHTable(HTable table) {
      super.setHTable(table);
    }
  }

  public static void main(String[] args) throws Exception{
    PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection("jdbc:phoenix:localhost:2181");
    System.err.println("[PhoenixStorageHandler/main] " + conn);

    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

    Configuration conf = HBaseConfiguration.create(new Configuration());

    String line;
    StringBuilder query = new StringBuilder();
    while ((line = reader.readLine()) != null) {
      line = line.trim();
      if (line.isEmpty()) {
        continue;
      }
      if (line.equals("quit")) {
        break;
      }
      if (query.length() > 0) {
        query.append(' ');
      }
      if (line.endsWith(";")) {
        query.append(line.substring(0, line.length() - 1));
        try {
          String sql = query.toString();
          System.err.println("-- [PhoenixStorageHandler/main] " + sql);
          PhoenixPreparedStatement pstmt = (PhoenixPreparedStatement) conn.prepareStatement(sql);

          if (sql.startsWith("UPSERT")) {
            int row = pstmt.executeUpdate();
            System.err.println("-- [PhoenixStorageHandler/main] fin " + row);
          } else if (pstmt.execute()) {
            QueryPlan plan = pstmt.compileQuery(sql);
            toString(conf, plan);
            plan = conn.getQueryServices().getOptimizer().optimize(pstmt, plan);
            toString(conf, plan);

            ResultSet result = pstmt.getResultSet();
            ResultSetMetaData metaData = result.getMetaData();
            StringBuilder row = new StringBuilder();
            while (result.next()) {
              for (int i = 1; i <= metaData.getColumnCount(); i++) {
                if (row.length() > 0) {
                  row.append(", ");
                }
                row.append(String.valueOf(result.getObject(i)));
              }
              System.out.println(row.toString());
              row.setLength(0);
            }
            result.close();
          }
          System.err.println("-- [PhoenixStorageHandler/main] fin " + sql);
          pstmt.close();
        } catch (SQLException e) {
          e.printStackTrace();
        } finally {
          query.setLength(0);
        }
      } else {
        query.append(line);
      }
    }
    conn.close();
  }

  private static void toString(Configuration conf, QueryPlan plan) throws IOException {
    System.err.println("--- [PhoenixStorageHandler/main] " + plan.getTableRef().getTable().getName().getString());
    System.err.println("--- [PhoenixStorageHandler/main] " + plan.getContext().getScan());
    System.err.println("--- [PhoenixStorageHandler/main] " + plan.getContext().getScanRanges());
    if (plan.getContext().getWhereCoditionColumns() != null) {
      for (Pair<byte[], byte[]> pair : plan.getContext().getWhereCoditionColumns()) {
        System.err.println("--- [PhoenixStorageHandler/main] " + Bytes.toString(pair.getFirst()) + " = " + Bytes.toString(pair.getSecond()));
      }
    }
    if (plan.getGroupBy() != null) {
      System.err.println("---- [PhoenixStorageHandler/main] GBY.EXPR : " + plan.getGroupBy().getExpressions());
      System.err.println("---- [PhoenixStorageHandler/main] GBY.KEY  : " + plan.getGroupBy().getKeyExpressions());
    }
    if (plan.getOrderBy() != null) {
      System.err.println("---- [PhoenixStorageHandler/main] OBY.EXPR : " + plan.getOrderBy().getOrderByExpressions());
    }
    if (plan.getLimit() != null) {
      System.err.println("---- [PhoenixStorageHandler/main] LIMIT : " + plan.getLimit());
    }
    for (ColumnProjector column : plan.getProjector().getColumnProjectors()) {
      System.err.println("+++++ [PhoenixStorageHandler/main] " + column.getName() + ":" + column.getExpression().getDataType());
    }

    byte[] tableName = plan.getTableRef().getTable().getName().getBytes();

    DummyTableInputFormat tableIF = new DummyTableInputFormat();
    tableIF.setHTable(new HTable(conf, tableName));
    tableIF.setScan(plan.getContext().getScan());
    for (InputSplit split : tableIF.getSplits(new JobContext(new Configuration(), new JobID("navis", 0)))) {
      System.err.println("-- [PhoenixStorageHandler/main] " + split);
    }
  }
}
