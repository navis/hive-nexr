package org.apache.hadoop.hive.rdbms.db;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.rdbms.ColumnAccess;
import org.apache.hadoop.hive.rdbms.ConfigurationUtils;
import org.apache.hadoop.hive.rdbms.JDBCSplit;
import org.apache.hadoop.hive.rdbms.RowWritable;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBRecordReader implements RecordReader<LongWritable, RowWritable> {

  private static final Logger log = LoggerFactory.getLogger(DBRecordReader.class);

  private Connection connection;
  private Statement statement;
  private ResultSet results;
  private JDBCSplit split;
  private long pos = 0;

  private ColumnAccess[] columns;

  public DBRecordReader(JDBCSplit split, DatabaseProperties dbProperties, Connection connection,
                        List<Integer> colIndices) throws Exception {

    this.split = split;
    this.connection = connection;

    if (dbProperties.getDatabaseType() == null) {
      dbProperties.setDatabaseType(DatabaseType.getDatabaseType(dbProperties, connection));
    }

    connection.setAutoCommit(false);

    String[] names = dbProperties.getColumnNames();
    PrimitiveCategory[] types = ConfigurationUtils.toTypes(dbProperties.getColumnTypes());

    Map<String, String> mapping = dbProperties.getInputColumnMappingFields();
    List<String> fieldNames = new ArrayList<String>();
    if (colIndices != null && !colIndices.isEmpty()) {
      for (int i : colIndices) {
        fieldNames.add(mapping == null ? names[i] : mapping.get(names[i]));
      }
    } else {
      for (int i = 0; i < names.length; i++) {
        fieldNames.add(mapping == null ? names[i] : mapping.get(names[i]));
      }
    }
    dbProperties.setFieldNames(fieldNames.toArray(new String[fieldNames.size()]));

    String sqlQuery = QueryConstructor.constructSelectQueryForReading(dbProperties, split);
    log.error("select query for split = " + sqlQuery);
    statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    statement.setFetchSize(dbProperties.getBatchSize());

    results = statement.executeQuery(sqlQuery);

    ResultSetMetaData resultsMetaData = results.getMetaData();
    int columnCount = resultsMetaData.getColumnCount();

    Map<String, Integer> nameToIndex = new HashMap<String, Integer>();
    for (int i = 1; i <= columnCount; i++) {
      // This is the column name in db table
      nameToIndex.put(resultsMetaData.getColumnName(i), i);
    }

    ColumnAccess[] columns = new ColumnAccess[names.length];
    for (int i = 0; i < names.length; i++) {
      String name = mapping == null ? names[i] : mapping.get(names[i]);
      Integer index = nameToIndex.get(name);
      if (index == null) {
        log.warn("Invalid column name " + names[i] + ":" + name);
      } else {
        columns[i] = new ColumnAccess(types[i], index);
      }
    }
    this.columns = columns;
  }

  public boolean next(LongWritable key, RowWritable value) throws IOException {
    try {
      if (!results.next()) {
        return false;
      }
      // Set the key field value as the output key value
      key.set(pos + split.getStart());

      value.clear();
      for (ColumnAccess expr : columns) {
        value.add(expr == null ? null : expr.getValue(results));
      }
      pos++;
    } catch (SQLException e) {
      throw new IOException(e.toString(), e);
    }
    return true;
  }

  public LongWritable createKey() {
    return new LongWritable();
  }

  public RowWritable createValue() {
    return new RowWritable();
  }

  public long getPos() throws IOException {
    return pos;
  }

  public void close() throws IOException {
    DBOperation.closeResultSet(results, log);
    DBOperation.closeStatement(statement, log);
    DBOperation.closeConnection(connection, log);
  }

  public float getProgress() throws IOException {
    return split.getLength() > 0 ? pos / (float) split.getLength() : 1.0f;
  }
}
