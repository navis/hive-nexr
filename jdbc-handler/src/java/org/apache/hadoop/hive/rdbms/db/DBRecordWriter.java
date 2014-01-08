package org.apache.hadoop.hive.rdbms.db;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.rdbms.ColumnAccess;
import org.apache.hadoop.hive.rdbms.ConfigurationUtils;
import org.apache.hadoop.hive.rdbms.RowWritable;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBRecordWriter implements RecordWriter {

  private static final Logger log = LoggerFactory.getLogger(DBRecordWriter.class);

  private Connection connection;
  private DatabaseProperties dbProperties;

  private ColumnAccess[] columns;
  private PreparedStatement statement;

  private int maxBatch;

  public DBRecordWriter(DatabaseProperties dbProperties, Connection connection) throws Exception {
    this.dbProperties = dbProperties;
    this.connection = connection;
    this.maxBatch = dbProperties.getBatchSize();

    if (dbProperties.getDatabaseType() == null) {
      dbProperties.setDatabaseType(DatabaseType.getDatabaseType(dbProperties, connection));
    }
    String[] types = dbProperties.getColumnTypes();
    PrimitiveCategory[] categories = ConfigurationUtils.toTypes(types);
    ColumnAccess[] columns = new ColumnAccess[types.length];
    for (int i = 0; i < types.length; i++) {
      columns[i] = new ColumnAccess(dbProperties.getDatabaseType(), categories[i], i + 1);
    }
    this.columns = columns;

    String[] fieldNames = dbProperties.getOutputColumnMappingFields();
    if (fieldNames == null) {
      fieldNames = dbProperties.getColumnNames();
    }
    dbProperties.setFieldNames(fieldNames);

    String sqlQuery = QueryConstructor.constructInsertQuery(dbProperties);
    statement = connection.prepareStatement(sqlQuery);
  }

  private transient int queued = 0;

  public void write(Writable writable) throws IOException {
    List<Object> row = ((RowWritable) writable).get();

    try {
      for (int i = 0; i < columns.length; i++) {
        columns[i].setValue(statement, row.get(i));
      }
      statement.addBatch();
      if (queued++ == maxBatch) {
        statement.executeBatch();
        queued = 0;
      }
    } catch (SQLException e) {
      log.error("Failed to write data to the table: " + dbProperties.getTableName(), e);
    }
  }

  public void close(boolean abort) throws IOException {
    try {
      if (!abort && queued > 0) {
        try {
          statement.executeBatch();
        } catch (SQLException e) {
          log.error("Failed to write data to the table: " + dbProperties.getTableName(), e);
        }
      }
    } finally {
      DBOperation.closeStatement(statement, log);
      DBOperation.closeConnection(connection, log);
    }
  }
}
