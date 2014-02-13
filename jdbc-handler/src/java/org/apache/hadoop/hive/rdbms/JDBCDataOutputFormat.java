package org.apache.hadoop.hive.rdbms;

import java.io.IOException;
import java.sql.Connection;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.rdbms.db.DBOperation;
import org.apache.hadoop.hive.rdbms.db.DBRecordWriter;
import org.apache.hadoop.hive.rdbms.db.DatabaseProperties;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

import static org.apache.hadoop.hive.serde.serdeConstants.LIST_COLUMNS;
import static org.apache.hadoop.hive.serde.serdeConstants.LIST_COLUMN_TYPES;

public class JDBCDataOutputFormat implements OutputFormat<NullWritable, MapWritable>,
    HiveOutputFormat<NullWritable, MapWritable> {

  public FileSinkOperator.RecordWriter getHiveRecordWriter(JobConf conf, Path path,
      Class<? extends Writable> aClass, boolean b, Properties properties,
      Progressable progressable) throws IOException {
    try {
      DatabaseProperties dbProperties = new DatabaseProperties();
      dbProperties.setTableName(ConfigurationUtils.getOutputTableName(conf));
      dbProperties.setOutputColumnMappingFields(ConfigurationUtils.getColumnMappingFields(conf));

      dbProperties.setColumnNameTypes(
          conf.get(LIST_COLUMNS),
          conf.get(LIST_COLUMN_TYPES),
          true);

      dbProperties.setBatchSize(conf.getInt(
          ConfigurationUtils.HIVE_JDBC_OUTPUT_BATCH_SIZE,
          ConfigurationUtils.HIVE_JDBC_OUTPUT_BATCH_SIZE_DEFAULT));
      dbProperties.setUseUpperCase(conf.getBoolean(
          ConfigurationUtils.HIVE_JDBC_ORACLE_USE_UPPERCASE_TABLENAME,
          ConfigurationUtils.HIVE_JDBC_ORACLE_USE_UPPERCASE_TABLENAME_DEFAULT));

      Connection connection = DBOperation.createConnection(conf);
      return new DBRecordWriter(dbProperties, connection);
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public RecordWriter<NullWritable, MapWritable> getRecordWriter(FileSystem fileSystem,
      JobConf entries, String s, Progressable progressable) throws IOException {
    throw new RuntimeException("Error: Hive should not invoke this method.");
  }

  public void checkOutputSpecs(FileSystem fileSystem, JobConf conf) throws IOException {
  }
}
