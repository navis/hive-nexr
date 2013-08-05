package org.apache.hadoop.hive.rdbms;

import java.io.IOException;
import java.sql.Connection;
import java.util.List;

import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.rdbms.db.DBOperation;
import org.apache.hadoop.hive.rdbms.db.DBRecordReader;
import org.apache.hadoop.hive.rdbms.db.DatabaseProperties;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class JDBCDataInputFormat extends HiveInputFormat<LongWritable, MapWritable> {

  @Override
  public RecordReader getRecordReader(InputSplit split, JobConf conf, Reporter reporter)
      throws IOException {
    DatabaseProperties dbProperties = new DatabaseProperties();
    dbProperties.setTableName(ConfigurationUtils.getInputTableName(conf));
    dbProperties.setInputColumnMappingFields(ConfigurationUtils.getHiveToDB(conf));

    dbProperties.setColumnNameTypes(
        conf.get(ConfigurationUtils.LIST_COLUMNS),
        conf.get(ConfigurationUtils.LIST_COLUMN_TYPES),
        false);

    dbProperties.setBatchSize(conf.getInt(ConfigurationUtils.HIVE_JDBC_INPUT_BATCH_SIZE,
          ConfigurationUtils.HIVE_JDBC_INPUT_BATCH_SIZE_DEFAULT));

    List<Integer> columns = ColumnProjectionUtils.getReadColumnIDs(conf);
    int rowLimit = ColumnProjectionUtils.getRowLimit(conf);

    try {
      Connection connection = DBOperation.createConnection(conf);
      return new DBRecordReader((JDBCSplit) split, dbProperties, connection, columns, rowLimit);
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    return JDBCSplit.getSplits(job, numSplits);
  }
}