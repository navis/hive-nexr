package org.apache.hadoop.hive.phoenix;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.rdbms.ColumnAccess;
import org.apache.hadoop.hive.rdbms.db.DBRecordReader;
import org.apache.hadoop.hive.rdbms.db.DatabaseType;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.jdbc.PhoenixResultSet;

import java.io.IOException;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

public class PhoenixDataInputFormat extends HiveInputFormat<LongWritable, MapWritable> {

  @Override
  public InputSplit[] getSplits(JobConf jobConf, int numSplits) throws IOException {
    String url = jobConf.get(DBConfiguration.URL_PROPERTY);
    String query = jobConf.get(PhoenixStorageHandler.PHOENIX_QUERY);
    try {
      QueryPlan plan = PhoenixUtils.getPlan(url, query);
      byte[] tableName = plan.getTableRef().getTable().getName().getBytes();
      PhoenixUtils.DummyTableInputFormat tableIF = new PhoenixUtils.DummyTableInputFormat();
      tableIF.setHTable(new HTable(jobConf, tableName));
      tableIF.setScan(plan.getContext().getScan());

      Job job = new Job(jobConf);
      JobContext jobContext = ShimLoader.getHadoopShims().newJobContext(job);
      Path[] tablePaths = FileInputFormat.getInputPaths(jobContext);

      List<org.apache.hadoop.mapreduce.InputSplit> splits =
          tableIF.getSplits(new JobContext(jobConf, new JobID("dummy", 0)));
      InputSplit[] results = new InputSplit[splits.size()];

      for (int i = 0; i < splits.size(); i++) {
        TableSplit tsplit = (TableSplit) splits.get(i);
        results[i] = new PhoenixSplit(url, query, tsplit, tablePaths[0]);
      }

      return results;
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  @Override
  public RecordReader getRecordReader(InputSplit split, JobConf conf, Reporter reporter)
      throws IOException {
    try {
      return createRecordReader(split, conf);
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  private RecordReader createRecordReader(InputSplit split, JobConf conf) throws Exception {
    PhoenixSplit psplit = (PhoenixSplit) split;
    TableSplit tsplit = psplit.getSplit();

    PhoenixConnection connection = PhoenixUtils.getConnection(psplit.getURL());
    PhoenixPreparedStatement pstmt =
        (PhoenixPreparedStatement) connection.prepareStatement(psplit.getQuery());

    QueryPlan plan = pstmt.compileQuery();
    plan = connection.getQueryServices().getOptimizer().optimize(pstmt, plan);

    RowSchema schema = PhoenixUtils.toSchema(null, plan.getProjector());

    Scan scan = plan.getContext().getScan();
    if (tsplit.getStartRow() != HConstants.EMPTY_START_ROW) {
      scan.setStartRow(tsplit.getStartRow());
    }
    if (tsplit.getEndRow() != HConstants.EMPTY_END_ROW) {
      scan.setStopRow(tsplit.getEndRow());
    }
    LOG.info(scan);
    PhoenixResultSet results = new PhoenixResultSet(plan.iterator(), plan.getProjector(), pstmt);

    ResultSetMetaData resultsMetaData = results.getMetaData();
    int columnCount = resultsMetaData.getColumnCount();

    List<ColumnInfo> signature = schema.getSignature();
    ColumnAccess[] columns = new ColumnAccess[columnCount];
    for (int i = 0; i < columns.length; i++) {
      PrimitiveTypeInfo type = (PrimitiveTypeInfo) signature.get(i).getType();
      columns[i] = new ColumnAccess(DatabaseType.PHOENIX, type.getPrimitiveCategory(), i + 1);
    }
    return new DBRecordReader(split, connection, results, columns);
  }
}
