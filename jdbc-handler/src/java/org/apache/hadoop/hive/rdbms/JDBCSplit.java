package org.apache.hadoop.hive.rdbms;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.rdbms.db.DBOperation;
import org.apache.hadoop.hive.rdbms.db.DatabaseProperties;
import org.apache.hadoop.hive.rdbms.db.QueryConstructor;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JDBCSplit extends FileSplit implements InputSplit {

  private static final Logger log = LoggerFactory.getLogger(JDBCSplit.class);
  private static final String[] EMPTY_ARRAY = new String[]{};

  private long start, end;

  public JDBCSplit() {
    super((Path) null, 0, 0, EMPTY_ARRAY);
  }

  public JDBCSplit(long start, long end, Path dummyPath) {
    super(dummyPath, 0, 0, EMPTY_ARRAY);
    this.start = start;
    this.end = end;
  }

  @Override
  public void readFields(final DataInput input) throws IOException {
    super.readFields(input);
    start = input.readLong();
    end = input.readLong();
  }

  @Override
  public void write(final DataOutput output) throws IOException {
    super.write(output);
    output.writeLong(start);
    output.writeLong(end);
  }

  /* Data is remote for all nodes. */
  @Override
  public String[] getLocations() throws IOException {
    return EMPTY_ARRAY;
  }

  public long getStart() {
    return start;
  }

  public long getEnd() {
    return end;
  }

  @Override
  public long getLength() {
    return end - start;
  }

  public static JDBCSplit[] getSplits(JobConf conf, int numSplits) throws IOException {

    ExprNodeDesc filterExpr = getFilterExpr(conf);

    int rowLimit = ColumnProjectionUtils.getRowLimit(conf);
    final Path[] tablePaths = FileInputFormat.getInputPaths(conf);
    int maxrow = conf.getInt(ConfigurationUtils.HIVE_JDBC_MAXROW_PER_TASK,
        ConfigurationUtils.HIVE_JDBC_MAXROW_PER_TASK_DEFAULT);
    if (maxrow < 0) {
      return new JDBCSplit[]{new JDBCSplit(-1, -1, tablePaths[0])};
    }
    int minrow = conf.getInt(ConfigurationUtils.HIVE_JDBC_MINROW_PER_TASK,
        ConfigurationUtils.HIVE_JDBC_MINROW_PER_TASK_DEFAULT);
    if (minrow > 0 && minrow > maxrow) {
      minrow = -1;
    }

    DatabaseProperties dbProperties = new DatabaseProperties();
    dbProperties.setTableName(ConfigurationUtils.getInputTableName(conf));
    dbProperties.setUserName(ConfigurationUtils.getDatabaseUserName(conf));
    dbProperties.setPassword(ConfigurationUtils.getDatabasePassword(conf));
    dbProperties.setConnectionUrl(ConfigurationUtils.getConnectionUrl(conf));
    dbProperties.setDriverClass(ConfigurationUtils.getDriverClass(conf));
    dbProperties.setInputColumnMappingFields(ConfigurationUtils.getHiveToDB(conf));

    QueryConstructor queryConstructor = new QueryConstructor();
    String sql = queryConstructor.constructCountQuery(dbProperties, filterExpr);
    log.info("total count sql = " + sql);

    try {
      Connection connection = DBOperation.createConnection(conf);
      long total = DBOperation.getTotalCount(sql, connection);
      log.info("total count = " + total);
      if (rowLimit > 0 && rowLimit < total) {
        log.info("revised total count by limit = " + rowLimit);
        total = rowLimit;
      }
      long splitSize = total / numSplits;
      if (splitSize == 0) {
        splitSize = total;
      } else {
        splitSize = Math.min(splitSize, maxrow);
        if (minrow > 0) {
          splitSize = Math.max(splitSize, minrow);
        }
      }

      long remain = total;
      List<JDBCSplit> splits = new ArrayList<JDBCSplit>();
      for (int i = 0; remain > 0; i++) {
        JDBCSplit split;
        if (remain - splitSize < 0) {
          split = new JDBCSplit(i * splitSize, i * splitSize + remain, tablePaths[0]);
        } else {
          split = new JDBCSplit(i * splitSize, (i + 1) * splitSize, tablePaths[0]);
        }
        splits.add(split);
        log.info("split-" + i + " = " + split.getStart() + "~" + split.getEnd());
        remain -= splitSize;
      }
      return splits.toArray(new JDBCSplit[splits.size()]);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public static ExprNodeDesc getFilterExpr(JobConf conf) {
    String filterExprSerialized = conf.get(TableScanDesc.FILTER_EXPR_CONF_STR);
    if (filterExprSerialized == null || filterExprSerialized.isEmpty()) {
      return null;
    }
    final Map<String,String> mappings = ConfigurationUtils.getHiveToDB(conf);
    ExprNodeDesc filterExpr = Utilities.deserializeExpression(filterExprSerialized, conf);
    ExprNodeDescUtils.traverse(filterExpr, new ExprNodeDescUtils.Function<ExprNodeDesc, Void>() {
      public Void apply(ExprNodeDesc input) {
        if (input instanceof ExprNodeColumnDesc) {
          ExprNodeColumnDesc column = (ExprNodeColumnDesc) input;
          String dbColumn = mappings.get(column.getColumn());
          if (dbColumn == null) {
            throw new RuntimeException("Invalid column name " + column.getColumn());
          }
          column.setColumn(dbColumn);
        }
        return null;
      }
    });
    return filterExpr;
  }
}
