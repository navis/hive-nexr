package org.apache.hadoop.hive.phoenix;

import antlr.build.ANTLR;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.TokenRewriteStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.mapred.TableMapReduceUtil;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.lib.Utils;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveStorageSubQueryHandler;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.QBParseInfo;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.rdbms.JDBCDataOutputFormat;
import org.apache.hadoop.hive.rdbms.JDBCDataSerDe;
import org.apache.hadoop.hive.rdbms.JDBCStorageHandler;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.jdbc.PhoenixConnection;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PhoenixStorageHandler extends DefaultStorageHandler implements HiveStorageSubQueryHandler {

  protected static final Log LOG =
      LogFactory.getLog(PhoenixStorageHandler.class.getName());

  public static final String PHOENIX_TABLE_NAME = "phoenix.table.name";
  public static final String PHOENIX_QUERY = "phoenix.query.str";

  @Override
  public Class<? extends org.apache.hadoop.mapred.InputFormat> getInputFormatClass() {
    return PhoenixDataInputFormat.class;
  }

  @Override
  public Class<? extends org.apache.hadoop.mapred.OutputFormat> getOutputFormatClass() {
    return JDBCDataOutputFormat.class;
  }

  @Override
  public Class<? extends SerDe> getSerDeClass() {
    return JDBCDataSerDe.class;
  }

  @Override
  public void configureJobConf(TableDesc tableDesc, JobConf jobConf) {
    try {
      TableMapReduceUtil.addDependencyJars(jobConf);
      org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil.addDependencyJars(jobConf,
          PhoenixStorageHandler.class, JDBCStorageHandler.class,
          org.apache.hadoop.hbase.HBaseConfiguration.class,
          ANTLR.class, RecognitionException.class,
          PhoenixConnection.class);
      jobConf.set(DBConfiguration.INPUT_TABLE_NAME_PROPERTY, tableDesc.getTableName());
      jobConf.set(DBConfiguration.OUTPUT_TABLE_NAME_PROPERTY, tableDesc.getTableName());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public TableScanOperator handleSubQuery(
      QBParseInfo parseInfo, Table table, TokenRewriteStream rewriter, ASTNode source) {
    Map<String,String> params = table.getSd().getSerdeInfo().getParameters();
    final String tableName = params.get(PHOENIX_TABLE_NAME);
    Utils.traverse(Arrays.asList(source), new Utils.Function<ASTNode, Boolean>() {
      public Boolean apply(ASTNode input) {
        if (input.getType() == HiveParser.TOK_TABNAME) {
          ASTNode original = (ASTNode) input.getChildren().get(input.getChildCount() - 1);
          original.getToken().setText(tableName);
          return false;
        }
        return true;
      }
    });
    String query = rewriter.toString(source.getTokenStartIndex(), source.getTokenStopIndex());
    String url = params.get(DBConfiguration.URL_PROPERTY);

    Map<String, String> scanPlan = new HashMap<String, String>();
    scanPlan.put(DBConfiguration.URL_PROPERTY, url);
    scanPlan.put(PHOENIX_QUERY, query);
    try {
      QueryPlan queryPlan = PhoenixUtils.getPlan(url, query);
      if (queryPlan.getGroupBy() != null) {
        List<Expression> keys = queryPlan.getGroupBy().getKeyExpressions();
        return null;  // todo: should use hash distribution on group by key
      }
      RowSchema schema = PhoenixUtils.toSchema(table.getTableName(), queryPlan.getProjector());
      TableScanOperator ts = (TableScanOperator) OperatorFactory.get(new TableScanDesc(), schema);
      ts.setScanPlan(scanPlan);
      return ts;
    } catch (Exception e) {
      LOG.info("Failed to make plan of sub query for phoenix table " + tableName, e);
      e.printStackTrace();
      return null;
    }
  }
}
