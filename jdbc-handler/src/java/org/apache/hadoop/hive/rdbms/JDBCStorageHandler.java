package org.apache.hadoop.hive.rdbms;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.index.IndexPredicateAnalyzer;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
import org.apache.hadoop.hive.ql.plan.AlterTableDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.rdbms.db.DBOperation;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class JDBCStorageHandler extends DefaultStorageHandler
    implements HiveMetaHook, HiveStoragePredicateHandler {

  protected Configuration conf;

  @Override
  public boolean supports(org.apache.hadoop.hive.ql.metadata.Table tbl,
                          AlterTableDesc.AlterTableTypes alter) {
    return
        alter == AlterTableDesc.AlterTableTypes.ADDPROPS ||
        alter == AlterTableDesc.AlterTableTypes.DROPPROPS ||
        alter == AlterTableDesc.AlterTableTypes.ADDSERDEPROPS;
  }

  @Override
  public Class<? extends org.apache.hadoop.mapred.InputFormat> getInputFormatClass() {
    return JDBCDataInputFormat.class;
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
  public HiveMetaHook getMetaHook() {
    return this;
  }

  @Override
  public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
    Properties properties = tableDesc.getProperties();
    ConfigurationUtils.copyJDBCProperties(properties, jobProperties, false);
  }

  @Override
  public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
    Properties properties = tableDesc.getProperties();
    ConfigurationUtils.copyJDBCProperties(properties, jobProperties, true);
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void preCreateTable(Table table) throws MetaException {
    if (!MetaStoreUtils.isExternalTable(table)) {
      throw new MetaException("Tables must be external.");
    }
  }

  @Override
  public void rollbackCreateTable(Table table) throws MetaException {
  }

  @Override
  public void commitCreateTable(Table table) throws MetaException {
    try {
      DBOperation.createTableIfNotExist(table.getParameters());
    } catch (Exception e) {
      throw new MetaException(StringUtils.stringifyException(e));
    }
  }

  @Override
  public void preDropTable(Table table) throws MetaException {
  }

  @Override
  public void rollbackDropTable(Table table) throws MetaException {
  }

  @Override
  public void commitDropTable(Table table, boolean b) throws MetaException {
    try {
      DBOperation.runSQLQueryBeforeDataInsert(table.getParameters());
    } catch (Exception e) {
      throw new MetaException(StringUtils.stringifyException(e));
    }
  }

  @Override
  public void preCreatePartition(Table table, Partition partition) throws MetaException {
  }

  @Override
  public void rollbackCreatePartition(Table table, Partition partition) throws MetaException {
  }

  @Override
  public void commitCreatePartition(Table table, Partition partition) throws MetaException {
  }

  @Override
  public void preDropPartition(Table table, Partition partition) throws MetaException {
  }

  @Override
  public void rollbackDropPartition(Table table, Partition partition) throws MetaException {
  }

  @Override
  public void commitDropPartition(Table table, Partition partition, boolean deleteData)
      throws MetaException {
  }

  @Override
  public DecomposedPredicate decomposePredicate(JobConf jobConf,
      Deserializer deserializer, ExprNodeDesc predicate) {

    if (!conf.getBoolean(ConfigurationUtils.HIVE_JDBC_ENABLE_FILTER_PUSHDOWN,
        ConfigurationUtils.HIVE_JDBC_ENABLE_FILTER_PUSHDOWN_DEFAULT)) {
      return null;
    }

    IndexPredicateAnalyzer analyzer = newIndexPredicateAnalyzer();
    List<IndexSearchCondition> searchConditions = new ArrayList<IndexSearchCondition>();
    ExprNodeDesc residualPredicate = analyzer.analyzePredicate(predicate, searchConditions);

    DecomposedPredicate decomposedPredicate = new DecomposedPredicate();
    decomposedPredicate.pushedPredicate = analyzer.translateSearchConditions(searchConditions);
    decomposedPredicate.residualPredicate = residualPredicate;
    return decomposedPredicate;
  }

  private IndexPredicateAnalyzer newIndexPredicateAnalyzer() {
    IndexPredicateAnalyzer analyzer = new IndexPredicateAnalyzer();
    analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual");
    analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan");
    analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan");
    analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan");
    analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan");

    return analyzer;
  }
}
