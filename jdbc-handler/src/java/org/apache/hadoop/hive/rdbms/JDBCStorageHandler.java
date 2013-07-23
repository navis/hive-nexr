package org.apache.hadoop.hive.rdbms;

import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.rdbms.db.DBOperation;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.util.StringUtils;

public class JDBCStorageHandler extends DefaultStorageHandler implements HiveMetaHook {

  private Configuration conf;

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
    boolean isExternal = MetaStoreUtils.isExternalTable(table);

    if (!isExternal) {
      throw new MetaException("Tables must be external.");
    }
    try {
      DBOperation.createTableIfNotExist(table.getParameters());
    } catch (Exception e) {
      throw new MetaException(StringUtils.stringifyException(e));
    }
  }

  @Override
  public void rollbackCreateTable(Table table) throws MetaException {
  }

  @Override
  public void commitCreateTable(Table table) throws MetaException {
  }

  @Override
  public void preDropTable(Table table) throws MetaException {
    try {
      DBOperation.runSQLQueryBeforeDataInsert(table.getParameters());
    } catch (Exception e) {
      throw new MetaException(StringUtils.stringifyException(e));
    }
  }

  @Override
  public void rollbackDropTable(Table table) throws MetaException {
  }

  @Override
  public void commitDropTable(Table table, boolean b) throws MetaException {
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
}
