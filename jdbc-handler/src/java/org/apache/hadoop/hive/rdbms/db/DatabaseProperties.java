package org.apache.hadoop.hive.rdbms.db;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.metadata.VirtualColumn;

public class DatabaseProperties {

  private DatabaseType databaseType;

  private String connectionUrl;
  private String userName;
  private String password;
  private String driverClass;
  private String tableName;

  // hive to db
  private Map<String, String> inputColumnMappingFields;
  private String[] outputColumnMappingFields;

  // hive-side
  private String[] columnNames;
  private String[] columnTypes;

  // db-side
  private String[] fieldNames;

  private boolean useUpperCase;

  private transient int batchSize;

  public DatabaseType getDatabaseType() {
    return databaseType;
  }

  public void setDatabaseType(DatabaseType databaseType) {
    this.databaseType = databaseType;
  }

  public Map<String, String> getInputColumnMappingFields() {
    return inputColumnMappingFields;
  }

  public void setInputColumnMappingFields(Map<String, String> inputColumnMappingFields) {
    this.inputColumnMappingFields = inputColumnMappingFields;
  }

  public String[] getOutputColumnMappingFields() {
    return outputColumnMappingFields;
  }

  public void setOutputColumnMappingFields(String[] outputColumnMappingFields) {
    this.outputColumnMappingFields = outputColumnMappingFields;
  }

  public String getConnectionUrl() {
    return connectionUrl;
  }

  public void setConnectionUrl(String connectionUrl) {
    if (connectionUrl != null) {
      connectionUrl = connectionUrl.replaceAll(" ", "");
    }
    this.connectionUrl = connectionUrl;
  }

  public String getUserName() {
    return userName;
  }

  public void setUserName(String userName) {
    this.userName = userName;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public String getDriverClass() {
    return driverClass;
  }

  public void setDriverClass(String driverClass) {
    this.driverClass = driverClass;
  }

  public String getTableName() {
    return useUpperCaseTableName() ? tableName.toUpperCase() : tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String[] getColumnNames() {
    return columnNames;
  }

  public String[] getColumnTypes() {
    return columnTypes;
  }

  // output schema is from serde, which is delimited by colon
  public void setColumnNameTypes(String columnNames, String columnTypes, boolean output) {
    String[] names = columnNames.split(",");
    String[] types = columnTypes.split(output ? ":" : ",");
    assert names.length == types.length;

    List<String> newNames = new ArrayList<String>();
    List<String> newTypes = new ArrayList<String>();
    for (int i = 0; i < names.length; i++) {
      if (names[i].equals(VirtualColumn.FILENAME.getName()) ||
          names[i].equals(VirtualColumn.BLOCKOFFSET.getName()) ||
          names[i].equals(VirtualColumn.ROWOFFSET.getName()) ||
          names[i].equals(VirtualColumn.RAWDATASIZE.getName())) {
        continue;
      }
      newNames.add(names[i]);
      newTypes.add(types[i]);
    }
    this.columnNames = newNames.toArray(new String[newNames.size()]);
    this.columnTypes = newTypes.toArray(new String[newTypes.size()]);
  }

  public String[] getFieldNames() {
    return fieldNames;
  }

  public void setFieldNames(String[] fieldNames) {
    this.fieldNames = fieldNames;
  }

  public int getBatchSize() {
    return databaseType == DatabaseType.MYSQL ? Integer.MIN_VALUE : batchSize;
  }

  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }

  public boolean useUpperCaseTableName() {
    return databaseType == DatabaseType.ORACLE && useUpperCase;
  }

  public void setUseUpperCase(boolean useUpperCase) {
    this.useUpperCase = useUpperCase;
  }
}
