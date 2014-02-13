package org.apache.hadoop.hive.rdbms;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.rdbms.db.DatabaseProperties;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;

public class ConfigurationUtils {

  public static final String HIVE_JDBC_UPDATE_ON_DUPLICATE = "hive.jdbc.update.on.duplicate";
  public static final String HIVE_JDBC_PRIMARY_KEY_FIELDS = "hive.jdbc.primary.key.fields";
  public static final String HIVE_JDBC_INPUT_COLUMNS_MAPPING = "hive.jdbc.input.columns.mapping";
  public static final String HIVE_JDBC_OUTPUT_COLUMNS_MAPPING = "hive.jdbc.output.columns.mapping";
  public static final String HIVE_JDBC_OUTPUT_SQL_QUERY_BEFORE_DATA_INSERT = "hive.jdbc.sql.query.before.data.insert";

  public static final String HIVE_JDBC_TABLE_CREATE_QUERY = "hive.jdbc.table.create.query";
  public static final String HIVE_JDBC_OUTPUT_UPSERT_QUERY = "hive.jdbc.output.upsert.query";
  public static final String HIVE_JDBC_UPSERT_QUERY_VALUES_ORDER = "hive.jdbc.upsert.query.values.order";

  public static final String HIVE_JDBC_MAXROW_PER_TASK = "hive.jdbc.maxrow.per.task";
  public static final int HIVE_JDBC_MAXROW_PER_TASK_DEFAULT = -1;
  public static final String HIVE_JDBC_MINROW_PER_TASK = "hive.jdbc.minrow.per.task";
  public static final int HIVE_JDBC_MINROW_PER_TASK_DEFAULT = -1;

  public static final String HIVE_JDBC_INPUT_BATCH_SIZE = "hive.jdbc.input.batch.size";
  public static final int HIVE_JDBC_INPUT_BATCH_SIZE_DEFAULT = 1000;
  public static final String HIVE_JDBC_OUTPUT_BATCH_SIZE = "hive.jdbc.output.batch.size";
  public static final int HIVE_JDBC_OUTPUT_BATCH_SIZE_DEFAULT = 1000;

  public static final String HIVE_JDBC_ENABLE_FILTER_PUSHDOWN = "hive.jdbc.enable.filter.pushdown";
  public static final boolean HIVE_JDBC_ENABLE_FILTER_PUSHDOWN_DEFAULT = false;

  public static final String HIVE_JDBC_ORACLE_USE_UPPERCASE_TABLENAME = "hive.jdbc.oracle.use.uppercase.tablename";
  public static final boolean HIVE_JDBC_ORACLE_USE_UPPERCASE_TABLENAME_DEFAULT = false;

  // serdeConstants
  public static final String LIST_COLUMNS = "columns";
  public static final String LIST_COLUMN_TYPES = "columns.types";

  // hive_metastoreConstants
  public static final String META_TABLE_NAME = "name";

  public static final List<String> ALL_PROPERTIES = Arrays.asList(
      DBConfiguration.DRIVER_CLASS_PROPERTY,
      DBConfiguration.USERNAME_PROPERTY,
      DBConfiguration.PASSWORD_PROPERTY,
      DBConfiguration.URL_PROPERTY,
      DBConfiguration.INPUT_TABLE_NAME_PROPERTY,
      DBConfiguration.OUTPUT_TABLE_NAME_PROPERTY,
      DBConfiguration.OUTPUT_FIELD_NAMES_PROPERTY,
      META_TABLE_NAME,
      HIVE_JDBC_UPDATE_ON_DUPLICATE,
      HIVE_JDBC_PRIMARY_KEY_FIELDS,
      HIVE_JDBC_INPUT_COLUMNS_MAPPING,
      HIVE_JDBC_OUTPUT_COLUMNS_MAPPING,
      HIVE_JDBC_TABLE_CREATE_QUERY,
      HIVE_JDBC_OUTPUT_UPSERT_QUERY,
      HIVE_JDBC_UPSERT_QUERY_VALUES_ORDER,
      HIVE_JDBC_OUTPUT_SQL_QUERY_BEFORE_DATA_INSERT,
      HIVE_JDBC_MAXROW_PER_TASK,
      HIVE_JDBC_MINROW_PER_TASK,
      HIVE_JDBC_INPUT_BATCH_SIZE,
      HIVE_JDBC_OUTPUT_BATCH_SIZE,
      HIVE_JDBC_ENABLE_FILTER_PUSHDOWN);

  public static final List<String> UPDATABLE_PROPERTIES = Arrays.asList(
      HIVE_JDBC_INPUT_COLUMNS_MAPPING,
      HIVE_JDBC_OUTPUT_COLUMNS_MAPPING,
      HIVE_JDBC_OUTPUT_SQL_QUERY_BEFORE_DATA_INSERT,
      HIVE_JDBC_MAXROW_PER_TASK,
      HIVE_JDBC_MINROW_PER_TASK,
      HIVE_JDBC_INPUT_BATCH_SIZE,
      HIVE_JDBC_OUTPUT_BATCH_SIZE,
      HIVE_JDBC_ENABLE_FILTER_PUSHDOWN
  );

  public static void copyJDBCProperties(Properties from, Map<String, String> to, boolean output) {
    for (String key : ALL_PROPERTIES) {
      String value = from.getProperty(key);
      if (value != null) {
        to.put(key, value);
      }
    }
    if (output) {
      to.put(LIST_COLUMNS, from.getProperty(LIST_COLUMNS));
      to.put(LIST_COLUMN_TYPES, from.getProperty(LIST_COLUMN_TYPES));
    }
  }

  public final static String getOutputTableName(Configuration configuration) {
    return getTableName(configuration, DBConfiguration.OUTPUT_TABLE_NAME_PROPERTY);
  }

  public final static String getInputTableName(Configuration configuration) {
    return getTableName(configuration, DBConfiguration.INPUT_TABLE_NAME_PROPERTY);
  }

  private static String getTableName(Configuration configuration, String key) {
    String tableName = configuration.get(key);
    if (tableName == null) {
      String createTableQuery = configuration.get(HIVE_JDBC_TABLE_CREATE_QUERY);
      if (createTableQuery != null) {
        tableName = extractingTableNameFromQuery(createTableQuery);
      } else {
        // assign the meta table name
        tableName = configuration.get(META_TABLE_NAME);
        if (tableName.contains("default.")) {
          tableName = tableName.replace("default.", "");
        }
      }
    }
    return tableName;
  }

  public final static String getConnectionUrl(Configuration conf) {
    return conf.get(DBConfiguration.URL_PROPERTY);
  }

  public final static String getDriverClass(Configuration conf) {
    return conf.get(DBConfiguration.DRIVER_CLASS_PROPERTY);
  }

  public final static String getDatabaseUserName(Configuration conf) {
    return conf.get(DBConfiguration.USERNAME_PROPERTY);
  }

  public final static String getDatabasePassword(Configuration conf) {
    return conf.get(DBConfiguration.PASSWORD_PROPERTY);
  }

  public final static Map<String, String> getHiveToDB(Configuration conf) {
    String inputFieldNames = conf.get(HIVE_JDBC_INPUT_COLUMNS_MAPPING);
    if (inputFieldNames == null) {
      return null;
    }
    Map<String, String> mapFields = new LinkedHashMap<String, String>();

    // Field name getting from hive meta table
    String[] fieldNames = conf.get(LIST_COLUMNS).split(",");
    List<String> names = new ArrayList<String>(Arrays.asList(fieldNames));
    names.remove(VirtualColumn.FILENAME.getName());
    names.remove(VirtualColumn.BLOCKOFFSET.getName());
    names.remove(VirtualColumn.RAWDATASIZE.getName());
    names.remove(VirtualColumn.ROWOFFSET.getName());

    fieldNames = names.toArray(new String[names.size()]);

    String[] dbTableFieldName = trim(inputFieldNames.split(","));
    if (dbTableFieldName.length != fieldNames.length) {
      throw new IllegalArgumentException("hive.jdbc.input.columns.mapping size " +
          dbTableFieldName.length + " doesn't match with no of hive meta table columns, " +
          "which is " + fieldNames.length);
    }
    for (int i = 0; i < dbTableFieldName.length; i++) {
      mapFields.put(fieldNames[i], dbTableFieldName[i]);
    }
    return mapFields;
  }

  public final static String[] getColumnMappingFields(Configuration conf) {
    String mappingFields = conf.get(HIVE_JDBC_OUTPUT_COLUMNS_MAPPING);
    if (mappingFields != null) {
      return trim(mappingFields.split(","));
    }
    return null;
  }

  public static PrimitiveCategory[] toTypes(String[] types) {
    PrimitiveCategory[] categories = new PrimitiveCategory[types.length];
    for (int i = 0; i < types.length; i++) {
      PrimitiveObjectInspectorUtils.PrimitiveTypeEntry entry =
          PrimitiveObjectInspectorUtils.getTypeEntryFromTypeName(types[i]);
      if (entry == null) {
        throw new IllegalArgumentException("Invalid primitive type " + types[i]);
      }
      categories[i] = entry.primitiveCategory;
    }
    return categories;
  }

  public static PrimitiveObjectInspector[] toPrimitiveJavaOIs(PrimitiveCategory[] categories) {
    PrimitiveObjectInspector[] inspectors = new PrimitiveObjectInspector[categories.length];
    for (int i = 0; i < categories.length; i++) {
      inspectors[i] =
          PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(categories[i]);
    }
    return inspectors;
  }


  /**
   * Trim the white spaces, new lines from the input array.
   *
   * @param input a input string array
   * @return a trimmed string array
   */
  private static String[] trim(String[] input) {
    String[] trimmed = new String[input.length];
    for (int i = 0; i < input.length; i++) {
      trimmed[i] = input[i].trim();
    }
    return trimmed;
  }

  // If user has given the query for creating table, we don't need to ask the table name again.
  // Get the table name from the query
  public final static String extractingTableNameFromQuery(String createTableQuery) {
    String tableName = null;
    if (createTableQuery != null) {
      List<String> queryList = Arrays.asList(createTableQuery.split(" "));
      Iterator<String> iterator = queryList.iterator();
      while (iterator.hasNext()) {
        if (iterator.next().equalsIgnoreCase("create")) {
          while (iterator.hasNext()) {
            if (iterator.next().equalsIgnoreCase("table")) {
              while (iterator.hasNext()) {
                String nextString = iterator.next();
                if (!nextString.equalsIgnoreCase("")) {
                  // create table foo(bar varchar(100)); split("\\(") needed for fixing
                  // the issue If we get a create query like this.
                  tableName = nextString.split("\\(")[0];
                  return tableName;
                }
              }
            }
          }
        }

      }
    } else {
      throw new IllegalArgumentException("You should provide at least "
          + DBConfiguration.OUTPUT_TABLE_NAME_PROPERTY + " or "
          + HIVE_JDBC_TABLE_CREATE_QUERY + " property.");
    }
    return tableName;
  }

  // get database properties obj.
  public static DatabaseProperties getDbPropertiesObj(JobConf conf) {
    DatabaseProperties dbProperties = new DatabaseProperties();
    String connectionUrl = getConnectionUrl(conf);
    connectionUrl = connectionUrl.replaceAll(" ", "");
    dbProperties.setConnectionUrl(connectionUrl);
    dbProperties.setDriverClass(getDriverClass(conf));
    dbProperties.setUserName(getDatabaseUserName(conf));
    dbProperties.setPassword(getDatabasePassword(conf));
    return dbProperties;
  }
}
