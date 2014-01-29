package org.apache.hadoop.hive.serde2.lazy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.FieldRewriter;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyObjectInspectorParameters;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hive.common.util.HiveStringUtils;

/**
 * SerDeParameters.
 *
 */
public class LazySerDeParameters implements LazyObjectInspectorParameters {
  public static final byte[] DefaultSeparators = {(byte) 1, (byte) 2, (byte) 3};
  public static final String SERIALIZATION_EXTEND_NESTING_LEVELS
  	= "hive.serialization.extend.nesting.levels";
  public static final String SERIALIZATION_EXTEND_ADDITIONAL_NESTING_LEVELS
	= "hive.serialization.extend.additional.nesting.levels";

  private Properties tableProperties;
  private String serdeName;

  // The list of bytes used for the separators in the column (a nested struct 
  // such as Array<Array<int>> will use multiple separators).
  // The list of separators + escapeChar are the bytes required to be escaped.
  private byte[] separators;	

  private String nullString;
  private Text nullSequence;
  private TypeInfo rowTypeInfo;
  private boolean lastColumnTakesRest;
  private List<String> columnNames;
  private List<TypeInfo> columnTypes;

  private boolean escaped;
  private byte escapeChar;
  private boolean[] needsEscape = new boolean[256];  // A flag for each byte to indicate if escape is needed. 

  private boolean extendedBooleanLiteral;
  List<String> timestampFormats;

  boolean[] needEncoding;

  public FieldRewriter rewriter;
  public transient final ByteStream.Input input = new ByteStream.Input();
  public transient final ByteStream.Output output = new ByteStream.Output();
  
  public LazySerDeParameters(Configuration job, Properties tbl, String serdeName) throws SerDeException {
   this.tableProperties = tbl;
   this.serdeName = serdeName;
  	
    nullString = tbl.getProperty(
        serdeConstants.SERIALIZATION_NULL_FORMAT, "\\N");
    nullSequence = new Text(nullString);
    
    String lastColumnTakesRestString = tbl
        .getProperty(serdeConstants.SERIALIZATION_LAST_COLUMN_TAKES_REST);
    lastColumnTakesRest = (lastColumnTakesRestString != null && lastColumnTakesRestString
        .equalsIgnoreCase("true"));

    extractColumnInfo();

    // Create the LazyObject for storing the rows
    rowTypeInfo = TypeInfoFactory.getStructTypeInfo(columnNames, columnTypes);

    collectSeparators(tbl);
  	
    // Get the escape information      
    String escapeProperty = tbl.getProperty(serdeConstants.ESCAPE_CHAR);
    escaped = (escapeProperty != null);
    if (escaped) {
      escapeChar = LazyUtils.getByte(escapeProperty, (byte) '\\');
      needsEscape[escapeChar & 0xFF] = true;  // Converts the negative byte into positive index
      for (byte b : separators) {
        needsEscape[b & 0xFF] = true;         // Converts the negative byte into positive index
      }
    }
    
    extendedBooleanLiteral = (job == null ? false :
        job.getBoolean(ConfVars.HIVE_LAZYSIMPLE_EXTENDED_BOOLEAN_LITERAL.varname, false));
    
    String[] timestampFormatsArray =
        HiveStringUtils.splitAndUnEscape(tbl.getProperty(serdeConstants.TIMESTAMP_FORMATS));
    if (timestampFormatsArray != null) {
      timestampFormats = Arrays.asList(timestampFormatsArray);
    }

    String encodeIndices = tbl.getProperty(serdeConstants.COLUMN_ENCODE_INDICES);
    if (encodeIndices != null) {
      TreeSet<Integer> indices = new TreeSet<Integer>();
      for (String index : encodeIndices.split(",")) {
        indices.add(Integer.parseInt(index.trim()));
      }
      needEncoding = new boolean[columnNames.size()];
      for (int index : indices) {
        needEncoding[index] = true;
      }
    }
    String encodeColumns = tbl.getProperty(serdeConstants.COLUMN_ENCODE_COLUMNS);
    if (encodeColumns != null) {
      if (needEncoding == null) {
        needEncoding = new boolean[columnNames.size()];
      }
      for (String column : encodeColumns.split(",")) {
        needEncoding[findIndex(columnNames, column.trim())] = true;
      }
    }

    String encoderClass = tbl.getProperty(serdeConstants.COLUMN_ENCODE_CLASSNAME);
    if (encoderClass != null) {
      rewriter = createRewriter(encoderClass, tbl, job);
    }
    if (needEncoding != null && rewriter == null) {
      throw new SerDeException("Encoder is not specified by serde property 'column.encode.classname'");
    }
  }

  private int findIndex(List<String> columnNames, String column) {
    for (int i = 0; i < columnNames.size(); i++) {
      if (columnNames.get(i).equals(column)) {
        return i;
      }
    }
    throw new IllegalArgumentException("Invalid column name " + column + " in " + columnNames);
  }

  private FieldRewriter createRewriter(String encoderClass, Properties properties, 
      Configuration job) throws SerDeException {
    try {
      FieldRewriter rewriter =
          (FieldRewriter) ReflectionUtils.newInstance(Class.forName(encoderClass), job);
      rewriter.init(columnNames, columnTypes, properties);
      return rewriter;
    } catch (Exception e) {
      throw new SerDeException(e);
    }
  }

  /**
   * Extracts and set column names and column types from the table properties
   * @throws SerDeException
   */
  public void extractColumnInfo() throws SerDeException {
    // Read the configuration parameters
    String columnNameProperty = tableProperties.getProperty(serdeConstants.LIST_COLUMNS);
    // NOTE: if "columns.types" is missing, all columns will be of String type
    String columnTypeProperty = tableProperties.getProperty(serdeConstants.LIST_COLUMN_TYPES);

    // Parse the configuration parameters

    if (columnNameProperty != null && columnNameProperty.length() > 0) {
      columnNames = Arrays.asList(columnNameProperty.split(","));
    } else {
      columnNames = new ArrayList<String>();
    }
    if (columnTypeProperty == null) {
      // Default type: all string
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < columnNames.size(); i++) {
        if (i > 0) {
          sb.append(":");
        }
        sb.append(serdeConstants.STRING_TYPE_NAME);
      }
      columnTypeProperty = sb.toString();
    }

    columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);

    if (columnNames.size() != columnTypes.size()) {
      throw new SerDeException(serdeName + ": columns has " + columnNames.size()
          + " elements while columns.types has " + columnTypes.size() + " elements!");
    }
  }
  
  public List<TypeInfo> getColumnTypes() {
    return columnTypes;
  }

  public List<String> getColumnNames() {
    return columnNames;
  }

  public byte[] getSeparators() {   	
    return separators;
  }

  public String getNullString() {
    return nullString;
  }

  public Text getNullSequence() {
    return nullSequence;
  }

  public TypeInfo getRowTypeInfo() {
    return rowTypeInfo;
  }

  public boolean isLastColumnTakesRest() {
    return lastColumnTakesRest;
  }

  public boolean isEscaped() {
    return escaped;
  }

  public byte getEscapeChar() {
    return escapeChar;
  }

  public boolean[] getNeedsEscape() {
    return needsEscape;
  }

  public boolean isExtendedBooleanLiteral() {
    return extendedBooleanLiteral;
  }

  public List<String> getTimestampFormats() {
    return timestampFormats;
  }


  public boolean isEncoded(int index) {
    return needEncoding != null && index >= 0 && needEncoding[index];
  }

  public void encode(int index, ByteStream.Output out, int pos) throws IOException {
    input.reset(out.getData(), pos, out.getCount() - pos);
    output.reset();
    rewriter.encode(index, input, output);
    out.writeTo(pos, output);
  }

  public void decode(int index, byte[] bytes, int start, int length) throws IOException {
    input.reset(bytes, start, length);
    output.reset();
    rewriter.decode(index, input, output);
  }

  public void setSeparator(int index, byte separator) throws SerDeException {
    if (index < 0 || index >= separators.length) {
      throw new SerDeException("Invalid separator array index value: " + index);
    }

    separators[index] = separator;
  }

  /**
   * To be backward-compatible, initialize the first 3 separators to 
   * be the given values from the table properties. The default number of separators is 8; if only
   * hive.serialization.extend.nesting.levels is set, the number of
   * separators is extended to 24; if hive.serialization.extend.additional.nesting.levels
   * is set, the number of separators is extended to 154.
   * @param tableProperties table properties to extract the user provided separators
   */
  private void collectSeparators(Properties tableProperties) {	
    List<Byte> separatorCandidates = new ArrayList<Byte>();

    String extendNestingValue = tableProperties.getProperty(SERIALIZATION_EXTEND_NESTING_LEVELS);
    String extendAdditionalNestingValue = tableProperties.getProperty(SERIALIZATION_EXTEND_ADDITIONAL_NESTING_LEVELS);
    boolean extendedNesting = extendNestingValue != null && extendNestingValue.equalsIgnoreCase("true");
    boolean extendedAdditionalNesting = extendAdditionalNestingValue != null 
    		&& extendAdditionalNestingValue.equalsIgnoreCase("true");

    separatorCandidates.add(LazyUtils.getByte(tableProperties.getProperty(serdeConstants.FIELD_DELIM,
        tableProperties.getProperty(serdeConstants.SERIALIZATION_FORMAT)), DefaultSeparators[0]));
    separatorCandidates.add(LazyUtils.getByte(tableProperties
        .getProperty(serdeConstants.COLLECTION_DELIM), DefaultSeparators[1]));
    separatorCandidates.add(LazyUtils.getByte(
        tableProperties.getProperty(serdeConstants.MAPKEY_DELIM), DefaultSeparators[2]));
    
    //use only control chars that are very unlikely to be part of the string
    // the following might/likely to be used in text files for strings
    // 9 (horizontal tab, HT, \t, ^I)
    // 10 (line feed, LF, \n, ^J),
    // 12 (form feed, FF, \f, ^L),
    // 13 (carriage return, CR, \r, ^M),
    // 27 (escape, ESC, \e [GCC only], ^[).
    for (byte b = 4; b <= 8; b++ ) {
    	separatorCandidates.add(b);
    }
    separatorCandidates.add((byte)11);
    for (byte b = 14; b <= 26; b++ ) {
      separatorCandidates.add(b);
    }
    for (byte b = 28; b <= 31; b++ ) {
      separatorCandidates.add(b);
    }

    for (byte b = -128; b <= -1; b++ ) {
      separatorCandidates.add(b);
    }

    int numSeparators = 8;
    if(extendedAdditionalNesting) {
      numSeparators = separatorCandidates.size();
    } else if (extendedNesting) {
      numSeparators = 24;
    }

    separators = new byte[numSeparators];
    for (int i = 0; i < numSeparators; i++) {
      separators[i] = separatorCandidates.get(i);
    }
  }
}
