package org.apache.hadoop.hive.rdbms;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;

import org.apache.hadoop.hive.rdbms.db.DatabaseType;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;

public class ColumnAccess {

  public final DatabaseType database;
  public final PrimitiveCategory type;
  public final int index;

  public ColumnAccess(DatabaseType database, PrimitiveCategory type, int index) {
    this.database = database;
    this.type = type;
    this.index = index;
  }

  public Object getValue(ResultSet results)
      throws IOException, SQLException {
    // Only primitive data types will be supported
    switch (type) {
      case VOID:
        return null;
      case BOOLEAN:
        if (database == DatabaseType.ORACLE) {
          // support non-spec boolean expression
          String stringVal = results.getString(index);
          return results.wasNull() ? null :
              (stringVal.equals("0") || stringVal.equals("t") || stringVal.equals("true"));
        }
        boolean boolVal = results.getBoolean(index);
        return results.wasNull() ? null : boolVal;
      case BYTE:
        byte byteVal = results.getByte(index);
        return results.wasNull() ? null : byteVal;
      case SHORT:
        short shortVal = results.getShort(index);
        return results.wasNull() ? null : shortVal;
      case INT:
        int intVal = results.getInt(index);
        return results.wasNull() ? null : intVal;
      case LONG:
        long longVal = results.getLong(index);
        return results.wasNull() ? null : longVal;
      case FLOAT:
        float floatVal = results.getFloat(index);
        return results.wasNull() ? null : floatVal;
      case DOUBLE:
        double doubleVal = results.getDouble(index);
        return results.wasNull() ? null : doubleVal;
      case STRING:
        String result = results.getString(index);
        return results.wasNull() ? null : result;
      case TIMESTAMP:
        Timestamp timestampVal = results.getTimestamp(index);
        return results.wasNull() ? null : timestampVal;
      case BINARY:
        byte[] binary = results.getBytes(index);
        return results.wasNull() ? null : binary;
      case DECIMAL:
        BigDecimal decimal = results.getBigDecimal(index);
        return results.wasNull() ? null : decimal;
      default:
        throw new IOException("Invalid type " + type);
    }
  }

  public void setValue(PreparedStatement pstmt, Object value)
      throws IOException, SQLException {
    // Only primitive data types will be supported
    switch (type) {
      case VOID:
        return;
      case BOOLEAN:
        if (value == null) {
          pstmt.setNull(index, Types.BIT);
        } else {
          pstmt.setBoolean(index, (Boolean) value);
        }
        return;
      case BYTE:
        if (value == null) {
          pstmt.setNull(index, Types.TINYINT);
        } else {
          pstmt.setByte(index, (Byte) value);
        }
        return;
      case SHORT:
        if (value == null) {
          pstmt.setNull(index, Types.SMALLINT);
        } else {
          pstmt.setShort(index, (Short) value);
        }
        return;
      case INT:
        if (value == null) {
          pstmt.setNull(index, Types.INTEGER);
        } else {
          pstmt.setInt(index, (Integer) value);
        }
        return;
      case LONG:
        if (value == null) {
          pstmt.setNull(index, Types.BIGINT);
        } else {
          pstmt.setLong(index, (Long) value);
        }
        return;
      case FLOAT:
        if (value == null) {
          pstmt.setNull(index, Types.FLOAT);
        } else {
          pstmt.setFloat(index, (Float) value);
        }
        return;
      case DOUBLE:
        if (value == null) {
          pstmt.setNull(index, Types.DOUBLE);
        } else {
          pstmt.setDouble(index, (Double) value);
        }
        return;
      case STRING:
        if (value == null) {
          pstmt.setNull(index, Types.VARCHAR);
        } else {
          pstmt.setString(index, (String) value);
        }
        return;
      case TIMESTAMP:
        if (value == null) {
          pstmt.setNull(index, Types.TIMESTAMP);
        } else {
          pstmt.setTimestamp(index, (Timestamp) value);
        }
        return;
      case BINARY:
        if (value == null) {
          pstmt.setNull(index, Types.BINARY);
        } else {
          pstmt.setBytes(index, (byte[]) value);
        }
        return;
      case DECIMAL:
        if (value == null) {
          pstmt.setNull(index, Types.DECIMAL);
        } else {
          pstmt.setBigDecimal(index, (BigDecimal) value);
        }
      default:
        throw new IOException("Invalid type " + type);
    }
  }

}
