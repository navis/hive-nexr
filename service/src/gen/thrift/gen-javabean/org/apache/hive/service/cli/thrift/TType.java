/**
 * Autogenerated by Thrift Compiler (0.9.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.hive.service.cli.thrift;


import java.util.Map;
import java.util.HashMap;
import org.apache.thrift.TEnum;

public enum TType implements org.apache.thrift.TEnum {
  VOID_TYPE(0),
  BOOLEAN_TYPE(1),
  TINYINT_TYPE(2),
  SMALLINT_TYPE(3),
  INT_TYPE(4),
  BIGINT_TYPE(5),
  FLOAT_TYPE(6),
  DOUBLE_TYPE(7),
  STRING_TYPE(8),
  TIMESTAMP_TYPE(9),
  BINARY_TYPE(10),
  ARRAY_TYPE(11),
  MAP_TYPE(12),
  STRUCT_TYPE(13),
  UNION_TYPE(14),
  USER_DEFINED_TYPE(15);

  private final int value;

  private TType(int value) {
    this.value = value;
  }

  /**
   * Get the integer value of this enum value, as defined in the Thrift IDL.
   */
  public int getValue() {
    return value;
  }

  /**
   * Find a the enum type by its integer value, as defined in the Thrift IDL.
   * @return null if the value is not found.
   */
  public static TType findByValue(int value) { 
    switch (value) {
      case 0:
        return VOID_TYPE;
      case 1:
        return BOOLEAN_TYPE;
      case 2:
        return TINYINT_TYPE;
      case 3:
        return SMALLINT_TYPE;
      case 4:
        return INT_TYPE;
      case 5:
        return BIGINT_TYPE;
      case 6:
        return FLOAT_TYPE;
      case 7:
        return DOUBLE_TYPE;
      case 8:
        return STRING_TYPE;
      case 9:
        return TIMESTAMP_TYPE;
      case 10:
        return BINARY_TYPE;
      case 11:
        return ARRAY_TYPE;
      case 12:
        return MAP_TYPE;
      case 13:
        return STRUCT_TYPE;
      case 14:
        return UNION_TYPE;
      case 15:
        return USER_DEFINED_TYPE;
      default:
        return null;
    }
  }
}
