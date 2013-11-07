/**
 * Autogenerated by Thrift Compiler (0.9.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.hive.service.cli.thrift;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.EncodingUtils;
import org.apache.thrift.TException;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TRowSet implements org.apache.thrift.TBase<TRowSet, TRowSet._Fields>, java.io.Serializable, Cloneable {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("TRowSet");

  private static final org.apache.thrift.protocol.TField START_ROW_OFFSET_FIELD_DESC = new org.apache.thrift.protocol.TField("startRowOffset", org.apache.thrift.protocol.TType.I64, (short)1);
  private static final org.apache.thrift.protocol.TField COL_VALS_FIELD_DESC = new org.apache.thrift.protocol.TField("colVals", org.apache.thrift.protocol.TType.LIST, (short)2);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new TRowSetStandardSchemeFactory());
    schemes.put(TupleScheme.class, new TRowSetTupleSchemeFactory());
  }

  private long startRowOffset; // required
  private List<TColumnValue> colVals; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    START_ROW_OFFSET((short)1, "startRowOffset"),
    COL_VALS((short)2, "colVals");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // START_ROW_OFFSET
          return START_ROW_OFFSET;
        case 2: // COL_VALS
          return COL_VALS;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final int __STARTROWOFFSET_ISSET_ID = 0;
  private byte __isset_bitfield = 0;
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.START_ROW_OFFSET, new org.apache.thrift.meta_data.FieldMetaData("startRowOffset", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.COL_VALS, new org.apache.thrift.meta_data.FieldMetaData("colVals", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, TColumnValue.class))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(TRowSet.class, metaDataMap);
  }

  public TRowSet() {
  }

  public TRowSet(
    long startRowOffset,
    List<TColumnValue> colVals)
  {
    this();
    this.startRowOffset = startRowOffset;
    setStartRowOffsetIsSet(true);
    this.colVals = colVals;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public TRowSet(TRowSet other) {
    __isset_bitfield = other.__isset_bitfield;
    this.startRowOffset = other.startRowOffset;
    if (other.isSetColVals()) {
      List<TColumnValue> __this__colVals = new ArrayList<TColumnValue>();
      for (TColumnValue other_element : other.colVals) {
        __this__colVals.add(new TColumnValue(other_element));
      }
      this.colVals = __this__colVals;
    }
  }

  public TRowSet deepCopy() {
    return new TRowSet(this);
  }

  @Override
  public void clear() {
    setStartRowOffsetIsSet(false);
    this.startRowOffset = 0;
    this.colVals = null;
  }

  public long getStartRowOffset() {
    return this.startRowOffset;
  }

  public void setStartRowOffset(long startRowOffset) {
    this.startRowOffset = startRowOffset;
    setStartRowOffsetIsSet(true);
  }

  public void unsetStartRowOffset() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __STARTROWOFFSET_ISSET_ID);
  }

  /** Returns true if field startRowOffset is set (has been assigned a value) and false otherwise */
  public boolean isSetStartRowOffset() {
    return EncodingUtils.testBit(__isset_bitfield, __STARTROWOFFSET_ISSET_ID);
  }

  public void setStartRowOffsetIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __STARTROWOFFSET_ISSET_ID, value);
  }

  public int getColValsSize() {
    return (this.colVals == null) ? 0 : this.colVals.size();
  }

  public java.util.Iterator<TColumnValue> getColValsIterator() {
    return (this.colVals == null) ? null : this.colVals.iterator();
  }

  public void addToColVals(TColumnValue elem) {
    if (this.colVals == null) {
      this.colVals = new ArrayList<TColumnValue>();
    }
    this.colVals.add(elem);
  }

  public List<TColumnValue> getColVals() {
    return this.colVals;
  }

  public void setColVals(List<TColumnValue> colVals) {
    this.colVals = colVals;
  }

  public void unsetColVals() {
    this.colVals = null;
  }

  /** Returns true if field colVals is set (has been assigned a value) and false otherwise */
  public boolean isSetColVals() {
    return this.colVals != null;
  }

  public void setColValsIsSet(boolean value) {
    if (!value) {
      this.colVals = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case START_ROW_OFFSET:
      if (value == null) {
        unsetStartRowOffset();
      } else {
        setStartRowOffset((Long)value);
      }
      break;

    case COL_VALS:
      if (value == null) {
        unsetColVals();
      } else {
        setColVals((List<TColumnValue>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case START_ROW_OFFSET:
      return Long.valueOf(getStartRowOffset());

    case COL_VALS:
      return getColVals();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case START_ROW_OFFSET:
      return isSetStartRowOffset();
    case COL_VALS:
      return isSetColVals();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof TRowSet)
      return this.equals((TRowSet)that);
    return false;
  }

  public boolean equals(TRowSet that) {
    if (that == null)
      return false;

    boolean this_present_startRowOffset = true;
    boolean that_present_startRowOffset = true;
    if (this_present_startRowOffset || that_present_startRowOffset) {
      if (!(this_present_startRowOffset && that_present_startRowOffset))
        return false;
      if (this.startRowOffset != that.startRowOffset)
        return false;
    }

    boolean this_present_colVals = true && this.isSetColVals();
    boolean that_present_colVals = true && that.isSetColVals();
    if (this_present_colVals || that_present_colVals) {
      if (!(this_present_colVals && that_present_colVals))
        return false;
      if (!this.colVals.equals(that.colVals))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    HashCodeBuilder builder = new HashCodeBuilder();

    boolean present_startRowOffset = true;
    builder.append(present_startRowOffset);
    if (present_startRowOffset)
      builder.append(startRowOffset);

    boolean present_colVals = true && (isSetColVals());
    builder.append(present_colVals);
    if (present_colVals)
      builder.append(colVals);

    return builder.toHashCode();
  }

  public int compareTo(TRowSet other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    TRowSet typedOther = (TRowSet)other;

    lastComparison = Boolean.valueOf(isSetStartRowOffset()).compareTo(typedOther.isSetStartRowOffset());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetStartRowOffset()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.startRowOffset, typedOther.startRowOffset);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetColVals()).compareTo(typedOther.isSetColVals());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetColVals()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.colVals, typedOther.colVals);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("TRowSet(");
    boolean first = true;

    sb.append("startRowOffset:");
    sb.append(this.startRowOffset);
    first = false;
    if (!first) sb.append(", ");
    sb.append("colVals:");
    if (this.colVals == null) {
      sb.append("null");
    } else {
      sb.append(this.colVals);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (!isSetStartRowOffset()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'startRowOffset' is unset! Struct:" + toString());
    }

    if (!isSetColVals()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'colVals' is unset! Struct:" + toString());
    }

    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bitfield = 0;
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class TRowSetStandardSchemeFactory implements SchemeFactory {
    public TRowSetStandardScheme getScheme() {
      return new TRowSetStandardScheme();
    }
  }

  private static class TRowSetStandardScheme extends StandardScheme<TRowSet> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, TRowSet struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // START_ROW_OFFSET
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.startRowOffset = iprot.readI64();
              struct.setStartRowOffsetIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // COL_VALS
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list100 = iprot.readListBegin();
                struct.colVals = new ArrayList<TColumnValue>(_list100.size);
                for (int _i101 = 0; _i101 < _list100.size; ++_i101)
                {
                  TColumnValue _elem102; // required
                  _elem102 = new TColumnValue();
                  _elem102.read(iprot);
                  struct.colVals.add(_elem102);
                }
                iprot.readListEnd();
              }
              struct.setColValsIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, TRowSet struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      oprot.writeFieldBegin(START_ROW_OFFSET_FIELD_DESC);
      oprot.writeI64(struct.startRowOffset);
      oprot.writeFieldEnd();
      if (struct.colVals != null) {
        oprot.writeFieldBegin(COL_VALS_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, struct.colVals.size()));
          for (TColumnValue _iter103 : struct.colVals)
          {
            _iter103.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class TRowSetTupleSchemeFactory implements SchemeFactory {
    public TRowSetTupleScheme getScheme() {
      return new TRowSetTupleScheme();
    }
  }

  private static class TRowSetTupleScheme extends TupleScheme<TRowSet> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, TRowSet struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      oprot.writeI64(struct.startRowOffset);
      {
        oprot.writeI32(struct.colVals.size());
        for (TColumnValue _iter104 : struct.colVals)
        {
          _iter104.write(oprot);
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, TRowSet struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      struct.startRowOffset = iprot.readI64();
      struct.setStartRowOffsetIsSet(true);
      {
        org.apache.thrift.protocol.TList _list105 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
        struct.colVals = new ArrayList<TColumnValue>(_list105.size);
        for (int _i106 = 0; _i106 < _list105.size; ++_i106)
        {
          TColumnValue _elem107; // required
          _elem107 = new TColumnValue();
          _elem107.read(iprot);
          struct.colVals.add(_elem107);
        }
      }
      struct.setColValsIsSet(true);
    }
  }

}

