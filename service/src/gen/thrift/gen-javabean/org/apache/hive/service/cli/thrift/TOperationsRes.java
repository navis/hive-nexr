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

public class TOperationsRes implements org.apache.thrift.TBase<TOperationsRes, TOperationsRes._Fields>, java.io.Serializable, Cloneable {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("TOperationsRes");

  private static final org.apache.thrift.protocol.TField OPERATIONS_FIELD_DESC = new org.apache.thrift.protocol.TField("operations", org.apache.thrift.protocol.TType.LIST, (short)1);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new TOperationsResStandardSchemeFactory());
    schemes.put(TupleScheme.class, new TOperationsResTupleSchemeFactory());
  }

  private List<TOperationInfo> operations; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    OPERATIONS((short)1, "operations");

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
        case 1: // OPERATIONS
          return OPERATIONS;
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
  private _Fields optionals[] = {_Fields.OPERATIONS};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.OPERATIONS, new org.apache.thrift.meta_data.FieldMetaData("operations", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, TOperationInfo.class))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(TOperationsRes.class, metaDataMap);
  }

  public TOperationsRes() {
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public TOperationsRes(TOperationsRes other) {
    if (other.isSetOperations()) {
      List<TOperationInfo> __this__operations = new ArrayList<TOperationInfo>();
      for (TOperationInfo other_element : other.operations) {
        __this__operations.add(new TOperationInfo(other_element));
      }
      this.operations = __this__operations;
    }
  }

  public TOperationsRes deepCopy() {
    return new TOperationsRes(this);
  }

  @Override
  public void clear() {
    this.operations = null;
  }

  public int getOperationsSize() {
    return (this.operations == null) ? 0 : this.operations.size();
  }

  public java.util.Iterator<TOperationInfo> getOperationsIterator() {
    return (this.operations == null) ? null : this.operations.iterator();
  }

  public void addToOperations(TOperationInfo elem) {
    if (this.operations == null) {
      this.operations = new ArrayList<TOperationInfo>();
    }
    this.operations.add(elem);
  }

  public List<TOperationInfo> getOperations() {
    return this.operations;
  }

  public void setOperations(List<TOperationInfo> operations) {
    this.operations = operations;
  }

  public void unsetOperations() {
    this.operations = null;
  }

  /** Returns true if field operations is set (has been assigned a value) and false otherwise */
  public boolean isSetOperations() {
    return this.operations != null;
  }

  public void setOperationsIsSet(boolean value) {
    if (!value) {
      this.operations = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case OPERATIONS:
      if (value == null) {
        unsetOperations();
      } else {
        setOperations((List<TOperationInfo>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case OPERATIONS:
      return getOperations();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case OPERATIONS:
      return isSetOperations();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof TOperationsRes)
      return this.equals((TOperationsRes)that);
    return false;
  }

  public boolean equals(TOperationsRes that) {
    if (that == null)
      return false;

    boolean this_present_operations = true && this.isSetOperations();
    boolean that_present_operations = true && that.isSetOperations();
    if (this_present_operations || that_present_operations) {
      if (!(this_present_operations && that_present_operations))
        return false;
      if (!this.operations.equals(that.operations))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    HashCodeBuilder builder = new HashCodeBuilder();

    boolean present_operations = true && (isSetOperations());
    builder.append(present_operations);
    if (present_operations)
      builder.append(operations);

    return builder.toHashCode();
  }

  public int compareTo(TOperationsRes other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    TOperationsRes typedOther = (TOperationsRes)other;

    lastComparison = Boolean.valueOf(isSetOperations()).compareTo(typedOther.isSetOperations());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetOperations()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.operations, typedOther.operations);
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
    StringBuilder sb = new StringBuilder("TOperationsRes(");
    boolean first = true;

    if (isSetOperations()) {
      sb.append("operations:");
      if (this.operations == null) {
        sb.append("null");
      } else {
        sb.append(this.operations);
      }
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
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
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class TOperationsResStandardSchemeFactory implements SchemeFactory {
    public TOperationsResStandardScheme getScheme() {
      return new TOperationsResStandardScheme();
    }
  }

  private static class TOperationsResStandardScheme extends StandardScheme<TOperationsRes> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, TOperationsRes struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // OPERATIONS
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list162 = iprot.readListBegin();
                struct.operations = new ArrayList<TOperationInfo>(_list162.size);
                for (int _i163 = 0; _i163 < _list162.size; ++_i163)
                {
                  TOperationInfo _elem164; // required
                  _elem164 = new TOperationInfo();
                  _elem164.read(iprot);
                  struct.operations.add(_elem164);
                }
                iprot.readListEnd();
              }
              struct.setOperationsIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, TOperationsRes struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.operations != null) {
        if (struct.isSetOperations()) {
          oprot.writeFieldBegin(OPERATIONS_FIELD_DESC);
          {
            oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, struct.operations.size()));
            for (TOperationInfo _iter165 : struct.operations)
            {
              _iter165.write(oprot);
            }
            oprot.writeListEnd();
          }
          oprot.writeFieldEnd();
        }
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class TOperationsResTupleSchemeFactory implements SchemeFactory {
    public TOperationsResTupleScheme getScheme() {
      return new TOperationsResTupleScheme();
    }
  }

  private static class TOperationsResTupleScheme extends TupleScheme<TOperationsRes> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, TOperationsRes struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetOperations()) {
        optionals.set(0);
      }
      oprot.writeBitSet(optionals, 1);
      if (struct.isSetOperations()) {
        {
          oprot.writeI32(struct.operations.size());
          for (TOperationInfo _iter166 : struct.operations)
          {
            _iter166.write(oprot);
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, TOperationsRes struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(1);
      if (incoming.get(0)) {
        {
          org.apache.thrift.protocol.TList _list167 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
          struct.operations = new ArrayList<TOperationInfo>(_list167.size);
          for (int _i168 = 0; _i168 < _list167.size; ++_i168)
          {
            TOperationInfo _elem169; // required
            _elem169 = new TOperationInfo();
            _elem169.read(iprot);
            struct.operations.add(_elem169);
          }
        }
        struct.setOperationsIsSet(true);
      }
    }
  }

}
