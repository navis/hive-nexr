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

public class TSessionsRes implements org.apache.thrift.TBase<TSessionsRes, TSessionsRes._Fields>, java.io.Serializable, Cloneable {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("TSessionsRes");

  private static final org.apache.thrift.protocol.TField SESSIONS_FIELD_DESC = new org.apache.thrift.protocol.TField("sessions", org.apache.thrift.protocol.TType.LIST, (short)1);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new TSessionsResStandardSchemeFactory());
    schemes.put(TupleScheme.class, new TSessionsResTupleSchemeFactory());
  }

  private List<TSessionInfo> sessions; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    SESSIONS((short)1, "sessions");

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
        case 1: // SESSIONS
          return SESSIONS;
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
  private _Fields optionals[] = {_Fields.SESSIONS};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.SESSIONS, new org.apache.thrift.meta_data.FieldMetaData("sessions", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, TSessionInfo.class))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(TSessionsRes.class, metaDataMap);
  }

  public TSessionsRes() {
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public TSessionsRes(TSessionsRes other) {
    if (other.isSetSessions()) {
      List<TSessionInfo> __this__sessions = new ArrayList<TSessionInfo>();
      for (TSessionInfo other_element : other.sessions) {
        __this__sessions.add(new TSessionInfo(other_element));
      }
      this.sessions = __this__sessions;
    }
  }

  public TSessionsRes deepCopy() {
    return new TSessionsRes(this);
  }

  @Override
  public void clear() {
    this.sessions = null;
  }

  public int getSessionsSize() {
    return (this.sessions == null) ? 0 : this.sessions.size();
  }

  public java.util.Iterator<TSessionInfo> getSessionsIterator() {
    return (this.sessions == null) ? null : this.sessions.iterator();
  }

  public void addToSessions(TSessionInfo elem) {
    if (this.sessions == null) {
      this.sessions = new ArrayList<TSessionInfo>();
    }
    this.sessions.add(elem);
  }

  public List<TSessionInfo> getSessions() {
    return this.sessions;
  }

  public void setSessions(List<TSessionInfo> sessions) {
    this.sessions = sessions;
  }

  public void unsetSessions() {
    this.sessions = null;
  }

  /** Returns true if field sessions is set (has been assigned a value) and false otherwise */
  public boolean isSetSessions() {
    return this.sessions != null;
  }

  public void setSessionsIsSet(boolean value) {
    if (!value) {
      this.sessions = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case SESSIONS:
      if (value == null) {
        unsetSessions();
      } else {
        setSessions((List<TSessionInfo>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case SESSIONS:
      return getSessions();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case SESSIONS:
      return isSetSessions();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof TSessionsRes)
      return this.equals((TSessionsRes)that);
    return false;
  }

  public boolean equals(TSessionsRes that) {
    if (that == null)
      return false;

    boolean this_present_sessions = true && this.isSetSessions();
    boolean that_present_sessions = true && that.isSetSessions();
    if (this_present_sessions || that_present_sessions) {
      if (!(this_present_sessions && that_present_sessions))
        return false;
      if (!this.sessions.equals(that.sessions))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    HashCodeBuilder builder = new HashCodeBuilder();

    boolean present_sessions = true && (isSetSessions());
    builder.append(present_sessions);
    if (present_sessions)
      builder.append(sessions);

    return builder.toHashCode();
  }

  public int compareTo(TSessionsRes other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    TSessionsRes typedOther = (TSessionsRes)other;

    lastComparison = Boolean.valueOf(isSetSessions()).compareTo(typedOther.isSetSessions());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetSessions()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.sessions, typedOther.sessions);
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
    StringBuilder sb = new StringBuilder("TSessionsRes(");
    boolean first = true;

    if (isSetSessions()) {
      sb.append("sessions:");
      if (this.sessions == null) {
        sb.append("null");
      } else {
        sb.append(this.sessions);
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

  private static class TSessionsResStandardSchemeFactory implements SchemeFactory {
    public TSessionsResStandardScheme getScheme() {
      return new TSessionsResStandardScheme();
    }
  }

  private static class TSessionsResStandardScheme extends StandardScheme<TSessionsRes> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, TSessionsRes struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // SESSIONS
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list154 = iprot.readListBegin();
                struct.sessions = new ArrayList<TSessionInfo>(_list154.size);
                for (int _i155 = 0; _i155 < _list154.size; ++_i155)
                {
                  TSessionInfo _elem156; // required
                  _elem156 = new TSessionInfo();
                  _elem156.read(iprot);
                  struct.sessions.add(_elem156);
                }
                iprot.readListEnd();
              }
              struct.setSessionsIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, TSessionsRes struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.sessions != null) {
        if (struct.isSetSessions()) {
          oprot.writeFieldBegin(SESSIONS_FIELD_DESC);
          {
            oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, struct.sessions.size()));
            for (TSessionInfo _iter157 : struct.sessions)
            {
              _iter157.write(oprot);
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

  private static class TSessionsResTupleSchemeFactory implements SchemeFactory {
    public TSessionsResTupleScheme getScheme() {
      return new TSessionsResTupleScheme();
    }
  }

  private static class TSessionsResTupleScheme extends TupleScheme<TSessionsRes> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, TSessionsRes struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetSessions()) {
        optionals.set(0);
      }
      oprot.writeBitSet(optionals, 1);
      if (struct.isSetSessions()) {
        {
          oprot.writeI32(struct.sessions.size());
          for (TSessionInfo _iter158 : struct.sessions)
          {
            _iter158.write(oprot);
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, TSessionsRes struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(1);
      if (incoming.get(0)) {
        {
          org.apache.thrift.protocol.TList _list159 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
          struct.sessions = new ArrayList<TSessionInfo>(_list159.size);
          for (int _i160 = 0; _i160 < _list159.size; ++_i160)
          {
            TSessionInfo _elem161; // required
            _elem161 = new TSessionInfo();
            _elem161.read(iprot);
            struct.sessions.add(_elem161);
          }
        }
        struct.setSessionsIsSet(true);
      }
    }
  }

}

