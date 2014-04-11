/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.jdbc;

import java.net.SocketException;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.service.auth.KerberosSaslHelper;
import org.apache.hive.service.auth.PlainSaslHelper;
import org.apache.hive.service.cli.thrift.EmbeddedThriftCLIService;
import org.apache.hive.service.cli.thrift.TCLIService;
import org.apache.hive.service.cli.thrift.TCloseSessionReq;
import org.apache.hive.service.cli.thrift.TOpenSessionReq;
import org.apache.hive.service.cli.thrift.TOpenSessionResp;
import org.apache.hive.service.cli.thrift.TProtocolVersion;
import org.apache.hive.service.cli.thrift.TSessionHandle;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

/**
 * HiveConnection.
 *
 */
public class HiveConnection implements java.sql.Connection {

  private static final String HIVE_AUTH_TYPE = "auth";
  private static final String HIVE_AUTH_SIMPLE = "noSasl";
  private static final String HIVE_AUTH_USER = "user";
  private static final String HIVE_AUTH_PRINCIPAL = "principal";
  private static final String HIVE_AUTH_PASSWD = "password";
  private static final String HIVE_ANONYMOUS_USER = "anonymous";
  private static final String HIVE_ANONYMOUS_PASSWD = "anonymous";

  private static final String CONNECT_TIMEOUT = "connectTimeout";
  private static final String SOCKET_TIMEOUT = "socketTimeout";
  private static final String REUSE_ADDRESS = "reuseAddress";
  private static final String KEEP_ALIVE = "keepAlive";
  private static final String SEND_BUFFER_SIZE = "sendBufferSize";
  private static final String RECV_BUFFER_SIZE = "recvBufferSize";

  private TSocket socket;
  private TTransport transport;
  private TCLIService.Iface client;
  private boolean isClosed = true;
  private SQLWarning warningChain = null;
  private TSessionHandle sessHandle = null;
  private final List<TProtocolVersion> supportedProtocols = new LinkedList<TProtocolVersion>();

  public HiveConnection() {}

  /**
   * TODO: - parse uri (use java.net.URI?).
   */
  public HiveConnection(String uri, Properties info) throws SQLException {
    try {
      initialize(Utils.parseURL(uri), info);
    } catch (Exception e) {
      try {
        close();
      } catch (SQLException e1) {
        // ignore
      }
      throw new SQLException("Could not establish connection to "
                  + uri + ": " + e.getMessage(), " 08S01", e);
    }
  }

  public void initialize(Utils.JdbcConnectionParams connParams, Properties info) throws Exception {
    if (connParams.isEmbeddedMode()) {
      client = new EmbeddedThriftCLIService();
    } else {
      // extract user/password from JDBC connection properties if its not supplied in the connection URL
      if (info.containsKey(HIVE_AUTH_USER)) {
        connParams.getSessionVars().put(HIVE_AUTH_USER, info.getProperty(HIVE_AUTH_USER));
        if (info.containsKey(HIVE_AUTH_PASSWD)) {
            connParams.getSessionVars().put(HIVE_AUTH_PASSWD, info.getProperty(HIVE_AUTH_PASSWD));
        }
      }
      socket = createSocket(connParams);
      transport = openTransport(connParams, socket);
      client = new TCLIService.Client(new TBinaryProtocol(transport));
    }

    // currently only V1 is supported
    supportedProtocols.add(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V1);

    // open client session
    sessHandle = openSession(supportedProtocols);
    isClosed = false;

    configureConnection(connParams);
  }

  private void configureConnection(Utils.JdbcConnectionParams connParams)
      throws SQLException {
    Statement stmt = createStatement();
    try {
      stmt.execute("set hive.fetch.output.serde = org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
    } finally {
      stmt.close();
    }

    // set the hive variable in session state for local mode
    if (connParams.isEmbeddedMode()) {
      if (!connParams.getHiveVars().isEmpty()) {
        SessionState.get().setHiveVariables(connParams.getHiveVars());
      }
    } else {
      // for remote JDBC client, try to set the conf var using 'set foo=bar'
      for (Entry<String, String> hiveConf : connParams.getHiveConfs().entrySet()) {
        stmt = createStatement();
        try {
          stmt.execute("set " + hiveConf.getKey() + "=" + hiveConf.getValue());
        } finally {
          stmt.close();
        }
      }
    }
  }

  private TSocket createSocket(Utils.JdbcConnectionParams connParams) throws SocketException {
    TSocket tsocket = new TSocket(connParams.getHost(), connParams.getPort());
    Map<String, String> sessConf = connParams.getSessionVars();
    String value = sessConf.get(CONNECT_TIMEOUT);
    if (value == null) {
      value = sessConf.get(SOCKET_TIMEOUT);
    }
    if (value != null) {
      tsocket.getSocket().setSoTimeout(Integer.parseInt(value));
    }
    value = sessConf.get(REUSE_ADDRESS);
    if (value != null) {
      tsocket.getSocket().setReuseAddress(Boolean.valueOf(value));
    }
    value = sessConf.get(KEEP_ALIVE);
    if (value != null) {
      tsocket.getSocket().setKeepAlive(Boolean.valueOf(value));
    }
    value = sessConf.get(SEND_BUFFER_SIZE);
    if (value != null) {
      tsocket.getSocket().setSendBufferSize(Integer.parseInt(value));
    }
    value = sessConf.get(RECV_BUFFER_SIZE);
    if (value != null) {
      tsocket.getSocket().setReceiveBufferSize(Integer.parseInt(value));
    }
    return tsocket;
  }

  private TTransport openTransport(Utils.JdbcConnectionParams connParams, TSocket tsocket)
      throws Exception {

    TTransport transport = tsocket;
    Map<String, String> sessConf = connParams.getSessionVars();
    // handle secure connection if specified
    if (!sessConf.containsKey(HIVE_AUTH_TYPE)
      || !sessConf.get(HIVE_AUTH_TYPE).equals(HIVE_AUTH_SIMPLE)){
      if (sessConf.containsKey(HIVE_AUTH_PRINCIPAL)) {
        transport = KerberosSaslHelper.getKerberosTransport(
          sessConf.get(HIVE_AUTH_PRINCIPAL), connParams.getHost(), transport);
      } else {
        String userName = sessConf.get(HIVE_AUTH_USER);
        if ((userName == null) || userName.isEmpty()) {
          userName = HIVE_ANONYMOUS_USER;
        }
        String passwd = sessConf.get(HIVE_AUTH_PASSWD);
        if ((passwd == null) || passwd.isEmpty()) {
          passwd = HIVE_ANONYMOUS_PASSWD;
        }
        transport = PlainSaslHelper.getPlainTransport(userName, passwd, transport);
      }
    }
    transport.open();

    String value = sessConf.get(SOCKET_TIMEOUT);
    tsocket.getSocket().setSoTimeout(value == null ? 0 : Integer.parseInt(value));

    return transport;
  }

  private TSessionHandle openSession(List<TProtocolVersion> supported) throws Exception {
    TOpenSessionReq openReq = new TOpenSessionReq();

    // set the session configuration
    // openReq.setConfiguration(null);

    TOpenSessionResp openResp = client.OpenSession(openReq);

    // validate connection
    Utils.verifySuccess(openResp.getStatus());
    if (!supportedProtocols.contains(openResp.getServerProtocolVersion())) {
      throw new TException("Unsupported Hive2 protocol");
    }
    return openResp.getSessionHandle();
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#clearWarnings()
   */

  public void clearWarnings() throws SQLException {
    warningChain = null;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#close()
   */

  public void close() throws SQLException {
    try {
      if (!isClosed) {
        TCloseSessionReq closeReq = new TCloseSessionReq(sessHandle);
        client.CloseSession(closeReq);
      }
    } catch (TException e) {
      throw new SQLException("Error while cleaning up the server resources", e);
    } finally {
      isClosed = true;
      if (transport != null) {
        try {
          transport.close();
        } catch (Exception e) {
          // ignore
        }
      }
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#commit()
   */

  public void commit() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#createArrayOf(java.lang.String,
   * java.lang.Object[])
   */

  public Array createArrayOf(String arg0, Object[] arg1) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#createBlob()
   */

  public Blob createBlob() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#createClob()
   */

  public Clob createClob() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#createNClob()
   */

  public NClob createNClob() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#createSQLXML()
   */

  public SQLXML createSQLXML() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /**
   * Creates a Statement object for sending SQL statements to the database.
   *
   * @throws SQLException
   *           if a database access error occurs.
   * @see java.sql.Connection#createStatement()
   */

  public Statement createStatement() throws SQLException {
    if (isClosed) {
      throw new SQLException("Can't create Statement, connection is closed");
    }
    return new HiveStatement(this, sessHandle);
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#createStatement(int, int)
   */

  public Statement createStatement(int resultSetType, int resultSetConcurrency)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#createStatement(int, int, int)
   */

  public Statement createStatement(int resultSetType, int resultSetConcurrency,
      int resultSetHoldability) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#createStruct(java.lang.String, java.lang.Object[])
   */

  public Struct createStruct(String typeName, Object[] attributes)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#getAutoCommit()
   */

  public boolean getAutoCommit() throws SQLException {
    return true;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#getCatalog()
   */

  public String getCatalog() throws SQLException {
    return "";
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#getClientInfo()
   */

  public Properties getClientInfo() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#getClientInfo(java.lang.String)
   */

  public String getClientInfo(String name) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#getHoldability()
   */

  public int getHoldability() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#getMetaData()
   */

  public DatabaseMetaData getMetaData() throws SQLException {
    return new HiveDatabaseMetaData(this, sessHandle);
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#getTransactionIsolation()
   */

  public int getTransactionIsolation() throws SQLException {
    return Connection.TRANSACTION_NONE;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#getTypeMap()
   */

  public Map<String, Class<?>> getTypeMap() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#getWarnings()
   */

  public SQLWarning getWarnings() throws SQLException {
    return warningChain;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#isClosed()
   */

  public boolean isClosed() throws SQLException {
    return isClosed;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#isReadOnly()
   */

  public boolean isReadOnly() throws SQLException {
    return false;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#isValid(int)
   */

  public boolean isValid(int timeout) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#nativeSQL(java.lang.String)
   */

  public String nativeSQL(String sql) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#prepareCall(java.lang.String)
   */

  public CallableStatement prepareCall(String sql) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#prepareCall(java.lang.String, int, int)
   */

  public CallableStatement prepareCall(String sql, int resultSetType,
      int resultSetConcurrency) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#prepareCall(java.lang.String, int, int, int)
   */

  public CallableStatement prepareCall(String sql, int resultSetType,
      int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#prepareStatement(java.lang.String)
   */

  public PreparedStatement prepareStatement(String sql) throws SQLException {
    return new HivePreparedStatement(this, sessHandle, sql);
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#prepareStatement(java.lang.String, int)
   */

  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
      throws SQLException {
    return new HivePreparedStatement(this, sessHandle, sql);
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#prepareStatement(java.lang.String, int[])
   */

  public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#prepareStatement(java.lang.String,
   * java.lang.String[])
   */

  public PreparedStatement prepareStatement(String sql, String[] columnNames)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#prepareStatement(java.lang.String, int, int)
   */

  public PreparedStatement prepareStatement(String sql, int resultSetType,
      int resultSetConcurrency) throws SQLException {
    return new HivePreparedStatement(this, sessHandle, sql);
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#prepareStatement(java.lang.String, int, int, int)
   */

  public PreparedStatement prepareStatement(String sql, int resultSetType,
      int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#releaseSavepoint(java.sql.Savepoint)
   */

  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#rollback()
   */

  public void rollback() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#rollback(java.sql.Savepoint)
   */

  public void rollback(Savepoint savepoint) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#setAutoCommit(boolean)
   */

  public void setAutoCommit(boolean autoCommit) throws SQLException {
    if (autoCommit) {
      throw new SQLException("enabling autocommit is not supported");
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#setCatalog(java.lang.String)
   */

  public void setCatalog(String catalog) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#setClientInfo(java.util.Properties)
   */

  public void setClientInfo(Properties properties)
      throws SQLClientInfoException {
    // TODO Auto-generated method stub
    throw new SQLClientInfoException("Method not supported", null);
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#setClientInfo(java.lang.String, java.lang.String)
   */

  public void setClientInfo(String name, String value)
      throws SQLClientInfoException {
    // TODO Auto-generated method stub
    throw new SQLClientInfoException("Method not supported", null);
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#setHoldability(int)
   */

  public void setHoldability(int holdability) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#setReadOnly(boolean)
   */

  public void setReadOnly(boolean readOnly) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#setSavepoint()
   */

  public Savepoint setSavepoint() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#setSavepoint(java.lang.String)
   */

  public Savepoint setSavepoint(String name) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#setTransactionIsolation(int)
   */

  public void setTransactionIsolation(int level) throws SQLException {
    // TODO: throw an exception?
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#setTypeMap(java.util.Map)
   */

  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Wrapper#isWrapperFor(java.lang.Class)
   */

  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    if (iface == TSocket.class) {
      return socket != null;
    }
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Wrapper#unwrap(java.lang.Class)
   */

  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (iface == TSocket.class) {
      return (T)socket;
    }
    throw new SQLException("Method not supported");
  }

  public TTransport getTransport() {
    return transport;
  }

  public TCLIService.Iface getClient() {
    return client;
  }

  public TSessionHandle getSessHandle() {
    return sessHandle;
  }
}
