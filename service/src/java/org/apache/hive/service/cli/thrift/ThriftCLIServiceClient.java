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

package org.apache.hive.service.cli.thrift;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hive.service.CompileResult;
import org.apache.hive.service.OperationInfo;
import org.apache.hive.service.SessionInfo;
import org.apache.hive.service.auth.PlainSaslHelper;
import org.apache.hive.service.cli.CLIServiceClient;
import org.apache.hive.service.cli.ColumnDescriptor;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.GetInfoType;
import org.apache.hive.service.cli.GetInfoValue;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.OperationState;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.TableSchema;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

/**
 * ThriftCLIServiceClient.
 *
 */
public class ThriftCLIServiceClient extends CLIServiceClient {
  private final TCLIService.Iface cliService;

  public ThriftCLIServiceClient(TCLIService.Iface cliService) {
    this.cliService = cliService;
  }

  public void checkStatus(TStatus status) throws HiveSQLException {
    if (TStatusCode.ERROR_STATUS.equals(status.getStatusCode())) {
      throw new HiveSQLException(status);
    }
  }

  @Override
  public List<SessionInfo> getSessions() throws HiveSQLException {
    try {
      TSessionsRes sessionsRes = cliService.GetSessions();
      List<TSessionInfo> sessions = sessionsRes.getSessions();
      if (sessions == null || sessions.isEmpty()) {
        return Collections.emptyList();
      }
      List<SessionInfo> result = new ArrayList<SessionInfo>();
      for (TSessionInfo session : sessions) {
        result.add(new SessionInfo(session.getSessionHandle(), session.getStartTime()));
      }
      return result;
    } catch (Exception e) {
      throw new HiveSQLException(e);
    }
  }

  @Override
  public List<OperationInfo> getOperations() throws HiveSQLException {
    try {
      TOperationsRes operationsRes = cliService.GetOperations();
      List<TOperationInfo> operations = operationsRes.getOperations();
      if (operations == null || operations.isEmpty()) {
        return Collections.emptyList();
      }
      List<OperationInfo> result = new ArrayList<OperationInfo>();
      for (TOperationInfo top : operations) {
        result.add(new OperationInfo(top.getStatus(), top.getOperationHandle(),
            top.getQuery(), top.getStartTime()));
      }
      return result;
    } catch (Exception e) {
      throw new HiveSQLException(e);
    }
  }

  /* (non-Javadoc)
     * @see org.apache.hive.service.cli.ICLIService#openSession(java.lang.String, java.lang.String, java.util.Map)
     */
  @Override
  public SessionHandle openSession(String username, String password,
      Map<String, String> configuration)
          throws HiveSQLException {
    try {
      TOpenSessionReq req = new TOpenSessionReq();
      req.setUsername(username);
      req.setPassword(password);
      req.setConfiguration(configuration);
      TOpenSessionResp resp = cliService.OpenSession(req);
      checkStatus(resp.getStatus());
      return new SessionHandle(resp.getSessionHandle());
    } catch (HiveSQLException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveSQLException(e);
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#closeSession(org.apache.hive.service.cli.SessionHandle)
   */
  @Override
  public SessionHandle openSessionWithImpersonation(String username, String password,
      Map<String, String> configuration, String delegationToken) throws HiveSQLException {
    throw new HiveSQLException("open with impersonation operation is not supported in the client");
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#closeSession(org.apache.hive.service.cli.SessionHandle)
   */
  @Override
  public void closeSession(SessionHandle sessionHandle) throws HiveSQLException {
    try {
      TCloseSessionReq req = new TCloseSessionReq(sessionHandle.toTSessionHandle());
      TCloseSessionResp resp = cliService.CloseSession(req);
      checkStatus(resp.getStatus());
    } catch (HiveSQLException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveSQLException(e);
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#getInfo(org.apache.hive.service.cli.SessionHandle, java.util.List)
   */
  @Override
  public GetInfoValue getInfo(SessionHandle sessionHandle, GetInfoType infoType)
      throws HiveSQLException {
    try {
      // FIXME extract the right info type
      TGetInfoReq req = new TGetInfoReq(sessionHandle.toTSessionHandle(), infoType.toTGetInfoType());
      TGetInfoResp resp = cliService.GetInfo(req);
      checkStatus(resp.getStatus());
      return new GetInfoValue(resp.getInfoValue());
    } catch (HiveSQLException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveSQLException(e);
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#executeStatement(org.apache.hive.service.cli.SessionHandle, java.lang.String, java.util.Map)
   */
  @Override
  public OperationHandle executeStatement(SessionHandle sessionHandle, String statement,
      Map<String, String> confOverlay)
          throws HiveSQLException {
    try {
      TExecuteStatementReq req = new TExecuteStatementReq(sessionHandle.toTSessionHandle(), statement);
      req.setConfOverlay(confOverlay);
      TExecuteStatementResp resp = cliService.ExecuteStatement(req);
      checkStatus(resp.getStatus());
      return new OperationHandle(resp.getOperationHandle());
    } catch (HiveSQLException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveSQLException(e);
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.sql.SQLServiceClient#compileStatement(org.apache.hive.service.sql.SessionHandle, java.lang.String, java.util.Map)
   */
  @Override
  public CompileResult compileStatement(SessionHandle sessionHandle, String statement, Map<String, String> confOverlay) throws HiveSQLException {
    try {
      TExecuteStatementReq req = new TExecuteStatementReq(sessionHandle.toTSessionHandle(), statement);
      req.setConfOverlay(confOverlay);
      TCompileRes resp = cliService.Compile(req);
      checkStatus(resp.getStatus());
      return new CompileResult(resp.getOperationHandle(), resp.getQueryPlan());
    } catch (HiveSQLException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveSQLException(e);
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.sql.SQLServiceClient#runStatement(org.apache.hive.service.sql.SessionHandle, org.apache.hive.service.sql.SessionHandle.OperationHandle)
   */
  @Override
  public void runStatement(SessionHandle sessionHandle, OperationHandle opHandle) throws HiveSQLException {
    try {
      TRunReq req = new TRunReq(sessionHandle.toTSessionHandle(), opHandle.toTOperationHandle());
      checkStatus(cliService.Run(req));
    } catch (HiveSQLException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveSQLException(e);
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.sql.SQLServiceClient#executeTransient(org.apache.hive.service.sql.SessionHandle, java.lang.String, java.util.Map)
   */
  @Override
  public void executeTransient(SessionHandle sessionHandle, String statement, Map<String, String> confOverlay) throws HiveSQLException {
    try {
      TExecuteStatementReq req = new TExecuteStatementReq(sessionHandle.toTSessionHandle(), statement);
      req.setConfOverlay(confOverlay);
      checkStatus(cliService.ExecuteTransient(req));
    } catch (Exception e) {
      throw new HiveSQLException(e);
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.sql.ISQLService#getTypeInfo(org.apache.hive.service.sql.SessionHandle)
   */
  @Override
  public OperationHandle getTypeInfo(SessionHandle sessionHandle) throws HiveSQLException {
    try {
      TGetTypeInfoReq req = new TGetTypeInfoReq(sessionHandle.toTSessionHandle());
      TGetTypeInfoResp resp = cliService.GetTypeInfo(req);
      checkStatus(resp.getStatus());
      return new OperationHandle(resp.getOperationHandle());
    } catch (HiveSQLException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveSQLException(e);
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#getCatalogs(org.apache.hive.service.cli.SessionHandle)
   */
  @Override
  public OperationHandle getCatalogs(SessionHandle sessionHandle) throws HiveSQLException {
    try {
      TGetCatalogsReq req = new TGetCatalogsReq(sessionHandle.toTSessionHandle());
      TGetCatalogsResp resp = cliService.GetCatalogs(req);
      checkStatus(resp.getStatus());
      return new OperationHandle(resp.getOperationHandle());
    } catch (HiveSQLException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveSQLException(e);
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#getSchemas(org.apache.hive.service.cli.SessionHandle, java.lang.String, java.lang.String)
   */
  @Override
  public OperationHandle getSchemas(SessionHandle sessionHandle, String catalogName,
      String schemaName)
          throws HiveSQLException {
    try {
      TGetSchemasReq req = new TGetSchemasReq(sessionHandle.toTSessionHandle());
      req.setCatalogName(catalogName);
      req.setSchemaName(schemaName);
      TGetSchemasResp resp = cliService.GetSchemas(req);
      checkStatus(resp.getStatus());
      return new OperationHandle(resp.getOperationHandle());
    } catch (HiveSQLException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveSQLException(e);
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#getTables(org.apache.hive.service.cli.SessionHandle, java.lang.String, java.lang.String, java.lang.String, java.util.List)
   */
  @Override
  public OperationHandle getTables(SessionHandle sessionHandle, String catalogName,
      String schemaName, String tableName, List<String> tableTypes)
          throws HiveSQLException {
    try {
      TGetTablesReq req = new TGetTablesReq(sessionHandle.toTSessionHandle());
      req.setTableName(tableName);
      req.setTableTypes(tableTypes);
      req.setSchemaName(schemaName);
      TGetTablesResp resp = cliService.GetTables(req);
      checkStatus(resp.getStatus());
      return new OperationHandle(resp.getOperationHandle());
    } catch (HiveSQLException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveSQLException(e);
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#getTableTypes(org.apache.hive.service.cli.SessionHandle)
   */
  @Override
  public OperationHandle getTableTypes(SessionHandle sessionHandle) throws HiveSQLException {
    try {
      TGetTableTypesReq req = new TGetTableTypesReq(sessionHandle.toTSessionHandle());
      TGetTableTypesResp resp = cliService.GetTableTypes(req);
      checkStatus(resp.getStatus());
      return new OperationHandle(resp.getOperationHandle());
    } catch (HiveSQLException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveSQLException(e);
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#getColumns(org.apache.hive.service.cli.SessionHandle)
   */
  @Override
  public OperationHandle getColumns(SessionHandle sessionHandle,
      String catalogName, String schemaName, String tableName, String columnName)
          throws HiveSQLException {
    try {
      TGetColumnsReq req = new TGetColumnsReq();
      req.setSessionHandle(sessionHandle.toTSessionHandle());
      req.setCatalogName(catalogName);
      req.setSchemaName(schemaName);
      req.setTableName(tableName);
      req.setColumnName(columnName);
      TGetColumnsResp resp = cliService.GetColumns(req);
      checkStatus(resp.getStatus());
      return new OperationHandle(resp.getOperationHandle());
    } catch (HiveSQLException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveSQLException(e);
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#getFunctions(org.apache.hive.service.cli.SessionHandle)
   */
  @Override
  public OperationHandle getFunctions(SessionHandle sessionHandle,
      String catalogName, String schemaName, String functionName) throws HiveSQLException {
    try {
      TGetFunctionsReq req = new TGetFunctionsReq(sessionHandle.toTSessionHandle(), functionName);
      req.setCatalogName(catalogName);
      req.setSchemaName(schemaName);
      TGetFunctionsResp resp = cliService.GetFunctions(req);
      checkStatus(resp.getStatus());
      return new OperationHandle(resp.getOperationHandle());
    } catch (HiveSQLException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveSQLException(e);
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#getOperationStatus(org.apache.hive.service.cli.OperationHandle)
   */
  @Override
  public OperationState getOperationStatus(OperationHandle opHandle) throws HiveSQLException {
    try {
      TGetOperationStatusReq req = new TGetOperationStatusReq(opHandle.toTOperationHandle());
      TGetOperationStatusResp resp = cliService.GetOperationStatus(req);
      checkStatus(resp.getStatus());
      return OperationState.getOperationState(resp.getOperationState());
    } catch (HiveSQLException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveSQLException(e);
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#cancelOperation(org.apache.hive.service.cli.OperationHandle)
   */
  @Override
  public void cancelOperation(OperationHandle opHandle) throws HiveSQLException {
    try {
      TCancelOperationReq req = new TCancelOperationReq(opHandle.toTOperationHandle());
      TCancelOperationResp resp = cliService.CancelOperation(req);
      checkStatus(resp.getStatus());
    } catch (HiveSQLException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveSQLException(e);
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#closeOperation(org.apache.hive.service.cli.OperationHandle)
   */
  @Override
  public void closeOperation(OperationHandle opHandle)
      throws HiveSQLException {
    try {
      TCloseOperationReq req  = new TCloseOperationReq(opHandle.toTOperationHandle());
      TCloseOperationResp resp = cliService.CloseOperation(req);
      checkStatus(resp.getStatus());
    } catch (HiveSQLException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveSQLException(e);
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#getResultSetMetadata(org.apache.hive.service.cli.OperationHandle)
   */
  @Override
  public TableSchema getResultSetMetadata(OperationHandle opHandle)
      throws HiveSQLException {
    try {
      TGetResultSetMetadataReq req = new TGetResultSetMetadataReq(opHandle.toTOperationHandle());
      TGetResultSetMetadataResp resp = cliService.GetResultSetMetadata(req);
      checkStatus(resp.getStatus());
      return new TableSchema(resp.getSchema());
    } catch (HiveSQLException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveSQLException(e);
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#fetchResults(org.apache.hive.service.cli.OperationHandle, org.apache.hive.service.cli.FetchOrientation, long)
   */
  @Override
  public RowSet fetchResults(OperationHandle opHandle, FetchOrientation orientation, long maxRows)
      throws HiveSQLException {
    try {
      TFetchResultsReq req = new TFetchResultsReq();
      req.setOperationHandle(opHandle.toTOperationHandle());
      req.setOrientation(orientation.toTFetchOrientation());
      req.setMaxRows(maxRows);
      TFetchResultsResp resp = cliService.FetchResults(req);
      checkStatus(resp.getStatus());
      return new RowSet(resp.getResults());
    } catch (HiveSQLException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveSQLException(e);
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.ICLIService#fetchResults(org.apache.hive.service.cli.OperationHandle)
   */
  @Override
  public RowSet fetchResults(OperationHandle opHandle) throws HiveSQLException {
    // TODO: set the correct default fetch size
    return fetchResults(opHandle, FetchOrientation.FETCH_NEXT, 10000);
  }

  public static void main(String[] args) throws Exception {

    String host = "localhost";
    int port = 10000;
    if (args.length > 0) {
      int index = args[0].indexOf(":");
      if (index > 0) {
        host = args[0].substring(0, index);
        port = Integer.valueOf(args[0].substring(index + 1));
      } else {
        host = args[0];
      }
    }
    TTransport transport = new TSocket(host, port);
    transport = PlainSaslHelper.getPlainTransport("scott", "tiger", transport);
    transport.open();
    System.err.println("[ThriftCLIServiceClient/main] " + transport);

    TProtocol protocol = new TBinaryProtocol(transport);
    ThriftCLIServiceClient client = new ThriftCLIServiceClient(new TCLIService.Client(protocol));

    SessionHandle session = client.openSession(null, null, null);
    System.err.println("[ThriftCLIServiceClient/main] " + session);

    for (int i = 0; i < 10000; i++) {
      CompileResult result = client.compileStatement(session, "truncate table table1", null);
      OperationHandle handle = new OperationHandle(result.getHandle());
      if (result.getPlan() != null) {
        client.runStatement(session, handle);
      }
      client.closeOperation(handle);
      Thread.sleep(3000);
    }
    String line;
    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

    while ((line = reader.readLine()) != null) {
      if (line.trim().isEmpty()) {
        continue;
      }
      System.err.println("[ThriftCLIServiceClient/main] " + line);
      CompileResult result = client.compileStatement(session, line, null);
      System.err.println("[ThriftCLIServiceClient/main] " + result.getHandle());
      System.err.println("[ThriftCLIServiceClient/main] " + result.getPlan());

      OperationHandle handle = new OperationHandle(result.getHandle());

      client.executeTransient(session, "set navis", null);

      client.runStatement(session, handle);
      try {
        TableSchema schema = client.getResultSetMetadata(new OperationHandle(result.getHandle()));
        List<ColumnDescriptor> columnDescs = schema.getColumnDescriptors();

        Iterator<Object[]> iterator = null;
        while (true) {
          if (iterator == null || !iterator.hasNext()) {
            RowSet rowset = client.fetchResults(handle);
            iterator = rowset.iterator();
          }
          if (!iterator.hasNext()) {
            break;
          }
          Object[] next = iterator.next();
          StringBuilder builder = new StringBuilder();
          for (int i = 0; i < columnDescs.size(); i++) {
            Object eval = RowSet.evaluate(columnDescs.get(i), next[i]);
            if (builder.length() > 0) {
              builder.append(" ");
            }
            builder.append(eval);
          }
          System.err.println("[ThriftCLIServiceClient/main] " + builder.toString());
        }
      } finally {
        client.closeOperation(handle);
      }
    }
  }
}
