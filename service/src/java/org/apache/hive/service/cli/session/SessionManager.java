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

package org.apache.hive.service.cli.session;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.service.CompositeService;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.operation.Operation;
import org.apache.hive.service.cli.operation.OperationManager;

/**
 * SessionManager.
 *
 */
public class SessionManager extends CompositeService {

  private HiveSession serverSession;
  private boolean inheritToClient;

  private HiveConf hiveConf;
  private final Map<SessionHandle, HiveSession> handleToSession = new HashMap<SessionHandle, HiveSession>();
  private OperationManager operationManager = new OperationManager();
  private static final Object sessionMapLock = new Object();

  public SessionManager() {
    super("SessionManager");
  }

  @Override
  public synchronized void init(HiveConf hiveConf) {
    this.hiveConf = hiveConf;

    operationManager = new OperationManager();
    addService(operationManager);

    super.init(hiveConf);
  }

  @Override
  public synchronized void start() {
    super.start();
    // TODO
  }

  @Override
  public synchronized void stop() {
    // TODO
    super.stop();
  }

  public Map<SessionHandle, HiveSession> getSessions() {
    return new HashMap<SessionHandle, HiveSession>(handleToSession);
  }

  public SessionHandle openSession(String username, String password, Map<String, String> sessionConf)
          throws HiveSQLException {
     return openSession(username, password, sessionConf, false, null);
  }

  public SessionHandle openSession(String username, String password, Map<String, String> sessionConf,
          boolean withImpersonation, String delegationToken) throws HiveSQLException {
    if (username == null) {
      username = threadLocalUserName.get();
    }
    HiveSession session;
    if (withImpersonation) {
      HiveSessionImplwithUGI hiveSessionUgi = new HiveSessionImplwithUGI(new HiveConf(hiveConf),
          username, password, sessionConf, delegationToken);
      session = HiveSessionProxy.getProxy(hiveSessionUgi, hiveSessionUgi.getSessionUgi());
      hiveSessionUgi.setProxySession(session);
    } else {
      session = new HiveSessionImpl(new HiveConf(hiveConf), username, password, sessionConf);
    }
    session.setSessionManager(this);
    session.setOperationManager(operationManager);
    synchronized(sessionMapLock) {
      handleToSession.put(session.getSessionHandle(), session);
    }
    SessionState clientState = session.getSessionState();
    if (inheritToClient && serverSession != null) {
      SessionState serverState = serverSession.getSessionState();
      session.getHiveConf().setClassLoader(serverState.getConf().getClassLoader());
      clientState.addResourceMap(serverState.getResourceMap());
    }
    SessionState.start(clientState);

    return session.getSessionHandle();
  }

  public void closeSession(SessionHandle sessionHandle) throws HiveSQLException {
    HiveSession session;
    synchronized(sessionMapLock) {
      session = handleToSession.remove(sessionHandle);
    }
    if (session == null) {
      throw new HiveSQLException("Session does not exist!");
    }
    session.close();
  }


  public HiveSession getSession(SessionHandle sessionHandle) throws HiveSQLException {
    HiveSession session;
    synchronized(sessionMapLock) {
      session = handleToSession.get(sessionHandle);
    }
    if (session == null) {
      throw new HiveSQLException("Invalid SessionHandle: " + sessionHandle);
    }
    return session;
  }

  public OperationManager getOperationManager() {
    return operationManager;
  }

  private static ThreadLocal<String> threadLocalIpAddress = new ThreadLocal<String>() {
    @Override
    protected synchronized String initialValue() {
      return null;
    }
  };

  public static void setIpAddress(String ipAddress) {
    threadLocalIpAddress.set(ipAddress);
  }

  private void clearIpAddress() {
    threadLocalIpAddress.remove();
  }

  private static ThreadLocal<String> threadLocalUserName = new ThreadLocal<String>(){
    @Override
    protected synchronized String initialValue() {
      return null;
    }
  };

  public static void setUserName(String userName) {
    threadLocalUserName.set(userName);
  }

  private void clearUserName() {
    threadLocalUserName.remove();
  }

  public HiveSession openServerSession(boolean inheritToClient) throws HiveSQLException {
    if (serverSession != null) {
      return serverSession;
    }
    SessionHandle handle = openSession(null, null, null);
    this.inheritToClient = inheritToClient;
    return serverSession = handleToSession.get(handle);
  }

  public List<Operation> getOperations() {
    return operationManager.getOperations();
  }
}
