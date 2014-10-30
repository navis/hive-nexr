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

package org.apache.hive.service.cli.operation;

import java.io.UnsupportedEncodingException;
import java.security.PrivilegedExceptionAction;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.OperationLog;
import org.apache.hadoop.hive.ql.processors.CommandProcessor;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.service.cli.ColumnDescriptor;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationState;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.session.HiveSession;
import org.apache.hive.service.server.ThreadWithGarbageCleanup;

/**
 * SQLOperation.
 *
 */
public class SQLOperation extends ExecuteStatementOperation {

  private transient SerDe serde;

  protected SQLOperation(HiveSession parentSession, String statement,
      CommandProcessor processor, Map<String, String> confOverlay, boolean runAsync) {
    super(parentSession, statement, processor, confOverlay, runAsync);

    // set the operation handle information in Driver, so that thrift API users
    // can use the operation handle they receive, to lookup query information in
    // Yarn ATS
    String guid64 = Base64.encodeBase64URLSafeString(getHandle().getHandleIdentifier()
        .toTHandleIdentifier().getGuid()).trim();
    ((Driver)processor).setOperationId(guid64);
  }

  @Override
  public void runInternal(HiveConf runtime) throws Exception {
    if (!shouldRunAsync()) {
      super.runInternal(runtime);
      return;
    }

    prepare(runtime);

    // We'll pass ThreadLocals in the background thread from the foreground (handler) thread
    final SessionState parentSessionState = SessionState.get();
    // ThreadLocal Hive object needs to be set in background thread.
    // The metastore client in Hive is associated with right user.
    final Hive parentHive = getSessionHive();
    // Current UGI will get used by metastore when metastore is in embedded mode
    // So this needs to get passed to the new background thread
    final UserGroupInformation currentUGI = getCurrentUGI(runtime);
    // Runnable impl to call runInternal asynchronously,
    // from a different thread
    final PrivilegedExceptionAction<Object> action = new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws Exception {
        Hive.set(parentHive);
        SessionState.setCurrentSessionState(parentSessionState);
        // Set current OperationLog in this async thread for keeping on saving query log.
        registerCurrentOperationLog();
        try {
          execute();
        } catch (Throwable t) {
          LOG.error("Error running hive query: ", t);
          setOperationException(t);
          setState(OperationState.ERROR);
        } finally {
          unregisterOperationLog();
        }
        return null;
      }
    };

    Runnable backgroundOperation = new Runnable() {
      @Override
      public void run() {
        try {
          currentUGI.doAs(action);
        } catch (Throwable t) {
          if (getOperationException() == null) {
            setOperationException(new HiveSQLException(t));
          }
        } finally {
          if (getOperationException() != null) {
            LOG.error("Error running hive query as user : " +
                currentUGI.getShortUserName(), operationException);
          }
          /**
           * We'll cache the ThreadLocal RawStore object for this background thread for an orderly cleanup
           * when this thread is garbage collected later.
           * @see org.apache.hive.service.server.ThreadWithGarbageCleanup#finalize()
           */
          if (ThreadWithGarbageCleanup.currentThread() instanceof ThreadWithGarbageCleanup) {
            ThreadWithGarbageCleanup currentThread =
                (ThreadWithGarbageCleanup) ThreadWithGarbageCleanup.currentThread();
            currentThread.cacheThreadLocalRawStore();
          }
        }
      }
    };
    try {
      // This submit blocks if no background threads are available to run this operation
      Future<?> backgroundHandle =
          getParentSession().getSessionManager().submitBackgroundOperation(backgroundOperation);
      setBackgroundHandle(backgroundHandle);
    } catch (RejectedExecutionException rejected) {
      setState(OperationState.ERROR);
      throw new HiveSQLException("The background threadpool cannot accept" +
          " new task for execution, please retry the operation", rejected);
    }
  }

  /**
   * Returns the current UGI on the stack
   * @param opConfig
   * @return UserGroupInformation
   * @throws HiveSQLException
   */
  private UserGroupInformation getCurrentUGI(HiveConf opConfig) throws HiveSQLException {
    try {
      return Utils.getUGI();
    } catch (Exception e) {
      throw new HiveSQLException("Unable to get current user", e);
    }
  }

  /**
   * Returns the ThreadLocal Hive for the current thread
   * @return Hive
   * @throws HiveSQLException
   */
  private Hive getSessionHive() throws HiveSQLException {
    try {
      return Hive.get();
    } catch (HiveException e) {
      throw new HiveSQLException("Failed to get ThreadLocal Hive object", e);
    }
  }

  private void registerCurrentOperationLog() {
    if (isOperationLogEnabled) {
      if (operationLog == null) {
        LOG.warn("Failed to get current OperationLog object of Operation: " +
            getHandle().getHandleIdentifier());
        isOperationLogEnabled = false;
        return;
      }
      OperationLog.setCurrentOperationLog(operationLog);
    }
  }

  @Override
  public void cancel() throws HiveSQLException {
    cleanup(OperationState.CANCELED);
  }

  @Override
  protected RowSet encode(List results, RowSet rowSet) throws HiveSQLException {
    try {
      if (processor.isFromFetchTask()) {
        return prepareFromRow(results, rowSet);
      }
      return decodeFromString(results, rowSet);
    } catch (Exception e) {
      throw new HiveSQLException(e);
    }
  }

  // already encoded to thrift-able object in ThriftFormatter
  private RowSet prepareFromRow(List<Object> rows, RowSet rowSet) {
    for (Object row : rows) {
      rowSet.addRow((Object[]) row);
    }
    return rowSet;
  }

  private RowSet decodeFromString(List<Object> rows, RowSet rowSet)
      throws SQLException, SerDeException {
    getSerDe();
    StructObjectInspector soi = (StructObjectInspector) serde.getObjectInspector();
    List<? extends StructField> fieldRefs = soi.getAllStructFieldRefs();

    Object[] deserializedFields = new Object[fieldRefs.size()];
    Object rowObj;
    ObjectInspector fieldOI;

    int protocol = getProtocolVersion().getValue();
    for (Object rowString : rows) {
      try {
        rowObj = serde.deserialize(new BytesWritable(((String)rowString).getBytes("UTF-8")));
      } catch (UnsupportedEncodingException e) {
        throw new SerDeException(e);
      }
      for (int i = 0; i < fieldRefs.size(); i++) {
        StructField fieldRef = fieldRefs.get(i);
        fieldOI = fieldRef.getFieldObjectInspector();
        Object fieldData = soi.getStructFieldData(rowObj, fieldRef);
        deserializedFields[i] = SerDeUtils.toThriftPayload(fieldData, fieldOI, protocol);
      }
      rowSet.addRow(deserializedFields);
    }
    return rowSet;
  }

  private SerDe getSerDe() throws SQLException {
    if (serde != null) {
      return serde;
    }
    try {
      List<ColumnDescriptor> fieldSchemas = resultSchema.getColumnDescriptors();
      StringBuilder namesSb = new StringBuilder();
      StringBuilder typesSb = new StringBuilder();

      if (fieldSchemas != null && !fieldSchemas.isEmpty()) {
        for (int pos = 0; pos < fieldSchemas.size(); pos++) {
          if (pos != 0) {
            namesSb.append(",");
            typesSb.append(",");
          }
          namesSb.append(fieldSchemas.get(pos).getName());
          typesSb.append(fieldSchemas.get(pos).getTypeName());
        }
      }
      String names = namesSb.toString();
      String types = typesSb.toString();

      serde = new LazySimpleSerDe();
      Properties props = new Properties();
      if (names.length() > 0) {
        LOG.debug("Column names: " + names);
        props.setProperty(serdeConstants.LIST_COLUMNS, names);
      }
      if (types.length() > 0) {
        LOG.debug("Column types: " + types);
        props.setProperty(serdeConstants.LIST_COLUMN_TYPES, types);
      }
      SerDeUtils.initializeSerDe(serde, new HiveConf(), props, null);

    } catch (Exception ex) {
      ex.printStackTrace();
      throw new SQLException("Could not create ResultSet: " + ex.getMessage(), ex);
    }
    return serde;
  }
}
