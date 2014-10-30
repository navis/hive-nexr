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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.parse.VariableSubstitution;
import org.apache.hadoop.hive.ql.processors.CommandProcessor;
import org.apache.hadoop.hive.ql.processors.CommandProcessorFactory;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationState;
import org.apache.hive.service.cli.OperationType;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.RowSetFactory;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.cli.session.HiveSession;

public class ExecuteStatementOperation extends Operation {

  protected final String statement;
  protected final CommandProcessor processor;
  protected final Map<String, String> confOverlay = new HashMap<String, String>();

  protected TableSchema resultSchema;

  public ExecuteStatementOperation(HiveSession parentSession, String statement,
      CommandProcessor processor, Map<String, String> confOverlay, boolean runInBackground) {
    super(parentSession, OperationType.EXECUTE_STATEMENT, runInBackground);
    this.statement = statement;
    this.processor = processor;
    if (confOverlay != null) {
      this.confOverlay.putAll(confOverlay);
    }
  }

  public static ExecuteStatementOperation newExecuteStatementOperation(
      HiveSession parentSession, String statement, Map<String, String> confOverlay, boolean runAsync)
          throws HiveSQLException {
    statement = statement.trim();
    CommandProcessor processor;
    try {
      processor = CommandProcessorFactory.get(statement.split("\\s+"));
    } catch (SQLException e) {
      throw new HiveSQLException(e.getMessage(), e.getSQLState(), e);
    } catch (Exception e) {
      throw new HiveSQLException(e.getMessage(), e);
    }
    if (processor instanceof Driver) {
      return new SQLOperation(parentSession, statement, processor, confOverlay, runAsync);
    }
    return new HiveCommandOperation(parentSession, statement, processor, confOverlay, false);
  }

  @Override
  protected final void runInternal() throws HiveSQLException {
    setState(OperationState.PENDING);
    try {
      runInternal(getConfigForOperation());
    } catch (HiveSQLException se) {
      if (getStatus().getState() == OperationState.CANCELED) {
        return;
      }
      setState(OperationState.ERROR);
      throw se;
    } catch (Exception e) {
      setState(OperationState.ERROR);
      throw new HiveSQLException("Error running query: " + e.toString(), e);
    }
  }

  public void runInternal(HiveConf runtime) throws Exception {
    prepare(runtime);
    execute();
  }

  protected final CommandProcessorResponse prepare(HiveConf runtime) throws Exception {
    setState(OperationState.RUNNING);
    processor.init(runtime, parentSession.getSessionState());

    String substituted = new VariableSubstitution().substitute(runtime, statement);
    CommandProcessorResponse response = processor.prepare(substituted);
    if (response.getResponseCode() != 0) {
      throw toSQLException("Error while preparing statement", response);
    }
    Schema schema = response.getSchema();
    if (schema != null) {
      resultSchema = new TableSchema(schema);
    }
    setHasResultSet(schema != null);
    return response;
  }

  protected final void execute() throws Exception {
    CommandProcessorResponse response = processor.run();
    if (response.getResponseCode() != 0) {
      throw toSQLException("Error while running statement", response);
    }
    setState(OperationState.FINISHED);
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.operation.Operation#getResultSetSchema()
   */
  @Override
  public TableSchema getResultSetSchema() throws HiveSQLException {
    assertState(OperationState.FINISHED);
    return resultSchema;
  }

  private transient final List<Object> convey = new ArrayList<Object>();

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.operation.Operation#getNextRowSet(org.apache.hive.service.cli.FetchOrientation, long)
   */
  @Override
  public RowSet getNextRowSet(FetchOrientation orientation, long maxRows) throws HiveSQLException {
    if (resultSchema == null) {
      throw new HiveSQLException("This operation does not provide output result");
    }
    assertState(OperationState.FINISHED);
    validateDefaultFetchOrientation(orientation);

    if (orientation == FetchOrientation.FETCH_FIRST) {
      processor.resetFetch();
    }

    RowSet rowSet = RowSetFactory.create(resultSchema, getProtocolVersion());
    try {
      processor.getResults(convey, maxRows);
    } catch (Exception e) {
      throw new HiveSQLException("Failed to get result", e);
    }
    try {
      return encode(convey, rowSet);
    } finally {
      convey.clear();
    }
  }

  protected RowSet encode(List results, RowSet rowSet) throws HiveSQLException {
    for (Object row : results) {
      rowSet.addRow(new Object[] {row});
    }
    return rowSet;
  }

  /**
   * If there are query specific settings to overlay, then create a copy of config
   * There are two cases we need to clone the session config that's being passed to hive driver
   * 1. Async query -
   *    If the client changes a config setting, that shouldn't reflect in the execution already underway
   * 2. confOverlay -
   *    The query specific settings should only be applied to the query config and not session
   * @return new configuration
   * @throws HiveSQLException
   */
  private HiveConf getConfigForOperation() throws HiveSQLException {
    HiveConf sqlOperationConf = getParentSession().getHiveConf();
    if (!confOverlay.isEmpty() || shouldRunAsync()) {
      // clone the parent session config for this query
      sqlOperationConf = new HiveConf(sqlOperationConf);

      // apply overlay query specific settings, if any
      for (Map.Entry<String, String> confEntry : confOverlay.entrySet()) {
        try {
          sqlOperationConf.verifyAndSet(confEntry.getKey(), confEntry.getValue());
        } catch (IllegalArgumentException e) {
          throw new HiveSQLException("Error applying statement specific settings", e);
        }
      }
    }
    return sqlOperationConf;
  }

  @Override
  protected void cleanup(OperationState state) throws HiveSQLException {
    try {
      super.cleanup(state);
    } finally {
      processor.close();
    }
  }
}
