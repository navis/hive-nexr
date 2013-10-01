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

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.parse.VariableSubstitution;
import org.apache.hadoop.hive.ql.plan.api.Query;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationState;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.cli.session.HiveSession;

/**
 * SQLOperation.
 *
 */
public class SQLOperation extends ExecuteStatementOperation {

  private Driver driver = null;
  private CommandProcessorResponse response;
  private TableSchema resultSchema = null;
  private Schema mResultSchema = null;
  private SerDe serde = null;

  public SQLOperation(HiveSession parentSession, String statement, Map<String, String> confOverlay) {
    // TODO: call setRemoteUser in ExecuteStatementOperation or higher.
    super(parentSession, statement, confOverlay);
  }

  public void prepare() throws HiveSQLException {
  }

  @Override
  public void run() throws HiveSQLException {
    compile();
    execute();
  }

  public Query compile() throws HiveSQLException {
    assert driver == null;
    setState(OperationState.RUNNING);

    HiveConf hiveConf = getParentSession().getHiveConf();
    try {
      driver = new Driver(hiveConf);
      // In Hive server mode, we are not able to retry in the FetchTask
      // case, when calling fetch queries since execute() has returned.
      // For now, we disable the test attempts.
      driver.setTryCount(Integer.MAX_VALUE);

      String subStatement = new VariableSubstitution().substitute(hiveConf, statement);

      response = driver.compileCommand(subStatement, false);
      if (0 != response.getResponseCode()) {
        setState(OperationState.ERROR);
        throw new HiveSQLException("Error while compiling statement: " + statement + " by "
            + response.toDetailedMessage(), response.getSQLState(), response.getResponseCode());
      }
      Query query = getPlan().getQueryPlan();
      setState(OperationState.FINISHED);
      return query;
    } catch (HiveSQLException e) {
      throw e;
    } catch (Exception e) {
      setState(OperationState.ERROR);
      throw new HiveSQLException("Error while compiling statement: " + statement + " by "
          + e.toString(), e);
    }
  }

  public void execute() throws HiveSQLException {
    assert driver != null;
    setState(OperationState.RUNNING);

    HiveSession session = getParentSession();
    try {
      response = driver.executePlan(false);
      if (0 != response.getResponseCode()) {
        if (getState() != OperationState.CANCELED) {
          setState(OperationState.ERROR);
        }
        throw new HiveSQLException("Error while executing statement: " + statement + " by "
            + response.toDetailedMessage(), response.getSQLState(), response.getResponseCode());
      }

      mResultSchema = driver.getSchema();
      if (mResultSchema != null && mResultSchema.isSetFieldSchemas()) {
        resultSchema = new TableSchema(mResultSchema);
        setHasResultSet(true);
      } else {
        setHasResultSet(false);
      }
    } catch (HiveSQLException e) {
      throw e;
    } catch (Exception e) {
      setState(OperationState.ERROR);
      throw new HiveSQLException("Error while executing statement: " + e.toString(), e);
    }
    setState(OperationState.FINISHED);
  }

  public QueryPlan getPlan() {
    return driver.getPlan();
  }

  @Override
  public void cancel() throws HiveSQLException {
    setState(OperationState.CANCELED);
    if (driver != null) {
      driver.close();
      driver.destroy();
    }

    SessionState session = SessionState.get();
    if (session.getTmpOutputFile() != null) {
      session.getTmpOutputFile().delete();
    }
  }

  @Override
  public void close() throws HiveSQLException {
    setState(OperationState.CLOSED);
    if (driver != null) {
      driver.close();
      driver.destroy();
    }

    SessionState session = SessionState.get();
    if (session.getTmpOutputFile() != null) {
      session.getTmpOutputFile().delete();
    }
  }

  @Override
  public TableSchema getResultSetSchema() throws HiveSQLException {
    assertState(OperationState.FINISHED);
    if (resultSchema == null) {
      resultSchema = new TableSchema(driver.getSchema());
    }
    return resultSchema;
  }


  private transient final List<Object> convey = new ArrayList<Object>();

  @Override
  public RowSet getNextRowSet(FetchOrientation orientation, long maxRows) throws HiveSQLException {
    assertState(OperationState.FINISHED);
    try {
      driver.setMaxRows((int) maxRows);
      if (driver.getResults(convey)) {
        return decode(convey);
      }
      return new RowSet();
    } catch (IOException e) {
      throw new HiveSQLException(e);
    } catch (CommandNeedRetryException e) {
      throw new HiveSQLException(e);
    } catch (Exception e) {
      throw new HiveSQLException(e);
    } finally {
      convey.clear();
    }
  }

  private RowSet decode(List<Object> rows) throws Exception {
    if (driver.isFetchingTable()) {
      return prepareFromRow(rows);
    }
    return decodeFromString(rows);
  }

  // already encoded to thriftable object in ThriftFormatter
  private RowSet prepareFromRow(List<Object> rows) throws Exception {
    RowSet rowSet = new RowSet();
    for (Object row : rows) {
      rowSet.addRow(resultSchema, (Object[]) row);
    }
    return rowSet;
  }

  private RowSet decodeFromString(List<Object> rows) throws SQLException, SerDeException {
    getSerDe();
    StructObjectInspector soi = (StructObjectInspector) serde.getObjectInspector();
    List<? extends StructField> fieldRefs = soi.getAllStructFieldRefs();
    RowSet rowSet = new RowSet();

    Object[] deserializedFields = new Object[fieldRefs.size()];
    Object rowObj;
    ObjectInspector fieldOI;

    for (Object rowString : rows) {
      rowObj = serde.deserialize(new BytesWritable(((String)rowString).getBytes()));
      for (int i = 0; i < fieldRefs.size(); i++) {
        StructField fieldRef = fieldRefs.get(i);
        fieldOI = fieldRef.getFieldObjectInspector();
        Object fieldData = soi.getStructFieldData(rowObj, fieldRef);
        deserializedFields[i] = SerDeUtils.toThriftPayload(fieldData, fieldOI);
      }
      rowSet.addRow(resultSchema, deserializedFields);
    }
    return rowSet;
  }

  private SerDe getSerDe() throws SQLException {
    if (serde != null) {
      return serde;
    }
    try {
      List<FieldSchema> fieldSchemas = mResultSchema.getFieldSchemas();
      List<String> columnNames = new ArrayList<String>();
      List<String> columnTypes = new ArrayList<String>();
      StringBuilder namesSb = new StringBuilder();
      StringBuilder typesSb = new StringBuilder();

      if (fieldSchemas != null && !fieldSchemas.isEmpty()) {
        for (int pos = 0; pos < fieldSchemas.size(); pos++) {
          if (pos != 0) {
            namesSb.append(",");
            typesSb.append(",");
          }
          columnNames.add(fieldSchemas.get(pos).getName());
          columnTypes.add(fieldSchemas.get(pos).getType());
          namesSb.append(fieldSchemas.get(pos).getName());
          typesSb.append(fieldSchemas.get(pos).getType());
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
      serde.initialize(new HiveConf(), props);

    } catch (Exception ex) {
      ex.printStackTrace();
      throw new SQLException("Could not create ResultSet: " + ex.getMessage(), ex);
    }
    return serde;
  }

}
