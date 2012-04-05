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

package org.apache.hadoop.hive.ql.exec;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.DelimitedJSONSerDe;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

/**
 * FetchTask implementation.
 **/
public class FetchTask extends Task<FetchWork> implements Serializable {
  private static final long serialVersionUID = 1L;

  private int maxRows = 100;
  private FetchOperator ftOp;
  private SerDe mSerde;
  private int totalRows;
  private static transient final Log LOG = LogFactory.getLog(FetchTask.class);

  private TableScanOperator processor;
  private ListSinkOperator sink;

  public FetchTask() {
    super();
  }

  @Override
  public void initialize(HiveConf conf, QueryPlan queryPlan, DriverContext ctx) {
    super.initialize(conf, queryPlan, ctx);

    try {
      // Create a file system handle
      JobConf job = new JobConf(conf, ExecDriver.class);

      String serdeName = HiveConf.getVar(conf, HiveConf.ConfVars.HIVEFETCHOUTPUTSERDE);
      Class<? extends SerDe> serdeClass = Class.forName(serdeName, true,
          JavaUtils.getClassLoader()).asSubclass(SerDe.class);
      // cast only needed for Hadoop 0.17 compatibility
      mSerde = (SerDe) ReflectionUtils.newInstance(serdeClass, null);

      Properties serdeProp = new Properties();

      // this is the default serialization format
      if (mSerde instanceof DelimitedJSONSerDe) {
        serdeProp.put(Constants.SERIALIZATION_FORMAT, "" + Utilities.tabCode);
        serdeProp.put(Constants.SERIALIZATION_NULL_FORMAT, work.getSerializationNullFormat());
      }

      mSerde.initialize(job, serdeProp);

      processor = (TableScanOperator) work.getProcessor();
      if (processor == null) {
        ftOp = new FetchOperator(work, job);
      } else {
        HiveInputFormat.pushFilters(job, processor);
        ftOp = new FetchOperator(work, processor, job);
        sink = initProcessor(processor, job, ftOp.getOutputObjectInspector());
      }
    } catch (Exception e) {
      // Bail out ungracefully - we should never hit
      // this here - but would have hit it in SemanticAnalyzer
      LOG.error(StringUtils.stringifyException(e));
      throw new RuntimeException(e);
    }
  }

  @Override
  public int execute(DriverContext driverContext) {
    assert false;
    return 0;
  }

  /**
   * Return the tableDesc of the fetchWork.
   */
  public TableDesc getTblDesc() {
    return work.getTblDesc();
  }

  /**
   * Return the maximum number of rows returned by fetch.
   */
  public int getMaxRows() {
    return maxRows;
  }

  /**
   * Set the maximum number of rows returned by fetch.
   */
  public void setMaxRows(int maxRows) {
    this.maxRows = maxRows;
  }

  @SuppressWarnings("unchecked")
  private ListSinkOperator initProcessor(Operator<?> processor, JobConf conf,
      ObjectInspector inspector) throws HiveException {
    Operator<?> op = processor;
    for (; op.getChildOperators() != null; op = op.getChildOperators().get(0)) {
      if (op.getChildOperators().size() != 1) {
        throw new IllegalStateException("never");
      }
    }
    ListSinkOperator receiver = new ListSinkOperator();
    receiver.setParentOperators(new ArrayList<Operator<? extends Serializable>>());
    receiver.getParentOperators().add(op);

    op.setChildOperators(new ArrayList<Operator<? extends Serializable>>());
    op.getChildOperators().add(receiver);

    processor.initialize(conf, new ObjectInspector[] {inspector});
    return receiver;
  }

  @Override
  public boolean fetch(ArrayList<String> res) throws IOException, CommandNeedRetryException {
    if (processor != null) {
      sink.res = res;
      sink.numRows = 0;
    }
    try {
      int numRows = 0;
      int rowsRet = maxRows;

      if (work.getLeastNumRows() > 0) {
        if (totalRows == work.getLeastNumRows()) {
          return false;
        }
        for (int i = 0; i < work.getLeastNumRows(); i++) {
          InspectableObject io = ftOp.getNextRow();
          if (io == null) {
            throw new CommandNeedRetryException();
          }
          if (processor == null) {
            res.add(mSerde.serialize(io.o, io.oi).toString());
            numRows++;
          } else {
            processor.process(io.o, 0);
            numRows = sink.numRows;
          }
        }
        totalRows = work.getLeastNumRows();
        return true;
      }

      if ((work.getLimit() >= 0) && ((work.getLimit() - totalRows) < rowsRet)) {
        rowsRet = work.getLimit() - totalRows;
      }
      if (rowsRet <= 0) {
        ftOp.clearFetchContext();
        return false;
      }

      while (numRows < rowsRet) {
        InspectableObject io = ftOp.getNextRow();
        if (io == null) {
          if (numRows == 0) {
            return false;
          }
          totalRows += numRows;
          return true;
        }
        if (processor == null) {
          res.add(mSerde.serialize(io.o, io.oi).toString());
          numRows++;
        } else {
          processor.process(io.o, 0);
          numRows = sink.numRows;
        }
      }
      totalRows += numRows;
      return true;
    } catch (CommandNeedRetryException e) {
      throw e;
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public StageType getType() {
    return StageType.FETCH;
  }

  @Override
  public String getName() {
    return "FETCH";
  }

  @Override
  protected void localizeMRTmpFilesImpl(Context ctx) {
    String s = work.getTblDir();
    if ((s != null) && ctx.isMRTmpFileURI(s)) {
      work.setTblDir(ctx.localizeMRTmpFileURI(s));
    }

    ArrayList<String> ls = work.getPartDir();
    if (ls != null) {
      ctx.localizePaths(ls);
    }
  }

  /**
   * Clear the Fetch Operator.
   *
   * @throws HiveException
   */
  public void clearFetch() throws HiveException {
    if (null != ftOp) {
      ftOp.clearFetchContext();
    }
    if (null != processor) {
      processor.close(false);
    }
  }

  private class ListSinkOperator extends Operator {

    ArrayList<String> res;
    int numRows;

    public void processOp(Object row, int tag) throws HiveException {
      try {
        res.add(mSerde.serialize(row, outputObjInspector).toString());
        numRows++;
      } catch (SerDeException e) {
        throw new HiveException(e);
      }
    }

    public OperatorType getType() {
      return OperatorType.FORWARD;
    }
  }
}
