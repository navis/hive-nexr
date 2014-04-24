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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.StringUtils;

/**
 * FetchTask implementation.
 **/
public class FetchTask extends Task<FetchWork> implements Serializable {
  private static final long serialVersionUID = 1L;

  private int maxRows = 100;
  private FetchOperator fetch;
  private ListSinkOperator sink;
  private int totalRows;

  private static transient final Log LOG = LogFactory.getLog(FetchTask.class);

  public FetchTask() {
    super();
  }

  @Override
  public void initialize(HiveConf conf, QueryPlan queryPlan, DriverContext ctx) {
    super.initialize(conf, queryPlan, ctx);
    try {
      initializeSource();
    } catch (Exception e) {
      // Bail out ungracefully - we should never hit
      // this here - but would have hit it in SemanticAnalyzer
      LOG.error(StringUtils.stringifyException(e));
      throw new RuntimeException(e);
    }
  }

  protected void initializeSource() throws Exception {
    if (!work.isPseudoMR()) {
      work.initializeForFetch();
    }
    JobConf job = new JobConf(conf);

    Operator<?> source = work.getSource();
    if (source instanceof TableScanOperator) {
      TableScanOperator ts = (TableScanOperator) source;
      ColumnProjectionUtils.appendReadColumnIDs(job, ts.getNeededColumnIDs());
      HiveInputFormat.pushFilters(job, ts);
    }
    if (work.isPseudoMR()) {
      setupTmpDir(source, job, new HashSet<Operator>());
    }
    sink = work.getSink();
    fetch = new FetchOperator(work, job, source, getVirtualColumns(source));
    source.initialize(conf, new ObjectInspector[]{fetch.getOutputObjectInspector()});
    totalRows = 0;
  }

  private List<VirtualColumn> getVirtualColumns(Operator<?> ts) {
    if (ts instanceof TableScanOperator && ts.getConf() != null) {
      return ((TableScanOperator) ts).getConf().getVirtualCols();
    }
    return null;
  }

  private void setupTmpDir(Operator<?> parent, JobConf job, Set<Operator> setup) throws IOException {
    if (parent.getChildOperators() == null) {
      return;
    }
    for (Operator<?> operator : parent.getChildOperators()) {
      if (operator instanceof FileSinkOperator && setup.add(operator)) {
        FileSinkDesc fdesc = ((FileSinkOperator) operator).getConf();
        Path tempPath = fdesc.getDirName();

        if (tempPath != null) {
          tempPath = Utilities.toTempPath(tempPath);
          LOG.info("Making Temp Directory: " + tempPath);
          FileSystem fs = tempPath.getFileSystem(job);
          fs.mkdirs(tempPath);
        }
      }
      setupTmpDir(operator, job, setup);
    }
  }

  @Override
  public int execute(DriverContext driverContext) {
    boolean success = true;
    try {
      while (fetch.pushRow()) {
      }
      fetch.clearFetchContext(false);
      return 0;
    } catch (Exception e) {
      success = false;
      console.printError("Failed with exception " + e.getMessage(),
          "\n" + StringUtils.stringifyException(e));
      try {
        fetch.clearFetchContext(true);
      } catch (HiveException e1) {
        throw new RuntimeException("Hive Runtime Error while closing operators", e1);
      }
      return 1;
    } finally {
      try {
        fetch.jobClosed(success);
      } catch (HiveException e) {
        throw new RuntimeException(e);
      }
    }
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

  public boolean fetch(List res) throws IOException, CommandNeedRetryException {
    sink.reset(res);
    int rowsRet = work.getLeastNumRows();
    if (rowsRet <= 0) {
      rowsRet = work.getLimit() >= 0 ? Math.min(work.getLimit() - totalRows, maxRows) : maxRows;
    }
    try {
      if (rowsRet <= 0) {
        fetch.clearFetchContext();
        return false;
      }
      boolean fetched = false;
      while (sink.getNumRows() < rowsRet) {
        if (!fetch.pushRow()) {
          if (work.getLeastNumRows() > 0) {
            throw new CommandNeedRetryException();
          }
          return fetched;
        }
        fetched = true;
      }
      return true;
    } catch (CommandNeedRetryException e) {
      throw e;
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    } finally {
      totalRows += sink.getNumRows();
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

  /**
   * Clear the Fetch Operator.
   *
   * @throws HiveException
   */
  public void clearFetch() throws HiveException {
    if (fetch != null) {
      fetch.clearFetchContext();
    }
  }
}
