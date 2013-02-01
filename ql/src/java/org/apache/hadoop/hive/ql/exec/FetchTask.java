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
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.StringUtils;

/**
 * FetchTask implementation.
 **/
public class FetchTask extends Task<FetchWork> implements Serializable {
  private static final long serialVersionUID = 1L;

  private int maxRows = 100;
  private int totalRows;
  private RowProcessor processor;

  private static transient final Log LOG = LogFactory.getLog(FetchTask.class);

  public FetchTask() {
    super();
  }

  @Override
  public void initialize(HiveConf conf, QueryPlan queryPlan, DriverContext ctx) {
    super.initialize(conf, queryPlan, ctx);
    work.initializeForFetch();

    try {
      // Create a file system handle
      JobConf job = new JobConf(conf);

      Operator<?> source = work.getSource();
      if (source instanceof TableScanOperator) {
        TableScanOperator ts = (TableScanOperator) source;
        // push down projections
        ColumnProjectionUtils.appendReadColumns(
            job, ts.getNeededColumnIDs(), ts.getNeededColumns());
        // push down filters
        HiveInputFormat.pushFilters(job, ts);
      }
      processor = createProcessor(work, conf, job);

    } catch (Exception e) {
      // Bail out ungracefully - we should never hit
      // this here - but would have hit it in SemanticAnalyzer
      LOG.error(StringUtils.stringifyException(e));
      throw new RuntimeException(e);
    }
  }

  private RowProcessor createProcessor(FetchWork work, HiveConf conf, JobConf job)
      throws HiveException {
    final Operator<?> source = work.getSource();

    RowFetcher fetcher;
    if (!work.isMergeFetcher()) {
      fetcher = new FetchOperator(work, job, source, getVirtualColumns(source));
    } else {
      fetcher = new MergeSortingFetcher(work, job) {
        private boolean initialized;
        public int compare(List<Object> o1, List<Object> o2) {
          return compareKeys(o1, o2);
        }
        @Override
        public InspectableObject fetchRow() throws IOException, HiveException {
          if (!initialized) {
            // this is called in here because setupFetchContext() is called in Driver
            // before executing plan
            setupSegments(getPaths(fetchWork.getTblDir(), jobConf));
            initialized = true;
          }
          return super.fetchRow();
        }
        @Override
        public boolean pushRow() throws IOException, HiveException {
          InspectableObject row = fetchRow();
          if (row != null) {
            source.processOp(row.o, 0);
          } else {
            source.flush();
          }
          return row != null;
        }
      };
    }
    source.initialize(conf, new ObjectInspector[]{fetcher.setupFetchContext()});
    return new RowProcessor(fetcher, work.getSink());
  }

  private int compareKeys(List<Object> k1, List<Object> k2) {
    int ret = k1.size() - k2.size();
    if (ret != 0) {
      return ret;
    }
    for (int i = 0; i < k1.size(); i++) {
      WritableComparable key_1 = (WritableComparable) k1.get(i);
      WritableComparable key_2 = (WritableComparable) k2.get(i);
      if (key_1 == null && key_2 == null) {
        return -1;
      }
      if (key_1 == null) {
        return -1;
      }
      if (key_2 == null) {
        return 1;
      }
      ret = WritableComparator.get(key_1.getClass()).compare(key_1, key_2);
      if(ret != 0) {
        return ret;
      }
    }
    return ret;
  }

  private List<VirtualColumn> getVirtualColumns(Operator<?> ts) {
    if (ts instanceof TableScanOperator && ts.getConf() != null) {
      return ((TableScanOperator)ts).getConf().getVirtualCols();
    }
    return null;
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

  public boolean fetch(List res) throws IOException, CommandNeedRetryException {
    processor.reset(res);
    int rowsRet = work.getLeastNumRows();
    if (rowsRet <= 0) {
      rowsRet = work.getLimit() >= 0 ? Math.min(work.getLimit() - totalRows, maxRows) : maxRows;
    }
    try {
        if (rowsRet <= 0 || work.getLimit() == totalRows) {
        processor.clearFetchContext();
        return false;
      }
      boolean fetched = false;
      while (processor.getNumRows() < rowsRet) {
        if (!processor.pushRow()) {
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
      totalRows += processor.getNumRows();
    }
  }

  public boolean isFetchFrom(FileSinkDesc fs) {
    return fs.getFinalDirName().equals(work.getTblDir());
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
    if (processor != null) {
      processor.clearFetchContext();
    }
  }

  private class RowProcessor {

    private final RowFetcher fetcher;
    private final ListSinkOperator sink;

    public RowProcessor(RowFetcher fetcher, ListSinkOperator sink) {
      this.fetcher = fetcher;
      this.sink = sink;
    }

    public void reset(List res) {
      sink.reset(res);
    }

    public boolean pushRow() throws IOException, HiveException {
      return fetcher.pushRow();
    }

    public int getNumRows() {
      return sink.getNumRows();
    }

    public void clearFetchContext() throws HiveException {
      fetcher.clearFetchContext();
    }
  }
}
