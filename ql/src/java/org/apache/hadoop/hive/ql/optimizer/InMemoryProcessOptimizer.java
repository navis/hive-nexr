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

package org.apache.hadoop.hive.ql.optimizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.ForwardTask;
import org.apache.hadoop.hive.ql.exec.HashReducer;
import org.apache.hadoop.hive.ql.exec.LimitOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.ppr.PartitionPruner;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.parse.QB;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.SplitSample;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class InMemoryProcessOptimizer {

  static final int SIMPLE = 0;
  static final int SIMPLE_AGGRESSIVE = 1;
  static final int FULL_AGGRESSIVE = 2;

  private final Log LOG = LogFactory.getLog(InMemoryProcessOptimizer.class.getName());

  public ParseContext transform(ParseContext pctx, int mode) throws SemanticException {
    Map<String, Operator<? extends Serializable>> topOps = pctx.getTopOps();
    List<Task<?>> tasks = new ArrayList<Task<?>>();
    for (Map.Entry<String, Operator<? extends Serializable>> entry : topOps.entrySet()) {
      String alias = entry.getKey();
      Operator topOp = entry.getValue();
      if (topOp instanceof TableScanOperator) {
        try {
          Task<?> task = optimize(pctx, alias, (TableScanOperator) topOp, mode);
          if (task == null) {
            return null;
          }
          tasks.add(task);
        } catch (HiveException e) {
          // Has to use full name to make sure it does not conflict with
          // org.apache.commons.lang.StringUtils
          LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
          if (e instanceof SemanticException) {
            throw (SemanticException) e;
          }
          throw new SemanticException(e.getMessage(), e);
        }
      }
    }
    if (mode != FULL_AGGRESSIVE) {
      pctx.setFetchTask((FetchTask) tasks.get(0));
    } else {
      prepareReducers(topOps.values());
      pctx.getRootTasks().addAll(tasks);
    }
    topOps.clear();
    return pctx;
  }

  @SuppressWarnings("unchecked")
  private Task<?> optimize(ParseContext pctx, String alias, TableScanOperator ts,
      int mode) throws HiveException {
    FetchData fetch = checkTree(pctx, alias, ts, mode);
    if (fetch != null) {
      int limit = pctx.getQB().getParseInfo().getOuterQueryLimit();
      FetchWork fetchWork = fetch.convertToWork(limit, ts);
      Task<?> fetchTask = mode == FULL_AGGRESSIVE ? new ForwardTask(fetchWork) :
          (FetchTask) TaskFactory.get(fetchWork, pctx.getConf());
      fetch.completed(pctx, mode);
      return fetchTask;
    }
    return null;
  }

  private FetchData checkTree(ParseContext pctx, String alias,
      TableScanOperator ts, int mode) throws HiveException {
    SplitSample splitSample = pctx.getNameToSplitSample().get(alias);
    if (splitSample != null) {
      return null;  // not-yet
    }
    QB qb = pctx.getQB();
    if (mode == SIMPLE && qb.hasTableSample(alias)) {
      return null;
    }

    Table table = qb.getMetaData().getAliasToTable().get(alias);
    if (table == null) {
      return null;
    }
    if (!table.isPartitioned()) {
      return checkOperators(new FetchData(table), ts, mode, false);
    }

    boolean bypassFilter = false;
    if (HiveConf.getBoolVar(pctx.getConf(), HiveConf.ConfVars.HIVEOPTPPD)) {
      ExprNodeDesc pruner = pctx.getOpToPartPruner().get(ts);
      bypassFilter = PartitionPruner.onlyContainsPartnCols(table, pruner);
    }
    if (mode != SIMPLE || bypassFilter) {
      PrunedPartitionList prunedPartitions = pctx.getPrunedPartitions(alias, ts);
      if (mode != SIMPLE || prunedPartitions.getUnknownPartns().isEmpty()) {
        bypassFilter &= prunedPartitions.getUnknownPartns().isEmpty();
        return checkOperators(new FetchData(prunedPartitions), ts, mode, bypassFilter);
      }
    }
    return null;
  }

  private FetchData checkOperators(FetchData fetch, TableScanOperator ts, int mode,
      boolean bypassFilter) {
    if (mode == FULL_AGGRESSIVE) {
      fetch.needProcessor = true;
      return fetch;
    }
    if (ts.getChildOperators().size() != 1) {
      return null;
    }
    boolean needProcessor = false;
    Operator<?> op = ts.getChildOperators().get(0);
    for (; ; op = op.getChildOperators().get(0)) {
      if (mode == SIMPLE_AGGRESSIVE) {
        if (!(op instanceof LimitOperator || op instanceof FilterOperator
            || op instanceof SelectOperator)) {
          break;
        }
        needProcessor |= op instanceof FilterOperator && !bypassFilter ||
            op instanceof SelectOperator && !((SelectOperator)op).getConf().isSelectStar();
      } else if (!(op instanceof LimitOperator || op instanceof FilterOperator && bypassFilter
          || op instanceof SelectOperator && ((SelectOperator) op).getConf().isSelectStar())) {
        break;
      }
      if (op.getChildOperators() == null || op.getChildOperators().size() != 1) {
        return null;
      }
    }
    if (op instanceof FileSinkOperator &&
        op.getChildOperators() == null || op.getChildOperators().isEmpty()) {
      fetch.needProcessor = needProcessor;
      fetch.fileSink = op;
      return fetch;
    }
    return null;
  }

  private class FetchData {

    private Table table;
    private List<Partition> partitions = new ArrayList<Partition>();
    private HashSet<ReadEntity> inputs = new HashSet<ReadEntity>();
    private Operator<?> fileSink;
    private boolean needProcessor;

    private FetchData(Table table) {
      this.table = table;
    }

    private FetchData(PrunedPartitionList partsList) {
      partitions.addAll(partsList.getUnknownPartns());
      partitions.addAll(partsList.getConfirmedPartns());
    }

    private FetchWork convertToWork(int limit, Operator<?> processor) throws HiveException {
      FetchWork fetchWork = convertToWork(limit);
      if (needProcessor) {
        fetchWork.setProcessor(processor);
      }
      return fetchWork;
    }

    private FetchWork convertToWork(int limit) throws HiveException {
      if (table != null) {
        String path = table.getPath().toString();
        FetchWork work = new FetchWork(path, Utilities.getTableDesc(table), limit);
        PlanUtils.configureInputJobPropertiesForStorageHandler(work.getTblDesc());
        inputs.add(new ReadEntity(table));
        return work;
      }
      List<String> listP = new ArrayList<String>();
      List<PartitionDesc> partP = new ArrayList<PartitionDesc>();

      for (Partition partition : partitions) {
        listP.add(partition.getPartitionPath().toString());
        partP.add(Utilities.getPartitionDesc(partition));
        inputs.add(new ReadEntity(partition));
      }
      FetchWork work = new FetchWork(listP, partP, limit);
      if (!work.getPartDesc().isEmpty()) {
        PartitionDesc part0 = work.getPartDesc().get(0);
        PlanUtils.configureInputJobPropertiesForStorageHandler(part0.getTableDesc());
      }
      return work;
    }

    private void completed(ParseContext pctx, int mode) {
      pctx.getSemanticInputs().addAll(inputs);
      if (mode != FULL_AGGRESSIVE) {
        fileSink.getParentOperators().get(0).setChildOperators(null);
        fileSink.setParentOperators(null);
      }
    }
  }

  private void prepareReducers(Collection<Operator<? extends Serializable>> operators) {
    Set<Operator<? extends Serializable>> targets =
        new HashSet<Operator<? extends Serializable>>();
    for (Operator<?> operator : operators) {
      targets.addAll(createReducer(operator, operator));
    }
    if (!targets.isEmpty()) {
      prepareReducers(targets);
    }
  }

  private Set<Operator<?>> createReducer(Operator<?> start, Operator<?> current) {
    if (current == null || current.getChildOperators() == null) {
      return Collections.emptySet();
    }
    Set<Operator<?>> next = new HashSet<Operator<?>>();
    if (current instanceof ReduceSinkOperator && current.getChildOperators() != null) {
      ReduceSinkOperator rs = (ReduceSinkOperator) current;
      for (Operator<?> child : rs.getChildOperators()) {
        if (child.getHashReducer() == null) {
          child.setHashReducer(new HashReducer(child, rs));
        }
        child.getHashReducer().setValueDesc(rs);
        start.setOutputCollector(child.getHashReducer());
        next.add(child);
      }
      return next;
    }
    for (Operator<?> child : current.getChildOperators()) {
      next.addAll(createReducer(start, child));
    }
    return next;
  }
}
