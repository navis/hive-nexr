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

package org.apache.hadoop.hive.ql.parse;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.ColumnStatsTask;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.OperatorUtils;
import org.apache.hadoop.hive.ql.exec.StatsTask;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.mr.ExecDriver;
import org.apache.hadoop.hive.ql.exec.spark.SparkTask;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.GenMapRedUtils;
import org.apache.hadoop.hive.ql.plan.ColumnStatsDesc;
import org.apache.hadoop.hive.ql.plan.ColumnStatsWork;
import org.apache.hadoop.hive.ql.plan.CreateTableDesc;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.LoadFileDesc;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.MoveWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;

import com.google.common.collect.Interner;
import com.google.common.collect.Interners;

/**
 * TaskCompiler is a the base class for classes that compile
 * operator pipelines into tasks.
 */
public abstract class TaskCompiler {

  protected final Log LOG = LogFactory.getLog(TaskCompiler.class);

  protected Hive db;
  protected LogHelper console;
  protected HiveConf conf;

  public void init(HiveConf conf, LogHelper console, Hive db) {
    this.conf = conf;
    this.db = db;
    this.console = console;
  }

  @SuppressWarnings({"nls", "unchecked"})
  public void compile(final ParseContext pCtx, final List<Task<? extends Serializable>> rootTasks,
      final HashSet<ReadEntity> inputs, final HashSet<WriteEntity> outputs) throws SemanticException {

    Context ctx = pCtx.getContext();
    GlobalLimitCtx globalLimitCtx = pCtx.getGlobalLimitCtx();
    QB qb = pCtx.getQB();
    List<Task<MoveWork>> mvTask = new ArrayList<Task<MoveWork>>();

    List<LoadTableDesc> loadTableWork = pCtx.getLoadTableWork();
    List<LoadFileDesc> loadFileWork = pCtx.getLoadFileWork();

    boolean isCStats = qb.isAnalyzeRewrite();

    if (pCtx.getFetchTask() != null) {
      return;
    }

    optimizeOperatorPlan(pCtx, inputs, outputs);

    /*
     * In case of a select, use a fetch task instead of a move task.
     * If the select is from analyze table column rewrite, don't create a fetch task. Instead create
     * a column stats task later.
     */
    if (pCtx.getQB().getIsQuery() && !isCStats) {
      if ((!loadTableWork.isEmpty()) || (loadFileWork.size() != 1)) {
        throw new SemanticException(ErrorMsg.GENERIC_ERROR.getMsg());
      }

      LoadFileDesc loadFileDesc = loadFileWork.get(0);

      String cols = loadFileDesc.getColumns();
      String colTypes = loadFileDesc.getColumnTypes();

      TableDesc resultTab = pCtx.getFetchTabledesc();
      if (resultTab == null) {
        String resFileFormat = HiveConf.getVar(conf, HiveConf.ConfVars.HIVEQUERYRESULTFILEFORMAT);
        resultTab = PlanUtils.getDefaultQueryOutputTableDesc(cols, colTypes, resFileFormat);
      }

      FetchWork fetch = new FetchWork(loadFileDesc.getSourcePath(),
                                      resultTab, qb.getParseInfo().getOuterQueryLimit());
      fetch.setSource(pCtx.getFetchSource());
      fetch.setSink(pCtx.getFetchSink());

      pCtx.setFetchTask((FetchTask) TaskFactory.get(fetch, conf));

      // For the FetchTask, the limit optimization requires we fetch all the rows
      // in memory and count how many rows we get. It's not practical if the
      // limit factor is too big
      int fetchLimit = HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVELIMITOPTMAXFETCH);
      if (globalLimitCtx.isEnable() && globalLimitCtx.getGlobalLimit() > fetchLimit) {
        LOG.info("For FetchTask, LIMIT " + globalLimitCtx.getGlobalLimit() + " > " + fetchLimit
            + ". Doesn't qualify limit optimiztion.");
        globalLimitCtx.disableOpt();

      }
      if (qb.getParseInfo().getOuterQueryLimit() == 0) {
        // Believe it or not, some tools do generate queries with limit 0 and than expect
        // query to run quickly. Lets meet their requirement.
        LOG.info("Limit 0. No query execution needed.");
        return;
      }
    } else if (!isCStats) {
      for (LoadTableDesc ltd : loadTableWork) {
        Task<MoveWork> tsk = TaskFactory.get(new MoveWork(null, null, ltd, null, false), conf);
        mvTask.add(tsk);
        // Check to see if we are stale'ing any indexes and auto-update them if we want
        if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVEINDEXAUTOUPDATE)) {
          IndexUpdater indexUpdater = new IndexUpdater(loadTableWork, inputs, conf);
          try {
            List<Task<? extends Serializable>> indexUpdateTasks = indexUpdater
                .generateUpdateTasks();
            for (Task<? extends Serializable> updateTask : indexUpdateTasks) {
              tsk.addDependentTask(updateTask);
            }
          } catch (HiveException e) {
            console
                .printInfo("WARNING: could not auto-update stale indexes, which are not in sync");
          }
        }
      }

      boolean oneLoadFile = true;
      for (LoadFileDesc lfd : loadFileWork) {
        if (qb.isCTAS()) {
          assert (oneLoadFile); // should not have more than 1 load file for
          // CTAS
          // make the movetask's destination directory the table's destination.
          Path location;
          String loc = qb.getTableDesc().getLocation();
          if (loc == null) {
            // get the table's default location
            Path targetPath;
            try {
              String[] names = Utilities.getDbTableName(qb.getTableDesc().getTableName());
              if (!db.databaseExists(names[0])) {
                throw new SemanticException("ERROR: The database " + names[0]
                    + " does not exist.");
              }
              Warehouse wh = new Warehouse(conf);
              targetPath = wh.getTablePath(db.getDatabase(names[0]), names[1]);
            } catch (HiveException e) {
              throw new SemanticException(e);
            } catch (MetaException e) {
              throw new SemanticException(e);
            }

            location = targetPath;
          } else {
            location = new Path(loc);
          }
          lfd.setTargetDir(location);

          oneLoadFile = false;
        }
        mvTask.add(TaskFactory.get(new MoveWork(null, null, null, lfd, false), conf));
      }
    }

    generateTaskTree(rootTasks, pCtx, mvTask, inputs, outputs);

    /*
     * If the query was the result of analyze table column compute statistics rewrite, create
     * a column stats task instead of a fetch task to persist stats to the metastore.
     */
    if (isCStats) {
      genColumnStatsTask(qb, loadTableWork, loadFileWork, rootTasks);
    }

    // For each task, set the key descriptor for the reducer
    for (Task<? extends Serializable> rootTask : rootTasks) {
      GenMapRedUtils.setKeyAndValueDescForTaskTree(rootTask);
    }

    // If a task contains an operator which instructs bucketizedhiveinputformat
    // to be used, please do so
    for (Task<? extends Serializable> rootTask : rootTasks) {
      setInputFormat(rootTask);
    }

    optimizeTaskPlan(rootTasks, pCtx, ctx);

    finalizeTaskPlan(rootTasks, pCtx, ctx);

    decideExecMode(rootTasks, ctx, globalLimitCtx);

    if (qb.isCTAS()) {
      // generate a DDL task and make it a dependent task of the leaf
      CreateTableDesc crtTblDesc = qb.getTableDesc();

      crtTblDesc.validate(conf);

      // clear the mapredWork output file from outputs for CTAS
      // DDLWork at the tail of the chain will have the output
      Iterator<WriteEntity> outIter = outputs.iterator();
      while (outIter.hasNext()) {
        switch (outIter.next().getType()) {
        case DFS_DIR:
        case LOCAL_DIR:
          outIter.remove();
          break;
        default:
          break;
        }
      }
      Task<? extends Serializable> crtTblTask = TaskFactory.get(new DDLWork(
          inputs, outputs, crtTblDesc), conf);

      // find all leaf tasks and make the DDLTask as a dependent task of all of
      // them
      HashSet<Task<? extends Serializable>> leaves = new LinkedHashSet<Task<? extends Serializable>>();
      getLeafTasks(rootTasks, leaves);
      assert (leaves.size() > 0);
      for (Task<? extends Serializable> task : leaves) {
        if (task instanceof StatsTask) {
          // StatsTask require table to already exist
          for (Task<? extends Serializable> parentOfStatsTask : task.getParentTasks()) {
            parentOfStatsTask.addDependentTask(crtTblTask);
          }
          for (Task<? extends Serializable> parentOfCrtTblTask : crtTblTask.getParentTasks()) {
            parentOfCrtTblTask.removeDependentTask(task);
          }
          crtTblTask.addDependentTask(task);
        } else {
          task.addDependentTask(crtTblTask);
        }
      }
    }

    if (globalLimitCtx.isEnable() && pCtx.getFetchTask() != null) {
      LOG.info("set least row check for FetchTask: " + globalLimitCtx.getGlobalLimit());
      pCtx.getFetchTask().getWork().setLeastNumRows(globalLimitCtx.getGlobalLimit());
    }

    if (globalLimitCtx.isEnable() && globalLimitCtx.getLastReduceLimitDesc() != null) {
      LOG.info("set least row check for LimitDesc: " + globalLimitCtx.getGlobalLimit());
      globalLimitCtx.getLastReduceLimitDesc().setLeastRows(globalLimitCtx.getGlobalLimit());
      List<ExecDriver> mrTasks = Utilities.getMRTasks(rootTasks);
      for (ExecDriver tsk : mrTasks) {
        tsk.setRetryCmdWhenFail(true);
      }
      List<SparkTask> sparkTasks = Utilities.getSparkTasks(rootTasks);
      for (SparkTask sparkTask : sparkTasks) {
        sparkTask.setRetryCmdWhenFail(true);
      }
    }

    Interner<TableDesc> interner = Interners.newStrongInterner();
    for (Task<? extends Serializable> rootTask : rootTasks) {
      GenMapRedUtils.internTableDesc(rootTask, interner);
    }
  }


  /**
   * A helper function to generate a column stats task on top of map-red task. The column stats
   * task fetches from the output of the map-red task, constructs the column stats object and
   * persists it to the metastore.
   *
   * This method generates a plan with a column stats task on top of map-red task and sets up the
   * appropriate metadata to be used during execution.
   *
   * @param qb
   */
  @SuppressWarnings("unchecked")
  protected void genColumnStatsTask(QB qb, List<LoadTableDesc> loadTableWork,
      List<LoadFileDesc> loadFileWork, List<Task<? extends Serializable>> rootTasks) {
    QBParseInfo qbParseInfo = qb.getParseInfo();
    ColumnStatsTask cStatsTask = null;
    ColumnStatsWork cStatsWork = null;
    FetchWork fetch = null;
    String tableName = qbParseInfo.getTableName();
    List<String> colName = qbParseInfo.getColName();
    List<String> colType = qbParseInfo.getColType();
    boolean isTblLevel = qbParseInfo.isTblLvl();

    String cols = loadFileWork.get(0).getColumns();
    String colTypes = loadFileWork.get(0).getColumnTypes();

    String resFileFormat = HiveConf.getVar(conf, HiveConf.ConfVars.HIVEQUERYRESULTFILEFORMAT);
    TableDesc resultTab = PlanUtils.getDefaultQueryOutputTableDesc(cols, colTypes, resFileFormat);

    fetch = new FetchWork(loadFileWork.get(0).getSourcePath(),
        resultTab, qb.getParseInfo().getOuterQueryLimit());

    ColumnStatsDesc cStatsDesc = new ColumnStatsDesc(tableName,
        colName, colType, isTblLevel);
    cStatsWork = new ColumnStatsWork(fetch, cStatsDesc);
    cStatsTask = (ColumnStatsTask) TaskFactory.get(cStatsWork, conf);
    rootTasks.add(cStatsTask);
  }


  /**
   * Find all leaf tasks of the list of root tasks.
   */
  protected void getLeafTasks(List<Task<? extends Serializable>> rootTasks,
      HashSet<Task<? extends Serializable>> leaves) {

    for (Task<? extends Serializable> root : rootTasks) {
      getLeafTasks(root, leaves);
    }
  }

  private void getLeafTasks(Task<? extends Serializable> task,
      HashSet<Task<? extends Serializable>> leaves) {
    if (task.getDependentTasks() == null) {
      if (!leaves.contains(task)) {
        leaves.add(task);
      }
    } else {
      getLeafTasks(task.getDependentTasks(), leaves);
    }
  }

  /*
   * Called to transform tasks into local tasks where possible/desirable
   */
  protected abstract void decideExecMode(List<Task<? extends Serializable>> rootTasks, Context ctx,
      GlobalLimitCtx globalLimitCtx) throws SemanticException;

  /*
   * Called at the beginning of the compile phase to have another chance to optimize the operator plan
   */
  protected void optimizeOperatorPlan(ParseContext pCtxSet, Set<ReadEntity> inputs,
      Set<WriteEntity> outputs) throws SemanticException {
  }

  /*
   * Called after the tasks have been generated to run another round of optimization
   */
  protected abstract void optimizeTaskPlan(List<Task<? extends Serializable>> rootTasks,
      ParseContext pCtx, Context ctx) throws SemanticException;

  protected abstract List<MapWork> extractMapWorks(List<Task<? extends Serializable>> rootTasks);

  protected void finalizeTaskPlan(List<Task<? extends Serializable>> rootTasks,
      ParseContext pCtx, Context ctx) throws SemanticException {
    for (MapWork mapWork : extractMapWorks(rootTasks)) {

      // for single sourced multiple aliases, multiple filter expr in TS
      // cannot be evaluated (see HiveInputFormat#getSplits)
      // todo : 'or'ed predicate can be useful for some storage handlers (not for hbase, currently)
      Map<String, Operator<? extends OperatorDesc>> aliasToWork = mapWork.getAliasToWork();
      for (ArrayList<String> aliases : mapWork.getPathToAliases().values()) {
        if (aliases.size() <= 1) {
          continue;
        }
        List<TableScanOperator> tss = new ArrayList<TableScanOperator>();
        List<List<ExprNodeDesc>> tsExprs = new ArrayList<List<ExprNodeDesc>>();
        for (String alias : aliases) {
          Operator<?> operator = aliasToWork.get(alias);
          if (operator instanceof TableScanOperator) {
            TableScanOperator ts = (TableScanOperator) operator;
            if (ts.getConf().getFilterExpr() != null) {
              tss.add(ts);
              tsExprs.add(ExprNodeDescUtils.split(ts.getConf().getFilterExpr()));
            }
          }
        }
        if (tss.isEmpty()) {
          continue;
        }
        Collection<ExprNodeDesc> commonSet = getCommonExprs(tsExprs);
        if (!commonSet.isEmpty()) {
          tsExprs = exceptCommonExpr(tsExprs, commonSet);
        }
        ExprNodeGenericFuncDesc commonExpr =
            (ExprNodeGenericFuncDesc) ExprNodeDescUtils.mergePredicates(commonSet);
        // recreate filter for each TS
        for (int i = 0 ; i < tss.size(); i++) {
          TableScanOperator ts = tss.get(i);
          ts.getConf().setFilterExpr(commonExpr);
          List<ExprNodeDesc> tsExpr = tsExprs.get(i);
          if (tsExpr.isEmpty()) {
            continue; // nothing todo
          }
          FilterOperator child = OperatorUtils.findSingleOperator(ts, FilterOperator.class);
          if (child != null) {
            FilterDesc filterDesc = child.getConf();
            filterDesc.setPredicate(
                ExprNodeDescUtils.appendPredicatesWithDedup(filterDesc.getPredicate(), tsExpr));
          } else {
            ExprNodeDesc predicate = ExprNodeDescUtils.mergePredicates(tsExpr);
            FilterDesc filterDesc = new FilterDesc(predicate, false);
            Operator<?>[] children = new Operator[ts.getNumChild()];
            for (int j = 0; j < children.length; j++) {
              children[j] = ts.getChildOperators().get(j);
              children[j].getParentOperators().clear();
            }
            Operator<?> newFilter = OperatorFactory.get(filterDesc, children);
            List<Operator<? extends OperatorDesc>> parents =
                new ArrayList<Operator<? extends OperatorDesc>>(Arrays.asList(ts));
            newFilter.setParentOperators(parents);
            ts.getChildOperators().clear();
            ts.getChildOperators().add(newFilter);
          }
        }
      }
    }
  }

  private Collection<ExprNodeDesc> getCommonExprs(List<List<ExprNodeDesc>> tsExprs) {
    Collection<ExprNodeDesc> commonExprs = null;
    for (List<ExprNodeDesc> exprs : tsExprs) {
      if (commonExprs == null) {
        commonExprs = new ArrayList<ExprNodeDesc>(exprs);
      } else {
        ExprNodeDescUtils.retainAll(commonExprs, exprs);
      }
      if (commonExprs.isEmpty()) {
        return Collections.emptyList();
      }
    }
    return commonExprs;
  }

  private List<List<ExprNodeDesc>> exceptCommonExpr(List<List<ExprNodeDesc>> tsExprs,
     Collection<ExprNodeDesc> common) {
    for (List<ExprNodeDesc> exprs : tsExprs) {
      exprs.removeAll(common);
    }
    return tsExprs;
  }

  /*
   * Called to set the appropriate input format for tasks
   */
  protected abstract void setInputFormat(Task<? extends Serializable> rootTask);

  /*
   * Called to generate the task tree from the parse context/operator tree
   */
  protected abstract void generateTaskTree(List<Task<? extends Serializable>> rootTasks, ParseContext pCtx,
      List<Task<MoveWork>> mvTask, Set<ReadEntity> inputs, Set<WriteEntity> outputs) throws SemanticException;

  /**
   * Create a clone of the parse context
   */
  public ParseContext getParseContext(ParseContext pCtx, List<Task<? extends Serializable>> rootTasks) {
    ParseContext clone = new ParseContext(conf,
        pCtx.getQB(),
        pCtx.getOpToPartPruner(), pCtx.getOpToPartList(), pCtx.getTopOps(),
        pCtx.getJoinOps(), pCtx.getSmbMapJoinOps(),
        pCtx.getLoadTableWork(), pCtx.getLoadFileWork(), pCtx.getContext(),
        pCtx.getIdToTableNameMap(), pCtx.getDestTableId(), pCtx.getUCtx(),
        pCtx.getListMapJoinOpsNoReducer(),
        pCtx.getPrunedPartitions(), pCtx.getOpToSamplePruner(), pCtx.getGlobalLimitCtx(),
        pCtx.getNameToSplitSample(), pCtx.getSemanticInputs(), rootTasks,
        pCtx.getOpToPartToSkewedPruner(), pCtx.getViewAliasToInput(),
        pCtx.getReduceSinkOperatorsAddedByEnforceBucketingSorting(),
        pCtx.getQueryProperties());
    clone.setFetchTask(pCtx.getFetchTask());
    clone.setLineageInfo(pCtx.getLineageInfo());
    clone.setMapJoinOps(pCtx.getMapJoinOps());
    return clone;
  }
}
