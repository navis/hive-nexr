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

import java.io.Serializable;
import java.util.List;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVEFETCHTASKCONVERSIONINTERM;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.optimizer.SimpleFetchOptimizer;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * TaskRunner implementation.
 **/

public class TaskRunner extends Thread {

  protected Task<? extends Serializable> tsk;
  protected TaskResult result;
  protected SessionState ss;

  protected Thread runner;

  public TaskRunner(Task<? extends Serializable> tsk, TaskResult result) {
    this.tsk = tsk;
    this.result = result;
    ss = SessionState.get();
  }

  public Task<? extends Serializable> getTask() {
    return tsk;
  }

  public TaskResult getTaskResult() {
    return result;
  }

  public Thread getRunner() {
    return runner;
  }

  public boolean isRunning() {
    return result.isRunning();
  }

  @Override
  public void run() {
    runner = Thread.currentThread();
    try {
      SessionState.start(ss);
      runSequential();
    } finally {
      runner = null;
    }
  }

  /**
   * Launches a task, and sets its exit value in the result variable.
   */

  public void runSequential() {
    int exitVal = -101;
    try {
      exitVal = localizeMRTask(tsk).executeTask();
    } catch (Throwable t) {
      t.printStackTrace();
    } finally {
      if (Thread.interrupted()) {
        LogFactory.getLog(TaskRunner.class.getName()).warn("Task for " + tsk.getId() +
            " was interrupted while execution");
      }
    }
    result.setExitVal(exitVal);
  }

  private Task<?> localizeMRTask(final Task<?> tsk) {
    if (!(tsk instanceof MapRedTask) ||
        !HiveConf.getBoolVar(tsk.getConf(), HIVEFETCHTASKCONVERSIONINTERM)) {
      return tsk;
    }
    final MapRedTask mapRedTask = (MapRedTask) tsk;
    if (!mapRedTask.getWork().hasMapJoin()) {
      final SimpleFetchOptimizer optimizer = new SimpleFetchOptimizer();
      final List<FetchTask> fetchTasks;
      try {
        fetchTasks = optimizer.transform(mapRedTask.getWork(), tsk.driverContext.getCtx());
      } catch (Exception e) {
        tsk.console.printInfo("Failed to localize task " + tsk.getId());
        return tsk;
      }
      if (fetchTasks != null) {
        tsk.console.printInfo("Executing localized task for " + tsk.getId());
        Task<?> task = new Task() {
          protected int execute(DriverContext driverContext) {
            for (FetchTask fetch : fetchTasks) {
              try {
                fetch.initialize(tsk.getConf(), tsk.getQueryPlan(), tsk.driverContext);
              } catch (Exception e) {
                return rollback(driverContext);
              }
            }
            for (FetchTask fetch : fetchTasks) {
              int code = fetch.execute(driverContext);
              if (code != 0) {
                return rollback(driverContext);
              }
            }
            tsk.setDone();
            return 0;
          }

          private int rollback(DriverContext driverContext) {
            tsk.console.printInfo("Failed to execute localized task for " + tsk.getId());
            optimizer.untransform(mapRedTask.getWork());
            return tsk.execute(driverContext);
          }

          public String getName() { return tsk.getName(); }
          public StageType getType() { return tsk.getType(); }
          protected void localizeMRTmpFilesImpl(Context ctx) { }
        };
        task.initialize(tsk.getConf(), tsk.getQueryPlan(), tsk.driverContext);
        return task;
      }
    }
    return tsk;
  }
}
