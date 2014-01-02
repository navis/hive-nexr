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

package org.apache.hadoop.hive.ql;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskRunner;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * DriverContext.
 *
 */
public class DriverContext {

  private static final Log LOG = LogFactory.getLog(Driver.class.getName());
  private static final SessionState.LogHelper console = new SessionState.LogHelper(LOG);

  private static final int SLEEP_TIME = 2000;

  private Queue<Task<? extends Serializable>> runnable;
  private Queue<TaskRunner> running;

  // how many jobs have been started
  private int curJobNo;

  private Context ctx;
  private volatile boolean shutdown;

  public DriverContext() {
  }

  public DriverContext(Context ctx) {
    this.runnable = new ConcurrentLinkedQueue<Task<? extends Serializable>>();
    this.running = new LinkedBlockingQueue<TaskRunner>();
    this.ctx = ctx;
  }

  public boolean isShutdown() {
    return shutdown;
  }

  public boolean isRunning() {
    return !shutdown && (!running.isEmpty() || !runnable.isEmpty());
  }

  public Queue<Task<? extends Serializable>> getRunnable() {
    return runnable;
  }

  public void launching(TaskRunner runner) {
    running.add(runner);
  }

  public Task<? extends Serializable> getRunnable(int maxthreads) {
    if (runnable.peek() != null && running.size() < maxthreads) {
      return runnable.remove();
    }
    return null;
  }

  /**
   * Polls running tasks to see if a task has ended.
   *
   * @return The result object for any completed/failed task
   */
  public TaskRunner pollFinished() throws InterruptedException {
    while (!shutdown) {
      Iterator<TaskRunner> it = running.iterator();
      while (it.hasNext()) {
        TaskRunner runner = it.next();
        if (runner != null && !runner.isRunning()) {
          it.remove();
          return runner;
        }
      }
      Thread.sleep(SLEEP_TIME);
    }
    return null;
  }

  /**
   * Cleans up remaining tasks in case of failure
   */
  public void shutdown() {
    if (isRunning()) {
      LOG.warn("Shutting down query " + ctx.getCmd());
    }
    shutdown = true;
    for (TaskRunner runner : running) {
      if (runner.isRunning()) {
        Task<?> task = runner.getTask();
        LOG.warn("Shutting down task : " + task);
        try {
          task.shutdown();
        } catch (Exception e) {
          console.printError("Exception on shutting down task " + task.getId() + ": " + e);
        }
        Thread thread = runner.getRunner();
        if (thread != null) {
          thread.interrupt();
        }
      }
    }
    running.clear();
  }

  /**
   * Checks if a task can be launched.
   *
   * @param tsk
   *          the task to be checked
   * @return true if the task is launchable, false otherwise
   */

  public static boolean isLaunchable(Task<? extends Serializable> tsk) {
    // A launchable task is one that hasn't been queued, hasn't been
    // initialized, and is runnable.
    return !tsk.getQueued() && !tsk.getInitialized() && tsk.isRunnable();
  }

  public void addToRunnable(Task<? extends Serializable> tsk) {
    runnable.add(tsk);
    tsk.setQueued();
  }

  public int getCurJobNo() {
    return curJobNo;
  }

  public Context getCtx() {
    return ctx;
  }

  public void incCurJobNo(int amount) {
    this.curJobNo = this.curJobNo + amount;
  }
}
