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
package org.apache.hadoop.hive.ql.hooks;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.ExplainTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.json.JSONObject;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * ATSHook sends query + plan info to Yarn App Timeline Server. To enable (hadoop 2.4 and up) set
 * hive.exec.pre.hooks/hive.exec.post.hooks/hive.exec.failure.hooks to include this class.
 */
public class ATSHook implements ExecuteWithHookContext {

  private static final Log LOG = LogFactory.getLog(ATSHook.class.getName());
  private static final Object LOCK = new Object();
  private static ExecutorService executor;
  private static TimelineClient timelineClient;
  private enum EntityTypes { HIVE_QUERY_ID };
  private enum EventTypes { QUERY_SUBMITTED, QUERY_COMPLETED };
  private enum OtherInfoTypes { QUERY, STATUS, TEZ, MAPRED };
  private enum PrimaryFilterTypes { user, operationid };

  private static final String ATS_QUEUE_SIZE = "hive.ats.queue.size";
  private static final String ATS_SHUTDOWN_WAIT = "hive.ats.shutdown.wait";

  private static final int QUEUE_SIZE = -1;
  private static final int WAIT_TIME = 3;
  private static final int SUBMIT_DELAY_LOG_THRESHOLD = 20;

  private ExecutorService getExecutors(HiveConf conf) {
    synchronized(LOCK) {
      if (executor == null) {

        final int queue = conf.getInt(ATS_QUEUE_SIZE, QUEUE_SIZE);
        final int wait = conf.getInt(ATS_SHUTDOWN_WAIT, WAIT_TIME);

        BlockingQueue<Runnable> workQueue =
            new LinkedBlockingQueue<Runnable>(queue < 0 ? Integer.MAX_VALUE : queue);

        executor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, workQueue,
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("ATS Logger %d").build(),
            new RejectedExecutionHandler() {
              @Override
              public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
                if (!e.isShutdown()) {
                  Runnable oldest = e.getQueue().poll();
                  LOG.warn("Discarded " + oldest + " by queue full");
                  e.execute(r);
                }
              }
            });

        YarnConfiguration yarnConf = new YarnConfiguration();
        timelineClient = TimelineClient.createTimelineClient();
        timelineClient.init(yarnConf);
        timelineClient.start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
          @Override
          public void run() {
            try {
              executor.shutdown();
              executor.awaitTermination(wait, TimeUnit.SECONDS);
              executor = null;
            } catch(InterruptedException ie) {
              /* ignore */
            } finally {
              timelineClient.stop();
            }
          }
        });
      }
    }
    return executor;
  }

  @Override
  public void run(HookContext hookContext) throws Exception {
    long currentTime = System.currentTimeMillis();
    getExecutors(hookContext.getConf()).submit(new ATSWork(hookContext, currentTime));
  }

  TimelineEntity createPreHookEvent(String queryId, String query, JSONObject explainPlan,
      long startTime, String user, int numMrJobs, int numTezJobs, String opId) throws Exception {

    JSONObject queryObj = new JSONObject();
    queryObj.put("queryText", query);
    queryObj.put("queryPlan", explainPlan);

    LOG.info("Received pre-hook notification for :" + queryId);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Otherinfo: " + queryObj.toString());
      LOG.debug("Operation id: <" + opId + ">");
    }

    TimelineEntity atsEntity = new TimelineEntity();
    atsEntity.setEntityId(queryId);
    atsEntity.setEntityType(EntityTypes.HIVE_QUERY_ID.name());
    atsEntity.addPrimaryFilter(PrimaryFilterTypes.user.name(), user);
    if (opId != null) {
      atsEntity.addPrimaryFilter(PrimaryFilterTypes.operationid.name(), opId);
    }

    TimelineEvent startEvt = new TimelineEvent();
    startEvt.setEventType(EventTypes.QUERY_SUBMITTED.name());
    startEvt.setTimestamp(startTime);
    atsEntity.addEvent(startEvt);

    atsEntity.addOtherInfo(OtherInfoTypes.QUERY.name(), queryObj.toString());
    atsEntity.addOtherInfo(OtherInfoTypes.TEZ.name(), numTezJobs > 0);
    atsEntity.addOtherInfo(OtherInfoTypes.MAPRED.name(), numMrJobs > 0);
    return atsEntity;
  }

  TimelineEntity createPostHookEvent(String queryId, long stopTime, String user, boolean success,
      String opId) {
    LOG.info("Received post-hook notification for :" + queryId);

    TimelineEntity atsEntity = new TimelineEntity();
    atsEntity.setEntityId(queryId);
    atsEntity.setEntityType(EntityTypes.HIVE_QUERY_ID.name());
    atsEntity.addPrimaryFilter(PrimaryFilterTypes.user.name(), user);
    if (opId != null) {
      atsEntity.addPrimaryFilter(PrimaryFilterTypes.operationid.name(), opId);
    }

    TimelineEvent stopEvt = new TimelineEvent();
    stopEvt.setEventType(EventTypes.QUERY_COMPLETED.name());
    stopEvt.setTimestamp(stopTime);
    atsEntity.addEvent(stopEvt);

    atsEntity.addOtherInfo(OtherInfoTypes.STATUS.name(), success);

    return atsEntity;
  }

  // synchronous call
  synchronized void fireAndForget(Configuration conf, TimelineEntity entity) throws Exception {
    timelineClient.putEntities(entity);
  }

  private class ATSWork implements Runnable {

    private final HookContext hookContext;
    private final long currentTime;

    public ATSWork(HookContext hookContext, long currentTime) {
      this.hookContext = hookContext;
      this.currentTime = currentTime;
    }

    @Override
    public void run() {
      long delay = System.currentTimeMillis() - currentTime;
      if (delay > SUBMIT_DELAY_LOG_THRESHOLD * 1000) {
        LOG.info("Submission " + this + " delayed " + delay + " msec");
      }
      try {
        QueryPlan plan = hookContext.getQueryPlan();
        if (plan == null) {
          return;
        }
        String queryId = plan.getQueryId();
        String opId = hookContext.getOperationId();
        long queryStartTime = plan.getQueryStartTime();
        String user = hookContext.getUgi().getUserName();
        int numMrJobs = Utilities.getMRTasks(plan.getRootTasks()).size();
        int numTezJobs = Utilities.getTezTasks(plan.getRootTasks()).size();

        if (numMrJobs + numTezJobs <= 0) {
          return; // ignore client only queries
        }

        HiveConf conf = new HiveConf(hookContext.getConf());
        switch (hookContext.getHookType()) {
          case PRE_EXEC_HOOK:
            ExplainTask explain = new ExplainTask();
            explain.initialize(conf, plan, null);
            String query = plan.getQueryStr();
            List<Task<?>> rootTasks = plan.getRootTasks();
            JSONObject explainPlan = explain.getJSONPlan(null, null, rootTasks,
                plan.getFetchTask(), true, false, false);
            fireAndForget(conf, createPreHookEvent(queryId, query,
                explainPlan, queryStartTime, user, numMrJobs, numTezJobs, opId));
            break;
          case POST_EXEC_HOOK:
            fireAndForget(conf, createPostHookEvent(queryId, currentTime, user, true, opId));
            break;
          case ON_FAILURE_HOOK:
            fireAndForget(conf, createPostHookEvent(queryId, currentTime, user, false, opId));
            break;
          default:
            //ignore
            break;
        }
      } catch (Exception e) {
        LOG.info("Failed to submit plan to ATS: " + StringUtils.stringifyException(e));
      }
    }

    @Override
    public String toString() {
      return hookContext.getHookType() + ":" + hookContext.getQueryPlan().getQueryId();
    }
  }
}
