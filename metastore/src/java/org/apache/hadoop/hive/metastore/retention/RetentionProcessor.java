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

package org.apache.hadoop.hive.metastore.retention;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreThread;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.RawStoreProxy;
import org.apache.hadoop.hive.metastore.api.MetaException;

import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTORE_RETENTION_DATABASES;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTORE_RETENTION_INTERVAL;

public class RetentionProcessor extends Thread implements MetaStoreThread {

  private static final Log LOG = LogFactory.getLog(RetentionProcessor.class);

  private HiveConf conf;
  private int threadId;
  private AtomicBoolean stop;
  private RawStore rs;

  private long interval;
  private String[] databases;

  @Override
  public void setHiveConf(HiveConf conf) {
    this.conf = conf;
  }

  @Override
  public void setThreadId(int threadId) {
    this.threadId = threadId;
  }

  @Override
  public void init(AtomicBoolean stop, AtomicBoolean loop) throws MetaException {
    this.stop = stop;
    this.rs = RawStoreProxy.getProxy(conf, conf,
        conf.getVar(HiveConf.ConfVars.METASTORE_RAW_STORE_IMPL), threadId);
    interval = HiveConf.getTimeVar(conf, METASTORE_RETENTION_INTERVAL, TimeUnit.MILLISECONDS);
    String databaseNames = HiveConf.getVar(conf, METASTORE_RETENTION_DATABASES).trim();
    databases = databaseNames.isEmpty() ? null : databaseNames.split(",");
    setName("Retention [" + TimeUnit.SECONDS.convert(interval, TimeUnit.MILLISECONDS) + "]");
    setPriority(MIN_PRIORITY);
    setDaemon(true);
  }

  @Override
  public void run() {
    while (!stop.get()) {
      try {
        Thread.sleep(interval);
      } catch (InterruptedException e) {
        // ignore
      }
      try {
        checkTTL(databases);
      } catch (MetaException e) {
        LOG.warn("Failed to access metastore", e);
      }
    }
  }

  private void checkTTL(String[] databases) throws MetaException {
    for (RetentionTarget target: rs.getRetentionTargets(databases)) {
      String name = target.getName();
      String retention = target.retention + " seconds";
      if (target.retention > 60 ) {
        if (target.retention > 60 * 60) {
          if (target.retention > 60 * 60 * 24) {
            retention += "(about " + target.retention / 60 / 60 / 24 + "+ days)";
          } else {
            retention += "(about " + target.retention / 60 / 60 + "+ hours)";
          }
        } else {
          retention += "(about " + target.retention / 60 + "+ minutes)";
        }
      }
      LOG.warn("Dropping " + name + " by retention policy (Created: " +
          new Date(target.createTime * 1000l) + ", Retention on: " + retention);
      try {
        if (target.partValues == null) {
          rs.dropTable(target.databaseName, target.tableName);
        } else {
          rs.dropPartition(target.databaseName, target.tableName, target.partValues);
        }
      } catch (Exception e) {
        LOG.warn("Failed to drop " + name + " (retention)", e);
      }
    }
  }
}