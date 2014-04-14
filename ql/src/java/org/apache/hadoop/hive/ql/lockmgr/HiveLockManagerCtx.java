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

package org.apache.hadoop.hive.ql.lockmgr;

import org.apache.hadoop.hive.conf.HiveConf;

public class HiveLockManagerCtx {

  private final HiveConf conf;

  private int sleepTime;
  private int numRetriesForLock;
  private int numRetriesForUnLock;

  private int lockTimeout;
  private int unlockTimeout;

  public HiveLockManagerCtx(HiveConf conf) {
    this.conf = conf;
    refresh();
  }

  public HiveConf getConf() {
    return conf;
  }

  public void refresh() {
    sleepTime = conf.getIntVar(HiveConf.ConfVars.HIVE_LOCK_SLEEP_BETWEEN_RETRIES) * 1000;
    numRetriesForLock = conf.getIntVar(HiveConf.ConfVars.HIVE_LOCK_NUMRETRIES);
    numRetriesForUnLock = conf.getIntVar(HiveConf.ConfVars.HIVE_UNLOCK_NUMRETRIES);
    lockTimeout = conf.getIntVar(HiveConf.ConfVars.HIVE_LOCK_TIMEOUT_MSEC);
    unlockTimeout = conf.getIntVar(HiveConf.ConfVars.HIVE_UNLOCK_TIMEOUT_MSEC);
  }

  public int lockTimeout() {
    return lockTimeout;
  }

  public int unlockTimeout() {
    return unlockTimeout;
  }

  public int numRetriesForLock() {
    return numRetriesForLock;
  }

  public int numRetriesForUnLock() {
    return numRetriesForUnLock;
  }

  public int sleepTime() {
    return sleepTime;
  }
}
