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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.util.ReflectionUtils;

import java.util.List;

public class LockManagers {

  private static final Log LOG = LogFactory.getLog(LockManagers.class.getName());

  private static boolean initialized;
  private static Class<? extends HiveLockManager> clazz;
  private static SharedLockManager sharedLockMgr;

  private static synchronized boolean initialize(HiveLockManagerCtx context) throws HiveException {
    if (!initialized) {
      HiveConf conf = context.getConf();
      if (conf.getBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY)) {
        clazz = getLockManagerClass(context.getConf());
        if (SharedLockManager.class.isAssignableFrom(clazz)) {
          sharedLockMgr = (SharedLockManager) createLockManager(clazz, context);
          Runtime.getRuntime().addShutdownHook(new LockShutdownHook());
        }
      }
      initialized = true;
    }
    return clazz != null;
  }

  public static HiveLockManager getLockManager(final HiveLockManagerCtx context) throws HiveException {
    if (!initialize(context)) {
      return null;
    }
    if (sharedLockMgr == null) {
      return createLockManager(clazz, context);
    }
    // return proxy for shared manager, overriding close method
    final HiveLockManager shared = sharedLockMgr;
    HiveLockManager manager = new SharedLockManager() {
      @Override
      public HiveLock lock(HiveLockObject key, HiveLockMode mode, boolean keepAlive)
          throws LockException {
        return shared.lock(key, mode, keepAlive);
      }
      @Override
      public List<HiveLock> lock(List<HiveLockObj> objs, boolean keepAlive) throws LockException {
        return shared.lock(objs, keepAlive);
      }
      @Override
      public void unlock(HiveLock hiveLock, boolean recursive) throws LockException {
        shared.unlock(hiveLock, recursive);
      }
      @Override
      public void releaseLocks(List<HiveLock> hiveLocks, boolean recursive) {
        shared.releaseLocks(hiveLocks, recursive);
      }
      @Override
      public List<HiveLock> getLocks(boolean verifyTablePartitions, boolean fetchData)
          throws LockException {
        return shared.getLocks(verifyTablePartitions, fetchData);
      }
      @Override
      public List<HiveLock> getLocks(HiveLockObject key, boolean verifyTablePartitions,
          boolean fetchData) throws LockException {
        return shared.getLocks(key, verifyTablePartitions, fetchData);
      }
      @Override
      public void close() {
        CTX.remove();
      }
    };
    manager.setContext(context);
    return manager;
  }

  private static class LockShutdownHook extends Thread {
    @Override
    public void run() {
      shutdown();
    }
  }

  public static synchronized void shutdown() {
    if (sharedLockMgr != null) {
      try {
        sharedLockMgr.close();
      } catch (Exception e) {
        LOG.error("Failed to close shared lock manager " + sharedLockMgr, e);
      }
      sharedLockMgr = null;
    }
    clazz = null;
    initialized = false;
  }

  private static HiveLockManager createLockManager(
      Class<? extends HiveLockManager> clazz, HiveLockManagerCtx context) throws HiveException {
    HiveLockManager hiveLockMgr = null;
    try {
      hiveLockMgr = ReflectionUtils.newInstance(clazz, context.getConf());
      hiveLockMgr.setContext(context);
    } catch (Exception e) {
      // set hiveLockMgr to null just in case this invalid manager got set to
      // next query's ctx.
      if (hiveLockMgr != null) {
        try {
          hiveLockMgr.close();
        } catch (Exception e1) {
          //nothing can do here
        }
      }
      LOG.warn("Failed to create lock manager " + clazz, e);
      throw new HiveException(ErrorMsg.LOCKMGR_NOT_INITIALIZED.getMsg(), e);
    }
    return hiveLockMgr;
  }

  private static Class<? extends HiveLockManager> getLockManagerClass(HiveConf conf)
      throws HiveException {
    String lockMgr = conf.getVar(HiveConf.ConfVars.HIVE_LOCK_MANAGER);
    if (lockMgr == null || lockMgr.isEmpty()) {
      throw new HiveException(ErrorMsg.LOCKMGR_NOT_SPECIFIED.getMsg());
    }
    try {
      return (Class<? extends HiveLockManager>) conf.getClassByName(lockMgr);
    } catch (Exception e) {
      LOG.warn("Failed to find lock manager class " + lockMgr, e);
      throw new HiveException(e);
    }
  }
}
