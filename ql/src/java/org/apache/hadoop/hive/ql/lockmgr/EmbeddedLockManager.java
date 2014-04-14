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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockObject.HiveLockObjectData;
import org.apache.hadoop.hive.ql.metadata.DummyPartition;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * shared lock manager for dedicated hive server. all locks are managed in memory
 */
public class EmbeddedLockManager extends SharedLockManager {

  private static final Log LOG = LogFactory.getLog("EmbeddedHiveLockManager");
  private static final SessionState.LogHelper console = new SessionState.LogHelper(LOG);

  private final Node root = new Node();

  public EmbeddedLockManager() {
  }

  public List<HiveLock> getLocks(boolean verifyTablePartitions, boolean fetchData)
      throws LockException {
    return getLocks(verifyTablePartitions, fetchData, getConf());
  }

  public List<HiveLock> getLocks(HiveLockObject key, boolean verifyTablePartitions,
      boolean fetchData) throws LockException {
    return getLocks(key, verifyTablePartitions, fetchData, getConf());
  }

  public HiveLock lock(HiveLockObject key, HiveLockMode mode, boolean keepAlive)
      throws LockException {
    for (int i = 0; i <= numRetriesForLock(); i++) {
      if (i > 0) {
        sleep(sleepTime());
      }
      HiveLock lock = root.lock(key, mode, lockTimeout());
      if (lock != null) {
        return lock;
      }
      console.printError("conflicting lock present for " + key.getDisplayName() + " mode " + mode);
    }
    return null;
  }

  private void sleep(int sleepTime) {
    try {
      Thread.sleep(sleepTime);
    } catch (InterruptedException e) {
      // ignore
    }
  }

  public List<HiveLock> lock(List<HiveLockObj> objs, boolean keepAlive)
      throws LockException {
    sortLocks(objs);
    for (int i = 0; i <= numRetriesForLock(); i++) {
      if (i > 0) {
        sleep(sleepTime());
      }
      List<HiveLock> locks = lockPrimitive(objs);
      if (locks != null) {
        return locks;
      }
    }
    return null;
  }

  private List<HiveLock> lockPrimitive(List<HiveLockObj> objs) throws LockException {
    List<HiveLock> locks = new ArrayList<HiveLock>();
    for (HiveLockObj obj : objs) {
      HiveLock lock = root.lock(obj.getObj(), obj.getMode(), lockTimeout());
      if (lock == null) {
        console.printError("conflicting lock present for " +
            obj.getObj().getDisplayName() + " mode " + obj.getMode());
        releaseLocks(locks, false);
        return null;
      }
      locks.add(lock);
    }
    return locks;
  }

  private void sortLocks(List<HiveLockObj> objs) {
    Collections.sort(objs, new Comparator<HiveLockObj>() {
      public int compare(HiveLockObj o1, HiveLockObj o2) {
        int cmp = o1.getName().compareTo(o2.getName());
        if (cmp == 0) {
          if (o1.getMode() == o2.getMode()) {
            return cmp;
          }
          // EXCLUSIVE locks occur before SHARED locks
          if (o1.getMode() == HiveLockMode.EXCLUSIVE) {
            return -1;
          }
          return +1;
        }
        return cmp;
      }
    });
  }

  public void unlock(HiveLock hiveLock, boolean recursive) throws LockException {
    String[] paths = hiveLock.getHiveLockObject().getPaths();
    HiveLockObjectData data = hiveLock.getHiveLockObject().getData();
    for (int i = 0; i <= numRetriesForUnLock(); i++) {
      if (i > 0) {
        sleep(sleepTime());
      }
      if (root.unlock(paths, data, recursive)) {
        return;
      }
    }
    throw new LockException("Failed to release lock " + hiveLock);
  }

  public void releaseLocks(List<HiveLock> hiveLocks, boolean recursive) {
    for (HiveLock locked : hiveLocks) {
      try {
        unlock(locked, recursive);
      } catch (LockException e) {
        LOG.info(e);
      }
    }
  }

  public List<HiveLock> getLocks(boolean verifyTablePartitions, boolean fetchData, HiveConf conf)
      throws LockException {
    return root.getLocks(verifyTablePartitions, fetchData, conf);
  }

  public List<HiveLock> getLocks(HiveLockObject key, boolean verifyTablePartitions,
      boolean fetchData, HiveConf conf) throws LockException {
    return root.getLocks(key.getPaths(), verifyTablePartitions, fetchData, conf);
  }

  // from ZooKeeperHiveLockManager
  private HiveLockObject verify(boolean verify, String[] names, HiveLockObjectData data,
      HiveConf conf) throws LockException {
    if (!verify) {
      return new HiveLockObject(names, data);
    }
    String database = names[0];
    String table = names[1];

    try {
      Hive db = Hive.get(conf);
      Table tab = db.getTable(database, table, false);
      if (tab == null) {
        return null;
      }
      if (names.length == 2 || !tab.isPartitioned()) {
        return new HiveLockObject(tab, data);
      }
      Map<String, String> partSpec = new HashMap<String, String>();
      for (String partial : names[2].split("/")) {
        String[] partVals = partial.split("=");
        partSpec.put(partVals[0], partVals[1]);
      }
      if (tab.getPartitionKeys().size() != partSpec.size()) {
        return new HiveLockObject(new DummyPartition(tab, null, partSpec), data);
      }
      Partition partn = db.getPartition(tab, partSpec, false);
      return new HiveLockObject(partn, data);
    } catch (Exception e) {
      throw new LockException(e);
    }
  }

  public void close() {
    root.lock.lock();
    try {
      root.datas = null;
      root.children = null;
    } finally {
      root.lock.unlock();
    }
  }

  private class Node {

    private HiveLockMode lockMode;
    private Map<String, Node> children;
    private Map<String, HiveLockObjectData> datas;
    private final ReentrantLock lock = new ReentrantLock();

    public Node() {
    }

    private SimpleHiveLock addLock(HiveLockObject lock, HiveLockMode lockMode) {
      this.lockMode = lockMode;
      if (datas == null) {
        datas = new LinkedHashMap<String, HiveLockObjectData>(3);
      }
      datas.put(lock.getQueryId(), lock.getData());
      return new SimpleHiveLock(lock, lockMode);
    }

    public HiveLock lock(HiveLockObject key, HiveLockMode mode, long timeout)
        throws LockException {
      try {
        return timedLock(key, 0, mode, timeout);
      } catch (InterruptedException e) {
        throw new LockException(e);
      }
    }

    public boolean unlock(String[] paths, HiveLockObjectData data, boolean recursive) {
      return unlock(paths, 0, data, recursive);
    }

    private List<HiveLock> getLocks(boolean verify, boolean fetchData, HiveConf conf)
        throws LockException {
      if (!root.hasChild()) {
        return Collections.emptyList();
      }
      List<HiveLock> locks = new ArrayList<HiveLock>();
      getLocks(new Stack<String>(), verify, fetchData, locks, conf);
      return locks;
    }

    private List<HiveLock> getLocks(String[] paths, boolean verify, boolean fetchData,
        HiveConf conf) throws LockException {
      if (!root.hasChild()) {
        return Collections.emptyList();
      }
      List<HiveLock> locks = new ArrayList<HiveLock>();
      getLocks(paths, 0, verify, fetchData, locks, conf);
      return locks;
    }

    private HiveLock timedLock(HiveLockObject key, int index, HiveLockMode mode, long remain)
        throws InterruptedException {
      if (remain <= 0) {
        if (!lock.tryLock()) {
          return null;
        }
      } else {
        long start = System.currentTimeMillis();
        if (!lock.tryLock(remain, TimeUnit.MILLISECONDS)) {
          return null;
        }
        remain -= System.currentTimeMillis() - start;
        if (remain <= 0 && index < key.pathNames.length) {
          return null;
        }
      }
      try {
        return lock(key, index, mode, remain);
      } finally {
        lock.unlock();
      }
    }

    private HiveLock lock(HiveLockObject key, int index, HiveLockMode mode, long remain)
        throws InterruptedException {
      if (index == key.pathNames.length) {
        if (mode == HiveLockMode.EXCLUSIVE && hasChild()) {
          return null;  // lock on child
        }
        if (!hasLock()) {
          return addLock(key, mode);
        }
        if (lockMode != mode) {
          if (isSoleOwner(key.getQueryId())) {
            // update lock data & mode
            return addLock(key, HiveLockMode.EXCLUSIVE);
          }
          // other query has lock on this, fail
          return null;
        }
        if (mode == HiveLockMode.SHARED || isSoleOwner(key.getQueryId())) {
          // update lock data
          return addLock(key, mode);
        }
        return null;
      }
      Node child;
      if (children == null) {
        children = new HashMap<String, Node>(3);
        children.put(key.pathNames[index], child = new Node());
      } else {
        child = children.get(key.pathNames[index]);
        if (child == null) {
          children.put(key.pathNames[index], child = new Node());
        }
      }
      return child.timedLock(key, index + 1, mode, remain);
    }

    private boolean unlock(String[] paths, int index, HiveLockObjectData data, boolean recursive) {
      if (!lock.tryLock()) {
        return false;
      }
      try {
        if (index == paths.length) {
          if (hasLock()) {
            if (data == null) {
              datas.clear();
            } else {
              datas.remove(data.getQueryId());
            }
          }
          if (lockMode == HiveLockMode.EXCLUSIVE && recursive) {
            children = null;
          }
          lockMode = null;
          return true;
        }
        Node child = hasChild() ? children.get(paths[index]) : null;
        if (child == null) {
          return true; // can be happened
        }
        if (child.unlock(paths, index + 1, data, recursive)) {
          if (!child.hasLock() && !child.hasChild()) {
            children.remove(paths[index]);
          }
          return true;
        }
        return false;
      } finally {
        lock.unlock();
      }
    }

    private void getLocks(Stack<String> names, boolean verify,
        boolean fetchData, List<HiveLock> locks, HiveConf conf) throws LockException {
      lock.lock();
      try {
        if (hasLock()) {
          getLocks(names.toArray(new String[names.size()]), verify, fetchData, locks, conf);
        }
        if (children != null) {
          for (Map.Entry<String, Node> entry : children.entrySet()) {
            names.push(entry.getKey());
            entry.getValue().getLocks(names, verify, fetchData, locks, conf);
            names.pop();
          }
        }
      } finally {
        lock.unlock();
      }
    }

    private void getLocks(String[] paths, int index, boolean verify,
        boolean fetchData, List<HiveLock> locks, HiveConf conf) throws LockException {
      lock.lock();
      try {
        if (index == paths.length) {
          getLocks(paths, verify, fetchData, locks, conf);
          return;
        }
        Node child = hasChild() ? children.get(paths[index]) : null;
        if (child != null) {
          child.getLocks(paths, index + 1, verify, fetchData, locks, conf);
        }
      } finally {
        lock.unlock();
      }
    }

    private void getLocks(String[] paths, boolean verify, boolean fetchData, List<HiveLock> locks,
        HiveConf conf) throws LockException {
      for (HiveLockObjectData data : datas.values()) {
        HiveLockObject lock = verify(verify, paths, fetchData ? data : null, conf);
        if (lock != null) {
          locks.add(new SimpleHiveLock(lock, lockMode));
        }
      }
    }

    private boolean hasLock() {
      return datas != null && !datas.isEmpty();
    }

    private boolean isSoleOwner(String queryId) {
      return hasLock() && datas.size() == 1 && datas.containsKey(queryId);
    }

    private boolean hasChild() {
      return children != null && !children.isEmpty();
    }
  }

  private static class SimpleHiveLock extends HiveLock {

    private final HiveLockObject lockObj;
    private final HiveLockMode lockMode;

    public SimpleHiveLock(HiveLockObject lockObj, HiveLockMode lockMode) {
      this.lockObj = lockObj;
      this.lockMode = lockMode;
    }

    @Override
    public HiveLockObject getHiveLockObject() {
      return lockObj;
    }

    @Override
    public HiveLockMode getHiveLockMode() {
      return lockMode;
    }

    @Override
    public String toString() {
      return lockMode + "=" + lockObj.getDisplayName() + "(" + lockObj.getData() + ")";
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof SimpleHiveLock)) {
        return false;
      }

      SimpleHiveLock simpleLock = (SimpleHiveLock) o;
      return lockMode == simpleLock.lockMode &&
        lockObj.equals(simpleLock.lockObj);
    }
  }
}
