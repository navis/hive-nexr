package org.apache.hadoop.hive.ql.lockmgr;

/**
 * common super class for shared lock manager
 */
public abstract class SharedLockManager extends AbstractLockManager {

  protected static final ThreadLocal<HiveLockManagerCtx> CTX
      = new InheritableThreadLocal<HiveLockManagerCtx>();

  @Override
  public void setContext(HiveLockManagerCtx ctx) throws LockException {
    CTX.set(ctx);
  }

  @Override
  public HiveLockManagerCtx getContext() {
    return CTX.get();
  }
}
