package org.apache.hive.service;

import org.apache.hadoop.hive.ql.plan.api.Query;
import org.apache.hive.service.cli.thrift.TOperationHandle;

public class CompileResult {

  private TOperationHandle handle;
  private Query plan;

  public CompileResult(TOperationHandle handle, Query plan) {
    this.handle = handle;
    this.plan = plan;
  }

  public TOperationHandle getHandle() {
    return handle;
  }

  public void setHandle(TOperationHandle handle) {
    this.handle = handle;
  }

  public Query getPlan() {
    return plan;
  }

  public void setPlan(Query plan) {
    this.plan = plan;
  }
}
