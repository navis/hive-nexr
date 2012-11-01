package org.apache.hadoop.hive.ql.exec;

import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ListSinkDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.SerDeException;

public abstract class AbstractListSinkOperator<T> extends Operator<ListSinkDesc> {

  private transient ArrayList<T> res;
  private transient int numRows;

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    try {
      initializeChildren(hconf);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public void reset(ArrayList<T> res) {
    this.res = res;
    this.numRows = 0;
  }

  public int getNumRows() {
    return numRows;
  }

  public void processOp(Object row, int tag) throws HiveException {
    try {
      res.add(convert(row));
      numRows++;
    } catch (SerDeException e) {
      throw new HiveException(e);
    }
  }

  public OperatorType getType() {
    return OperatorType.FORWARD;
  }

  protected abstract T convert(Object row) throws SerDeException;
}
