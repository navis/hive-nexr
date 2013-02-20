package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;
import java.util.List;

public class SamplingContext implements Serializable {

  private static final long serialVersionUID = 1L;

  private int samplingNum = 200;
  private List<ExprNodeDesc> samplingKeys;
  private TableDesc tableInfo;

  @Explain(displayName = "Sampling Number")
  public int getSamplingNum() {
    return samplingNum;
  }

  public void setSamplingNum(int samplingNum) {
    this.samplingNum = samplingNum;
  }

  @Explain(displayName = "Sampling Keys")
  public List<ExprNodeDesc> getSamplingKeys() {
    return samplingKeys;
  }

  public void setSamplingKeys(List<ExprNodeDesc> samplingKeys) {
    this.samplingKeys = samplingKeys;
  }

  public TableDesc getTableInfo() {
    return tableInfo;
  }

  public void setTableInfo(TableDesc tableInfo) {
    this.tableInfo = tableInfo;
  }
}
