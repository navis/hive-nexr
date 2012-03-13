package org.apache.hadoop.hive.ql.exec;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.mapred.JobConf;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class SearchOperator implements Serializable {

  private FetchWork work;

  private transient JobConf job;

  public SearchOperator() {
  }

  public SearchOperator(FetchWork work, JobConf job) {
    this.work = work;
    this.job = job;
  }

  public ObjectInspector getOutputObjectInspector() throws HiveException {
    try {
      TableDesc tbl = getTableDesc(work);
      if (tbl == null) {
        return null;
      }
      Deserializer serde = tbl.getDeserializerClass().newInstance();
      serde.initialize(job, tbl.getProperties());

      return serde.getObjectInspector();

    } catch (Exception e) {
      throw new HiveException("Failed with exception " + e.getMessage()
          + org.apache.hadoop.util.StringUtils.stringifyException(e));
    }
  }

  private TableDesc getTableDesc(FetchWork work) {
    if (work.getTblDir() != null) {
        return work.getTblDesc();
      }
    List<PartitionDesc> listParts = work.getPartDesc();
    if (listParts.size() == 0) {
      return null;
    }
    return listParts.get(0).getTableDesc();
  }

  public Iterable<Object> search(Byte tag, ArrayList<Object> keyWritable) {
    return null;
  }
}
