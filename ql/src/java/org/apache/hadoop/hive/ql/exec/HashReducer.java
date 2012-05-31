package org.apache.hadoop.hive.ql.exec;

import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

public class HashReducer extends AbstractExecReducer implements OutputCollector<HiveKey, Writable> {

  private final TableDesc keyDesc;
  private final List<TableDesc> valueDescs;
  private final TreeMap<HiveKey, List<BytesWritable>> tree;

  private final AtomicInteger remaining;

  @SuppressWarnings("unchecked")
  public HashReducer(Operator<?> reducer, ReduceSinkOperator rs) {
    this.reducer = reducer;
    this.isTagged = reducer instanceof CommonJoinOperator;
    this.keyDesc = rs.getConf().getKeySerializeInfo();
    this.valueDescs = new ArrayList<TableDesc>();
    this.remaining = new AtomicInteger();
    this.tree = new TreeMap<HiveKey, List<BytesWritable>>(new HiveKey.Comparator());
    this.oc = this;
  }

  public void setValueDesc(ReduceSinkOperator rs) {
    int tag = Math.max(0, rs.getConf().getTag());
    while (tag + 1 > valueDescs.size()) {
      valueDescs.add(null);
    }
    valueDescs.set(tag, rs.getConf().getValueSerializeInfo());
    remaining.set(valueDescs.size());
  }

  public boolean isInitialized() {
    return memoryMXBean != null;
  }

  protected void configureJob(JobConf job) {
    initKeyValueDesc(keyDesc, valueDescs);
  }

  public void collect(HiveKey key, Writable value) throws IOException {
    List<BytesWritable> list = tree.get(key);
    if (list == null) {
      HiveKey akey = new HiveKey();
      akey.set(key.getBytes(), 0, key.getLength());
      tree.put(akey, list = new ArrayList<BytesWritable>());
    }
    if (value instanceof BytesWritable) {
      BytesWritable bytes = (BytesWritable) value;
      list.add(new BytesWritable(Arrays.copyOf(bytes.getBytes(), bytes.getLength())));
    } else {
      Text text = (Text) value;
      list.add(new BytesWritable(Arrays.copyOf(text.getBytes(), text.getLength())));
    }
  }

  // op closed. return true if all reducers have finished
  public boolean flush() throws HiveException {
    int counter = remaining.decrementAndGet();
    if (counter == 0) {
      startReducing();
    }
    return counter < 0;
  }

  public boolean isFinished() {
    return remaining.get() < 0;
  }

  private void startReducing() throws HiveException {
    boolean success = true;
    try {
      for (Map.Entry<HiveKey, List<BytesWritable>> entry : tree.entrySet()) {
        reduce(entry.getKey(), entry.getValue().iterator(), this, null);
      }
      tree.clear();
    } catch (Exception e) {
      abort = true;
      throw new HiveException(e);
    } finally {
      close();
      JobCloseFeedBack feedBack = new JobCloseFeedBack();
      reducer.jobClose(jc, success, feedBack);
    }
  }
}
