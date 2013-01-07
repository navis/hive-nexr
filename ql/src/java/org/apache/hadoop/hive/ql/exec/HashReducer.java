package org.apache.hadoop.hive.ql.exec;

import org.apache.hadoop.hive.ql.exec.mr.ExecReducer;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class HashReducer extends ExecReducer implements OutputCollector<HiveKey, Writable> {

  private final TableDesc keyDesc;
  private final List<TableDesc> valueDescs;
  private final TreeMap<HiveKey, List<byte[]>> tree;

  private boolean flushed;

  @SuppressWarnings("unchecked")
  public HashReducer(Operator<?> reducer, ReduceSinkOperator rs) {
    this.reducer = reducer;
    this.isTagged = reducer instanceof CommonJoinOperator;
    this.keyDesc = rs.getConf().getKeySerializeInfo();
    this.valueDescs = new ArrayList<TableDesc>();
    this.tree = new TreeMap<HiveKey, List<byte[]>>(new HiveKey.Comparator());
    this.oc = this;
  }

  public void setValueDesc(ReduceSinkOperator rs) {
    int tag = Math.max(0, rs.getConf().getTag());
    while (tag + 1 > valueDescs.size()) {
      valueDescs.add(null);
    }
    valueDescs.set(tag, rs.getConf().getValueSerializeInfo());
  }

  public boolean isInitialized() {
    return memoryMXBean != null;
  }

  public boolean isFlushed() {
    return flushed;
  }

  @Override
  protected void configureJob(JobConf job) {
    initKeyValueDesc(keyDesc, valueDescs);
  }

  public void collect(HiveKey key, Writable value) throws IOException {
    List<byte[]> list = tree.get(key);
    if (list == null) {
      HiveKey akey = new HiveKey();
      akey.setHashCode(key.hashCode());
      akey.set(key.getBytes(), 0, key.getLength());
      tree.put(akey, list = new ArrayList<byte[]>());
    }
    if (value instanceof BytesWritable) {
      BytesWritable bytes = (BytesWritable) value;
      list.add(Arrays.copyOf(bytes.getBytes(), bytes.getLength()));
    } else {
      Text text = (Text) value;
      list.add(Arrays.copyOf(text.getBytes(), text.getLength()));
    }
  }

  public void flush() throws HiveException {
    BytesIterator iterator = new BytesIterator();
    try {
      for (Map.Entry<HiveKey, List<byte[]>> entry : tree.entrySet()) {
        reduce(entry.getKey(), iterator.reset(entry.getValue()), this, null);
        if (reducer.getDone()) {
          break;  // early exit
        }
      }
    } catch (Exception e) {
      abort = true;
      throw new HiveException(e);
    } finally {
      flushed = true;
      tree.clear();
      close();
    }
  }

  private static class BytesIterator implements Iterator<BytesWritable> {

    private static BytesWritable WRITABLE = new BytesWritable();

    private Iterator<byte[]> values;

    public Iterator<BytesWritable> reset(List<byte[]> values) {
      this.values = values.iterator();
      return this;
    }

    public boolean hasNext() {
      return values.hasNext();
    }

    public BytesWritable next() {
      byte[] value = values.next();
      WRITABLE.set(value, 0, value.length);
      return WRITABLE;
    }

    public void remove() {
      throw new UnsupportedOperationException("remove");
    }
  }
}
