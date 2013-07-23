package org.apache.hadoop.hive.rdbms;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;

public class RowWritable implements Writable {

  private final List<Object> row = new ArrayList<Object>();

  public void add(Object value) {
    row.add(value);
  }

  public void clear() {
    row.clear();
  }

  public List<Object> get() {
    return row;
  }

  public void write(DataOutput out) throws IOException {
    throw new UnsupportedOperationException("write");
  }

  public void readFields(DataInput in) throws IOException {
    throw new UnsupportedOperationException("readFields");
  }
}
