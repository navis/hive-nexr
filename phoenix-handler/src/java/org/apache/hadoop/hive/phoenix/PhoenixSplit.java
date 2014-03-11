package org.apache.hadoop.hive.phoenix;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PhoenixSplit extends FileSplit implements InputSplit {

  private final TableSplit split;
  private String url;
  private String query;

  public PhoenixSplit() {
    super(null, 0, 0, (String[]) null);
    split = new TableSplit();
  }

  public PhoenixSplit(String url, String query, TableSplit split, Path dummyPath) {
    super(dummyPath, 0, 0, (String[]) null);
    this.split = split;
    this.url = url;
    this.query = query;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    UTF8.writeString(out, url);
    UTF8.writeString(out, query);
    split.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    url = UTF8.readString(in);
    query = UTF8.readString(in);
    split.readFields(in);
  }

  public String getURL() {
    return url;
  }

  public String getQuery() {
    return query;
  }

  public TableSplit getSplit() {
    return split;
  }

  @Override
  public long getLength() {
    return split.getLength();
  }

  @Override
  public String[] getLocations() throws IOException {
    return split.getLocations();
  }

  @Override
  public String toString() {
    return "TableSplit " + split;
  }
}
