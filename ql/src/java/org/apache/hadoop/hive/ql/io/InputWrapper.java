package org.apache.hadoop.hive.ql.io;

import org.apache.hadoop.mapred.RecordReader;

public interface InputWrapper<K, V> extends RecordReader<K, V> {

  void setRecordReader(RecordReader<K, V> source);
}
