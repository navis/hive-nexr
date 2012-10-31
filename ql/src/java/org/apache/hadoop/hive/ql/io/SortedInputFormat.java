package org.apache.hadoop.hive.ql.io;

import java.io.IOException;

import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class SortedInputFormat implements InputFormat {

  private InputFormat delegated;

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    return delegated.getSplits(job, numSplits);
  }

  @Override
  public RecordReader getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }
}