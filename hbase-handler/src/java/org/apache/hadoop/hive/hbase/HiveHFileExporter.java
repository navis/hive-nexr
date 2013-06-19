/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.hbase;

import java.io.IOException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hive.conf.HiveConf;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_HADOOP_SUPPORTS_SUBDIRECTORIES;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputCommitter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.util.Progressable;

public class HiveHFileExporter extends HFileOutputFormat implements
    HiveOutputFormat<ImmutableBytesWritable, KeyValue>, HiveOutputCommitter {

  static final Log LOG = LogFactory.getLog(HiveHFileExporter.class.getName());

  private org.apache.hadoop.mapreduce.RecordWriter<ImmutableBytesWritable, KeyValue>
  getFileWriter(org.apache.hadoop.mapreduce.TaskAttemptContext tac)
      throws IOException {
    try {
      return super.getRecordWriter(tac);
    } catch (InterruptedException ex) {
      throw new IOException(ex);
    }
  }

  @Override
  public RecordWriter getHiveRecordWriter(
      final JobConf jc,
      final Path finalOutPath,
      Class<? extends Writable> valueClass,
      boolean isCompressed,
      Properties tableProperties,
      final Progressable progressable) throws IOException {

    final Job job = new Job(jc);

    setCompressOutput(job, isCompressed);
    setOutputPath(job, finalOutPath);

    // Create the HFile writer
    final org.apache.hadoop.mapreduce.TaskAttemptContext tac =
        ShimLoader.getHadoopShims().newTaskAttemptContext(
            job.getConfiguration(), progressable);

    final JobContext jctx = ShimLoader.getHadoopShims().newJobContext(job);
    jctx.getConfiguration().setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", false);

    final org.apache.hadoop.mapreduce.RecordWriter<
        ImmutableBytesWritable, KeyValue> fileWriter = getFileWriter(tac);

    return new RecordWriter() {

      @Override
      public void close(boolean abort) throws IOException {
        try {
          fileWriter.close(null);
          if (!abort) {
            FileOutputCommitter committer = new FileOutputCommitter(finalOutPath, tac);
            committer.commitTask(tac);
            committer.commitJob(jctx);
          }
        } catch (InterruptedException ex) {
          throw new IOException(ex);
        }
      }

      @Override
      public void write(Writable w) throws IOException {
        // Decompose the incoming text row into fields.
        HBaseExportSerDe.HBaseUnionWritable handover = (HBaseExportSerDe.HBaseUnionWritable) w;
        KeyValue kv = handover.toKeyValue();
        try {
          fileWriter.write(null, kv);
        } catch (InterruptedException ex) {
          throw new IOException(ex);
        }
      }
    };
  }

  private transient boolean prev;

  public void commit(HiveConf conf, Path path) throws HiveException {
    prev = HiveConf.getBoolVar(conf, HIVE_HADOOP_SUPPORTS_SUBDIRECTORIES);
    Set<Path> created = new HashSet<Path>();
    try {
      FileSystem fs = path.getFileSystem(conf);
      for (FileStatus status : fs.listStatus(path)) {
        if (!status.isDir()) {
          continue;
        }
        for (FileStatus family : fs.listStatus(status.getPath())) {
          Path source = family.getPath();
          Path target = new Path(path, source.getName());
          if (created.add(target)) {
            fs.mkdirs(target);
          }
          for (FileStatus region : fs.listStatus(source)) {
            Path regionPath = region.getPath();
            fs.rename(regionPath, new Path(target, regionPath.getName()));
          }
        }
        fs.delete(status.getPath(), true);
      }
      HiveConf.setBoolVar(conf, HIVE_HADOOP_SUPPORTS_SUBDIRECTORIES, true);
    } catch (IOException e) {
      throw new HiveException(e);
    }
  }

  @Override
  public void completed(HiveConf conf) {
    HiveConf.setBoolVar(conf, HIVE_HADOOP_SUPPORTS_SUBDIRECTORIES, prev);
  }

  @Override
  public void checkOutputSpecs(FileSystem ignored, JobConf jc) throws IOException {
    Job job = new Job(jc);
    checkOutputSpecs(ShimLoader.getHadoopShims().newJobContext(job));
  }

  @Override
  public org.apache.hadoop.mapred.RecordWriter<ImmutableBytesWritable, KeyValue> getRecordWriter(
      FileSystem ignored, JobConf job, String name, Progressable progress) throws IOException {
    throw new NotImplementedException("This will not be invoked");
  }
}
