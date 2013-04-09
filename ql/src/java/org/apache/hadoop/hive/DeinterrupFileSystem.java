package org.apache.hadoop.hive;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.util.Progressable;

public class DeinterrupFileSystem extends FilterFileSystem {

  public void initialize(URI name, Configuration conf) throws IOException {
    fs = new DistributedFileSystem();
    fs.initialize(name, conf);
    statistics = getStatistics(name.getScheme(), getClass());
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    clearInterrupt();
    return super.open(f, bufferSize);
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
    clearInterrupt();
    return super.create(f, permission, overwrite, bufferSize, replication, blockSize, progress);
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
    clearInterrupt();
    return super.append(f, bufferSize, progress);
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    clearInterrupt();
    return super.rename(src, dst);
  }

  @Override
  public boolean delete(Path f) throws IOException {
    clearInterrupt();
    return super.delete(f);
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    clearInterrupt();
    return super.delete(f, recursive);
  }

  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    clearInterrupt();
    return super.listStatus(f);
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    clearInterrupt();
    return super.mkdirs(f, permission);
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    clearInterrupt();
    return super.getFileStatus(f);
  }

  @Override
  public void setOwner(Path p, String username, String groupname) throws IOException {
    clearInterrupt();
    super.setOwner(p, username, groupname);
  }

  @Override
  public void setPermission(Path p, FsPermission permission) throws IOException {
    clearInterrupt();
    super.setPermission(p, permission);
  }

  private void clearInterrupt() {
    if (Thread.interrupted()) {
      StackTraceElement[] trace = new Exception().getStackTrace();
      LOG.warn("Worker thread " + Thread.currentThread() + " is in interrupted state, doing " +
          trace[1].getMethodName() + " in " + trace[2].toString() +
          ". interrupt flag is cleaned-up");
    }
  }
}
