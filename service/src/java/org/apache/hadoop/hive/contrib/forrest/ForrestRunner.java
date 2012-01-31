package org.apache.hadoop.hive.contrib.forrest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.service.ThriftHive;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;

import java.util.ArrayList;
import java.util.List;

public class ForrestRunner {

  private static final int DEFAULT_PORT = 10000;

  public ForrestRunner(String[] args) throws Exception {
    Path workingDirectory = new Path("forrest");
    FileSystem fs = FileSystem.get(new Configuration());
    fs.delete(workingDirectory, true);
    fs.mkdirs(workingDirectory);

    ThriftHive.Client client = clientFor(args[0]);
    client.execute("add jar lib/hive-service-0.9.0-SNAPSHOT.jar");
    client.execute("create temporary function forrest_group as 'org.apache.hadoop.hive.contrib.forrest.ForrestGroupUDF'");

    float[][] range = new float[][]{
        {5, 25, 45, 65, 85, 105},
        {5, 25, 45, 65, 85, 105}};
    ForrestEvaluator evaluator = new ForrestEvaluator(range);

    int iteraton = 0;
    while (true) {
      Path target = new Path(workingDirectory, "_" + iteraton);
      String path = fs.makeQualified(target).toString();
      FSDataOutputStream out = fs.create(target);
      evaluator.write(out);
      out.close();

      System.out.println("iteration : " + iteraton);
      StringBuilder builder = new StringBuilder();
      builder.append("select atom.groupID, count(*), sum(atom.decision) from (");
      builder.append("select explode(forrest_group('").append(path).append("', ");
      for (int i = 1; i <= 2; i++) {
        builder.append("x").append(i);
        builder.append(", ");
      }
      builder.append("decision)) as atom from forrest) subq ");
      builder.append("group by atom.groupID");

      System.out.println("executing " + builder);
      client.execute(builder.toString());

      List<Atom> results = new ArrayList<Atom>();
      for (String result : client.fetchAll()) {
        results.add(new Atom(result));
      }

      int[][][][] matrix = new int[1 << iteraton][range.length][][];
      for (Atom result : results) {
        if (result.total == 0) {
          continue;
        }
        int gindex = result.groupID - (1 << iteraton);
        if (gindex < 0) {
          continue;
        }
        System.out.println(" ---- " + gindex + ":" + result.axisID + ":" + result.axisIndex + " = " + result.positive + "/" + result.total);
        if (matrix[gindex][result.axisID] == null) {
          matrix[gindex][result.axisID] = new int[range[result.axisID].length][2];
        }
        matrix[gindex][result.axisID][result.axisIndex][0] += result.total;
        matrix[gindex][result.axisID][result.axisIndex][1] += result.positive;
      }
      int added = 0;
      for (int gindex = 0; gindex < matrix.length; gindex++) {
        int axisID = -1;
        int axisIndex = -1;
        int decision = -1;
        DoubleWritable minEntropy = new DoubleWritable(1);
        for (int aindex = 0; aindex < matrix[gindex].length; aindex++) {
          if (matrix[gindex][aindex] == null) {
            continue;
          }
          int[] result = calculate(matrix[gindex][aindex], minEntropy);
          if (result != null) {
            axisIndex = result[0];
            decision = result[1];
            axisID = aindex;
          }
        }
        int groupID = gindex + (1 << iteraton);
        if (axisIndex >= 0 && !evaluator.onBoundary(groupID, axisID, axisIndex)) {
          System.out.println(" -- selected -- > " + groupID + ":" + axisID + ":" + axisIndex + " = " + minEntropy.get());
          evaluator.addRule(groupID, axisID, axisIndex, decision);
          added++;
        }
      }
      if (added == 0) {
        break;
      }
      iteraton++;
    }
  }

  private int[] calculate(int[][] results, DoubleWritable minEntropy) {
    int total = 0;
    int positive = 0;

    int[] selected = null;
    for (int index = 0; index < results.length; index++) {
      if (results[index][0] == 0) {
        continue;
      }
      total += results[index][0];
      positive += results[index][1];
      float pratio = (float)positive / (float)total;
      float nratio = (float)(total - positive) / (float)total;
      double entropy = positive == 0 || positive == total ? 0 : -(pratio * (Math.log(pratio) / Math.log(2)) + nratio * (Math.log(nratio) / Math.log(2)));
      System.out.println(" entropy : " + positive + "/" + total + " = " + entropy);
      if (entropy <= minEntropy.get()) {
        minEntropy.set(entropy);
        selected = new int[] {index, Integer.valueOf(positive).compareTo(total - positive)};
      }
    }
    return selected;
  }

  private static class Atom {
    private int groupID;
    private int axisID;
    private int axisIndex;
    private int total;
    private int positive;

    Atom(String result) {
      String[] splitted = result.trim().split("\\s+");
      String[] key = splitted[0].split(":");
      groupID = Integer.valueOf(key[0]);
      axisID = Integer.valueOf(key[1]);
      axisIndex = Integer.valueOf(key[2]);
      total = Integer.valueOf(splitted[1]);
      positive = Integer.valueOf(splitted[2]);
      System.out.println(" -- atom : " + + groupID + ":" + axisID + ":" + axisIndex + "=" + positive + "/" + total);
    }
  }

  public ThriftHive.Client clientFor(String address) throws Exception {
    ThriftHive.Client.Factory syncFactory = new ThriftHive.Client.Factory();
    int index = address.indexOf(":");
    String host = index < 0 ? address : address.substring(0, index);
    int port = index < 0 ? DEFAULT_PORT : Integer.valueOf(address.substring(index + 1));
    TSocket protocol = new TSocket(host, port);
    protocol.open();
    return syncFactory.getClient(new TBinaryProtocol(protocol));
  }

  public static void main(String[] args) throws Exception {
    ForrestRunner runner = new ForrestRunner(args);
  }
}
