package org.apache.hadoop.hive.contrib.forrest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ForrestEvaluator implements Writable {

  private transient ObjectInspector[] inputOIs;
  private EvaluationNode[] evaluator;
  private float[][] ranges;

  public ForrestEvaluator(float[][] ranges) {
    this.evaluator = new EvaluationNode[0];
    this.ranges = ranges;
  }

  public ForrestEvaluator(ObjectInspector[] inputOIs) {
    this.inputOIs = inputOIs;
  }

  public boolean initialized() {
    return evaluator != null;
  }

  public void initialize(String address) throws HiveException {
    try {
      Path path = new Path(address);
      FileSystem fs = path.getFileSystem(new Configuration());
      FSDataInputStream input = fs.open(path);
      readFields(input);
      input.close();
    } catch (IOException e) {
      throw new HiveException(e);
    }
  }

  public int evaluate(GenericUDF.DeferredObject... vector) throws HiveException {
    int index = 1;
    while (index <= evaluator.length && evaluator[index - 1] != null) {
      index = (index << 1) + (evaluator[index - 1].evaluate(vector) ? 0 : 1);
    }
    return index;
  }

  public int[] location(GenericUDF.DeferredObject... arguments) throws HiveException {
    int[] result = new int[inputOIs.length - 2];
    for (int i = 0; i < result.length; i++) {
      float scalar = ((FloatObjectInspector) inputOIs[i + 1]).get(arguments[i + 1].get());
      for (int j = 0; j < ranges[i].length; j++) {
        if (scalar < ranges[i][j]) {
          result[i] = j;
          break;
        }
      }
    }
    return result;
  }

  public int evaluate(float... vector) {
    int index = 0;
    while (index < evaluator.length && evaluator[index] != null) {
      index = (index << 1) + (evaluator[index].evaluate(vector) ? 0 : 1);
    }
    return index;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(ranges.length);
    for (float[] range : ranges) {
      out.writeInt(range.length);
      for (float value : range) {
        out.writeFloat(value);
      }
    }
    out.writeInt(evaluator.length);
    for (EvaluationNode node : evaluator) {
      if (node == null) {
        out.writeInt(-1);
      } else {
        out.writeInt(node.axis);
        out.writeInt(node.index);
        out.writeInt(node.decision);
      }
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    ranges = new float[in.readInt()][];
    for (int i = 0; i < ranges.length; i++) {
      ranges[i] = new float[in.readInt()];
      for (int j = 0; j < ranges[i].length; j++) {
        ranges[i][j] = in.readFloat();
      }
    }
    evaluator = new EvaluationNode[in.readInt()];
    for (int i = 0; i < evaluator.length; i++) {
      int index = in.readInt();
      if (index >= 0) {
        evaluator[i] = new EvaluationNode(index, in.readInt(), in.readInt());
      }
    }
  }

  public void addRule(int group, int axis, int index, int decision) {
    if (evaluator.length < group) {
      EvaluationNode[] spanning = new EvaluationNode[group];
      System.arraycopy(evaluator, 0, spanning, 0, evaluator.length);
      evaluator = spanning;
    }
    evaluator[group - 1] = new EvaluationNode(axis, index, decision);
  }

  public boolean onBoundary(int groupID, int axisID, int axisIndex) {
    for (groupID >>= 1; groupID > 0; groupID >>= 1) {
      if (evaluator[groupID - 1] == null || evaluator[groupID - 1].axis != axisID) {
        continue;
      }
      if (evaluator[groupID - 1].index == axisIndex) {
        return true;
      }
    }
    return axisIndex == 0 || axisIndex == ranges[axisID].length - 1;
  }

  private class EvaluationNode {

    private int axis;
    private int index;
    private int decision;

    public EvaluationNode(int axis, int index, int decision) {
      this.axis = axis;
      this.index = index;
      this.decision = decision;
    }

    public boolean evaluate(float... vector) {
      return vector[axis] < ranges[axis][index];
    }

    public boolean evaluate(GenericUDF.DeferredObject... vector) throws HiveException {
      return ((FloatObjectInspector) inputOIs[axis + 1]).get(vector[axis + 1].get()) <= ranges[axis][index];
    }
  }
}
