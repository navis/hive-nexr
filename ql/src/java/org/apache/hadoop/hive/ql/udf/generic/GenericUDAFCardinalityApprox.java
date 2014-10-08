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

package org.apache.hadoop.hive.ql.udf.generic;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantDoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantFloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantIntObjectInspector;

import java.io.IOException;

@Description(name = "hll",
    value =
        "_FUNC_([float/double relativeSD, ]expr) or _FUNC_([int p, int sp, ]expr) - Returns " +
        "approximate cadinality of expression by using HyperLogLogPlus.")
public class GenericUDAFCardinalityApprox extends AbstractGenericUDAFResolver {

  @Override
  public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info) throws SemanticException {
    ObjectInspector[] inputs = info.getParameterObjectInspectors();
    if (inputs.length == 0 || inputs.length > 3) {
      throw new UDFArgumentLengthException("HLL accepts one to three arguments");
    }
    getParams(inputs);
    return new HLLEvaluator();
  }

  private static int[] getParams(ObjectInspector[] inputs) throws UDFArgumentException {
    int p = 16;
    int sp = 0;
    if (inputs.length > 1) {
      if (inputs[0] instanceof WritableConstantFloatObjectInspector) {
        float relativeSD = ((WritableConstantFloatObjectInspector)inputs[0]).getWritableConstantValue().get();
        p = (int) Math.ceil(2.0 * Math.log(1.054 / relativeSD) / Math.log(2));
      } else if (inputs[0] instanceof WritableConstantDoubleObjectInspector) {
        double relativeSD = ((WritableConstantDoubleObjectInspector)inputs[0]).getWritableConstantValue().get();
        p = (int) Math.ceil(2.0 * Math.log(1.054 / relativeSD) / Math.log(2));
      } else if (inputs[0] instanceof WritableConstantIntObjectInspector) {
        p = ((WritableConstantIntObjectInspector)inputs[0]).getWritableConstantValue().get();
      } else {
        throw new UDFArgumentTypeException(0, "first argument should be constant float/double or int");
      }
      if (inputs.length > 2) {
        if (!(inputs[1] instanceof WritableConstantIntObjectInspector)) {
          throw new UDFArgumentTypeException(1, "second argument should be a int value");
        }
        sp = ((WritableConstantIntObjectInspector)inputs[0]).getWritableConstantValue().get();
      }
    }
    if (p < 4 || (p > sp && sp != 0)) {
      throw new UDFArgumentException("p must be between 4 and sp (inclusive)");
    }
    if (sp > 32) {
      throw new UDFArgumentException("sp values greater than 32 not supported");
    }
    return new int[] {p, sp};
  }

  public static class HLLEvaluator extends GenericUDAFEvaluator {

    private int[] params;
    private ObjectInspector valueOI;

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters)
        throws HiveException {
      super.init(m, parameters);
      if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
        params = getParams(parameters);
      }
      valueOI = parameters[parameters.length - 1];
      if (mode == Mode.FINAL || mode == Mode.COMPLETE) {
        return PrimitiveObjectInspectorFactory.javaLongObjectInspector;
      }
      return PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      HLLBuffer buffer = new HLLBuffer();
      if (params != null) {
        buffer.hll = new HyperLogLogPlus(params[0], params[1]);
      }
      return buffer;
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      if (params != null) {
        ((HLLBuffer)agg).hll = new HyperLogLogPlus(params[0], params[1]);
      } else {
        ((HLLBuffer)agg).hll = null;
      }
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
      ((HLLBuffer)agg).hll.offer(ObjectInspectorUtils.copyToStandardJavaObject(parameters[parameters.length - 1], valueOI));
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      try {
        return ((HLLBuffer)agg).hll.getBytes();
      } catch (IOException e) {
        throw new HiveException(e);
      }
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
      try {
        byte[] binary = (byte[]) ObjectInspectorUtils.copyToStandardJavaObject(partial, valueOI);
        HyperLogLogPlus hll = HyperLogLogPlus.Builder.build(binary);
        if (((HLLBuffer)agg).hll != null) {
          ((HLLBuffer)agg).hll.addAll(hll);
        } else {
          ((HLLBuffer)agg).hll = hll;
        }
      } catch (Exception e) {
        throw new HiveException(e);
      }
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      return ((HLLBuffer)agg).hll.cardinality();
    }
  }

  private static class HLLBuffer extends GenericUDAFEvaluator.AbstractAggregationBuffer {

    private HyperLogLogPlus hll;

    @Override
    public int estimate() {
      return hll.sizeof();
    }
  }
}
