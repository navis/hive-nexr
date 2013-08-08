package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class GenericUDAFColumnNameTest extends AbstractGenericUDAFResolver {

  @Override
  public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info)
    throws SemanticException {
    return new GenericUDAFColumnNameEvaluator();
  }

  public static class GenericUDAFColumnNameEvaluator extends GenericUDAFEvaluator {

    private transient String colNames;

    @Override
    public ObjectInspector init(Mode m, StructObjectInspector inputOI) throws HiveException {
      super.init(m, inputOI);
      if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
        StringBuilder builder = new StringBuilder();
        for (StructField field : inputOI.getAllStructFieldRefs()) {
          if (builder.length() > 0) {
            builder.append(", ");
          }
          builder.append(field.getFieldName());
        }
        colNames = builder.toString();
      }
      return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }
    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      return new DummyBuffer();
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      return colNames;
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
      if (colNames == null) {
        colNames =
          PrimitiveObjectInspectorFactory.javaStringObjectInspector.getPrimitiveJavaObject(partial);
      }
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      return colNames;
    }
  }

  private static class DummyBuffer implements GenericUDAFEvaluator.AggregationBuffer {
  }
}
