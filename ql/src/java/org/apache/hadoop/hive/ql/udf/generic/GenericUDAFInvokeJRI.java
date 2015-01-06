package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

public class GenericUDAFInvokeJRI extends AbstractGenericUDAFResolver {

  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
      throws SemanticException {
    if (parameters.length != 1) {
      throw new UDFArgumentTypeException(parameters.length - 1,
          "Exactly one argument is expected.");
    }
    ObjectInspector oi = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(parameters[0]);
    if (!ObjectInspectorUtils.compareSupported(oi)) {
      throw new UDFArgumentTypeException(parameters.length - 1,
          "Cannot support comparison of map<> type or complex type containing map<>.");
    }
    return new GenericUDAFInvokeJRIEvaluator();
  }
  
  public static class GenericUDAFInvokeJRIEvaluator extends GenericUDAFEvaluator {

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      return super.init(m, parameters);
    }
    
    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      return null;
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      return null;
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      return null;
    }
  }
}
