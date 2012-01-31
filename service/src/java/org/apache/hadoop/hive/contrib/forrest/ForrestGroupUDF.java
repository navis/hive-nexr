package org.apache.hadoop.hive.contrib.forrest;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.util.ArrayList;
import java.util.Arrays;

@Description(name = "forrest_group",
    value = "_FUNC_() - Returns forrest group ID")
@UDFType(deterministic = true, stateful = false)
public class ForrestGroupUDF extends GenericUDF {

  ObjectInspector addressOI;
  ObjectInspector decisionOI;
  ForrestEvaluator evaluator;

  ArrayList<Object> result = new ArrayList<Object>();

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    this.addressOI = arguments[0];
    this.decisionOI = arguments[arguments.length - 1];

    this.evaluator = new ForrestEvaluator(arguments);

    ArrayList<String> fieldNames = new ArrayList<String>();
    ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

    fieldNames.add("groupID");
    fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    fieldNames.add("decision");
    fieldOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
    StandardStructObjectInspector param = ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);

    return ObjectInspectorFactory.getStandardListObjectInspector(param);
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (!evaluator.initialized()) {
      String address = ((StringObjectInspector) addressOI).getPrimitiveJavaObject(arguments[0].get());
      evaluator.initialize(address);
    }
    int decision = ((IntObjectInspector) decisionOI).get(arguments[arguments.length - 1].get());

    result.clear();
    int groupID = evaluator.evaluate(arguments);
    int[] axisIndex = evaluator.location(arguments);
    for (int axisID = 0; axisID < axisIndex.length; axisID++) {
      String groupKey = groupID + ":" + axisID + ":" + axisIndex[axisID];
      result.add(new Object[]{groupKey, decision});
    }
    return result;
  }

  @Override
  public String getDisplayString(String[] children) {
    return "forrest_group(" + children[0] + ")";
  }
}
