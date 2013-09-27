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

package org.apache.hadoop.hive.ql.exec;

import java.util.List;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ptf.PTFExpressionDef;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFunctionDef;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFLead;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFLeadLag;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;

public class LLRowResolver {

  private final PTFExpressionDef valueExpr;
  private final PTFExpressionDef defaultExpr;
  private final ObjectInspectorConverters.Converter defaultValueConverter;
  private final int amount;

  public LLRowResolver(List<PTFExpressionDef> argExprs, int amount) {
    this.valueExpr = argExprs.get(0);
    this.defaultExpr = argExprs.size() > 2 ? argExprs.get(2) : null;
    this.defaultValueConverter = defaultExpr != null ?
        ObjectInspectorConverters.getConverter(defaultExpr.getOI(), valueExpr.getOI()) : null;
    this.amount = amount;
  }

  public Object evaluateAndCopy(PTFPartition.PTFPartitionIterator iterator) throws HiveException {
    Object evaluate = evaluate(iterator);
    return evaluate != null ?
        ObjectInspectorUtils.copyToStandardObject(evaluate, valueExpr.getOI()) : null;
  }

  private Object evaluate(PTFPartition.PTFPartitionIterator iterator) throws HiveException {
    boolean hasValue = amount < 0 ? iterator.hasLead(-amount) : iterator.hasLag(amount);
    if (hasValue) {
      Object row = amount < 0 ? iterator.lead(-amount) : iterator.lag(amount);
      return valueExpr.getExprEvaluator().evaluate(row);
    }
    if (defaultExpr != null) {
      Object evaluate = defaultExpr.getExprEvaluator().evaluate(iterator.current());
      return defaultValueConverter.convert(evaluate);
    }
    return null;
  }

  public static boolean isLL(WindowFunctionDef function) {
    return function.getWFnEval() instanceof GenericUDAFLeadLag.GenericUDAFLeadLagEvaluator;
  }

  public static LLRowResolver[] toResolver(List<WindowFunctionDef> functions) {
    LLRowResolver[] resolvers = new LLRowResolver[functions.size()];
    for (int i = 0; i < resolvers.length; i++) {
      WindowFunctionDef function = functions.get(i);
      GenericUDAFEvaluator eval = function.getWFnEval();
      if (!(eval instanceof GenericUDAFLeadLag.GenericUDAFLeadLagEvaluator)) {
        continue;
      }
      if (resolvers == null) {
        resolvers = new LLRowResolver[functions.size()];
      }
      int amt = ((GenericUDAFLeadLag.GenericUDAFLeadLagEvaluator) eval).getAmt();
      if (eval instanceof GenericUDAFLead.GenericUDAFLeadEvaluator) {
        amt = -amt;
      }
      resolvers[i] = new LLRowResolver(function.getArgs(), amt);
    }
    return resolvers;
  }

  public static int[] toRange(LLRowResolver[] resolvers) {
    int[] range = new int[2];
    for (LLRowResolver window : resolvers) {
      if (window == null) {
        continue;
      }
      if (window.amount > 0) {
        range[0] = Math.max(range[0], window.amount); // lag
      } else if (window.amount < 0) {
        range[1] = Math.min(range[1], window.amount); // lead
      }
    }
    return range;
  }
}
