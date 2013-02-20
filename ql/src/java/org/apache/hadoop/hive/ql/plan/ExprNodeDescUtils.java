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

package org.apache.hadoop.hive.ql.plan;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluatorFactory;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.util.ReflectionUtils;

public class ExprNodeDescUtils {

  public static int indexOf(ExprNodeDesc origin, List<ExprNodeDesc> sources) {
    for (int i = 0; i < sources.size(); i++) {
      if (origin.isSame(sources.get(i))) {
        return i;
      }
    }
    return -1;
  }

  // traversing origin, find ExprNodeDesc in sources and replaces it with ExprNodeDesc
  // in targets having same index.
  // return null if failed to find
  public static ExprNodeDesc replace(ExprNodeDesc origin,
      List<ExprNodeDesc> sources, List<ExprNodeDesc> targets) {
    int index = indexOf(origin, sources);
    if (index >= 0) {
      return targets.get(index);
    }
    // encountered column or field which cannot be found in sources
    if (origin instanceof ExprNodeColumnDesc || origin instanceof ExprNodeFieldDesc) {
      return null;
    }
    // for ExprNodeGenericFuncDesc, it should be deterministic and stateless
    if (origin instanceof ExprNodeGenericFuncDesc) {
      ExprNodeGenericFuncDesc func = (ExprNodeGenericFuncDesc) origin;
      if (!FunctionRegistry.isDeterministic(func.getGenericUDF())
          || FunctionRegistry.isStateful(func.getGenericUDF())) {
        return null;
      }
      List<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>();
      for (int i = 0; i < origin.getChildren().size(); i++) {
        ExprNodeDesc child = replace(origin.getChildren().get(i), sources, targets);
        if (child == null) {
          return null;
        }
        children.add(child);
      }
      // duplicate function with possibily replaced children
      ExprNodeGenericFuncDesc clone = (ExprNodeGenericFuncDesc) func.clone();
      clone.setChildExprs(children);
      return clone;
    }
    // constant or null, just return it
    return origin;
  }

  /**
   * return true if predicate is already included in source
    */
  public static boolean containsPredicate(ExprNodeDesc source, ExprNodeDesc predicate) {
    if (source.isSame(predicate)) {
      return true;
    }
    if (FunctionRegistry.isOpAnd(source)) {
      if (containsPredicate(source.getChildren().get(0), predicate) ||
          containsPredicate(source.getChildren().get(1), predicate)) {
        return true;
      }
    }
    return false;
  }

  /**
   * bind two predicates by AND op
    */
  public static ExprNodeDesc mergePredicates(ExprNodeDesc prev, ExprNodeDesc next) {
    List<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>(2);
    children.add(prev);
    children.add(next);
    return new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
        FunctionRegistry.getGenericUDFForAnd(), children);
  }

  public static ExprNodeDesc[] extractComparePair(ExprNodeDesc expr1, ExprNodeDesc expr2) {
    expr1 = extractConstant(expr1);
    expr2 = extractConstant(expr2);
    if (expr1 instanceof ExprNodeColumnDesc && expr2 instanceof ExprNodeConstantDesc) {
      return new ExprNodeDesc[] {expr1, expr2};
    }
    if (expr1 instanceof ExprNodeConstantDesc && expr2 instanceof ExprNodeColumnDesc) {
      return new ExprNodeDesc[] {expr2, expr1, null}; // add null as a marker (inverted order)
    }
    return null;
  }

  // from IndexPredicateAnalyzer
  private static ExprNodeDesc extractConstant(ExprNodeDesc expr) {
    if (!(expr instanceof ExprNodeGenericFuncDesc)) {
      return expr;
    }
    ExprNodeConstantDesc folded = foldConstant(((ExprNodeGenericFuncDesc) expr));
    return folded == null ? expr : folded;
  }

  private static ExprNodeConstantDesc foldConstant(ExprNodeGenericFuncDesc func) {
    GenericUDF udf = func.getGenericUDF();
    if (!FunctionRegistry.isDeterministic(udf) || FunctionRegistry.isStateful(udf)) {
      return null;
    }
    try {
      // If the UDF depends on any external resources, we can't fold because the
      // resources may not be available at compile time.
      if (udf instanceof GenericUDFBridge) {
        UDF internal = ReflectionUtils.newInstance(((GenericUDFBridge) udf).getUdfClass(), null);
        if (internal.getRequiredFiles() != null || internal.getRequiredJars() != null) {
          return null;
        }
      } else {
        if (udf.getRequiredFiles() != null || udf.getRequiredJars() != null) {
          return null;
        }
      }

      for (ExprNodeDesc child : func.getChildExprs()) {
        if (child instanceof ExprNodeConstantDesc) {
          continue;
        } else if (child instanceof ExprNodeGenericFuncDesc) {
          if (foldConstant((ExprNodeGenericFuncDesc) child) != null) {
            continue;
          }
        }
        return null;
      }
      ExprNodeEvaluator evaluator = ExprNodeEvaluatorFactory.get(func);
      ObjectInspector output = evaluator.initialize(null);

      Object constant = evaluator.evaluate(null);
      Object java = ObjectInspectorUtils.copyToStandardJavaObject(constant, output);

      return new ExprNodeConstantDesc(java);
    } catch (Exception e) {
      return null;
    }
  }

  public static ExprNodeDesc backtrack(ExprNodeDesc origin, Operator<?> start, Operator<?> stop) {
    if (origin instanceof ExprNodeConstantDesc || origin instanceof ExprNodeNullDesc) {
      return origin;
    }
    Map<String, ExprNodeDesc> exprMapping =  start.getColumnExprMap();
    if (origin instanceof ExprNodeColumnDesc) {
      if (exprMapping != null && !exprMapping.isEmpty()) {
        ExprNodeDesc source = exprMapping.get(((ExprNodeColumnDesc) origin).getColumn());
        if (source == null) {
          throw new IllegalStateException();
        }
        return forward(source, start, stop);
      }
      return forward(origin, start, stop);
    }
    if (origin instanceof ExprNodeGenericFuncDesc) {
      ExprNodeGenericFuncDesc func = (ExprNodeGenericFuncDesc) origin.clone();
      func.getChildren().clear();
      for (ExprNodeDesc child : origin.getChildren()) {
        func.getChildren().add(backtrack(child, start, stop));
      }
      return func;
    }
    if (origin instanceof ExprNodeFieldDesc) {
      ExprNodeFieldDesc field = (ExprNodeFieldDesc) origin.clone();
      field.getChildren().clear();
      field.getChildren().add(backtrack(field.getDesc(), start, stop));
      return field;
    }
    throw new IllegalStateException();
  }

  private static ExprNodeDesc forward(ExprNodeDesc origin, Operator<?> start, Operator<?> stop) {
    if (start == stop || stop == null) {
      return origin;
    }
    if (start.getParentOperators() == null || start.getParentOperators().size() != 1) {
      throw new IllegalStateException();
    }
    return backtrack(origin, start.getParentOperators().get(0), stop);
  }
}
