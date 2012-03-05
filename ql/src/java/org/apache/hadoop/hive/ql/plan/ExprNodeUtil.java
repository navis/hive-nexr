package org.apache.hadoop.hive.ql.plan;

import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ExprNodeUtil {

  public static ArrayList<ExprNodeDesc> backtrack(List<ExprNodeDesc> sources,
      Operator<?> current, Operator<?> terminal, int alias) throws SemanticException {
    ArrayList<ExprNodeDesc> result = new ArrayList<ExprNodeDesc>();
    for (ExprNodeDesc expr : sources) {
      result.add(backtrack(expr, current, terminal, alias));
    }
    return result;
  }

  private static ExprNodeDesc backtrack(ExprNodeDesc source, Operator<?> current,
      Operator<?> terminal, int alias) throws SemanticException {
    if (current == null || current == terminal) {
      return source;
    }
    if (source instanceof ExprNodeGenericFuncDesc) {
      ExprNodeGenericFuncDesc function = (ExprNodeGenericFuncDesc) source.clone();
      function.setChildExprs(backtrack(function.getChildren(), current, terminal, alias));
      return function;
    }
    if (source instanceof ExprNodeColumnDesc) {
      ExprNodeColumnDesc column = (ExprNodeColumnDesc) source;
      return backtrack(column, current, terminal, alias);
    }
    if (source instanceof ExprNodeFieldDesc) {
      ExprNodeFieldDesc field = (ExprNodeFieldDesc) source.clone();
      field.setDesc(backtrack(field.getDesc(), current, terminal, alias));
      return field;
    }
    return source;
  }

  private static ExprNodeDesc backtrack(ExprNodeColumnDesc column, Operator<?> current,
      Operator<?> terminal, int alias) throws SemanticException {
    if (current == null || current == terminal) {
      return column;
    }
    Map<String, ExprNodeDesc> mapping = current.getColumnExprMap();
    if (mapping == null || !mapping.containsKey(column.getColumn())) {
      return backtrack(column, getSingleParent(current, terminal, alias), terminal, alias);
    }
    ExprNodeDesc mapped = mapping.get(column.getColumn());
    return backtrack(mapped, getSingleParent(current, terminal, alias), terminal, alias);
  }

  private static Operator<? extends Serializable> getSingleParent(Operator<?> current,
      Operator<?> terminal, int alias) throws SemanticException {
    List<Operator<? extends Serializable>> parents = current.getParentOperators();
    if (current instanceof JoinOperator || current instanceof MapJoinOperator
        || current instanceof UnionOperator) {
      if (parents.size() < alias) {
        throw new SemanticException("invalid parent alias " + alias);
      }
      return parents.get(alias);
    }
    return parents == null ? null : parents.get(0);
  }
}
