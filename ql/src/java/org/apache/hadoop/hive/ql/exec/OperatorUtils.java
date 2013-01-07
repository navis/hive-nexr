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

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Predicate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.NodeUtils.Function;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.mapred.OutputCollector;

import javax.annotation.Nullable;

public class OperatorUtils {

  private static final Log LOG = LogFactory.getLog(OperatorUtils.class);

  public static <T> Set<T> findOperators(Operator<?> start, Class<T> clazz) {
    return findOperators(start, clazz, new HashSet<T>());
  }

  public static <T> T findSingleOperator(Operator<?> start, Class<T> clazz) {
    Set<T> found = findOperators(start, clazz, new HashSet<T>());
    return found.size() == 1 ? found.iterator().next() : null;
  }

  public static <T> Set<T> findOperators(Collection<Operator<?>> starts, Class<T> clazz) {
    Set<T> found = new HashSet<T>();
    for (Operator<?> start : starts) {
      findOperators(start, clazz, found);
    }
    return found;
  }

  @SuppressWarnings("unchecked")
  private static <T> Set<T> findOperators(Operator<?> start, Class<T> clazz, Set<T> found) {
    if (clazz.isInstance(start)) {
      found.add((T) start);
    }
    if (start.getChildOperators() != null) {
      for (Operator<?> child : start.getChildOperators()) {
        findOperators(child, clazz, found);
      }
    }
    return found;
  }

  public static Set<Operator<?>> findOperators(Collection<Operator<?>> start,
      OperatorPredicate predicate) {
    for (Operator<?> operator : start) {
      findOperators(operator, predicate);
    }
    return predicate.found;
  }

  public static Set<Operator<?>> findOperators(Operator<?> start, OperatorPredicate predicate) {
    if (predicate.visit(start)) {
      return predicate.found;
    }
    if (start.getNumParent() > 0) {
      for (Operator<?> parent : start.getParentOperators()) {
        findOperators(parent, predicate);
      }
    }
    if (start.getNumChild() > 0) {
      for (Operator<?> child : start.getChildOperators()) {
        findOperators(child, predicate);
      }
    }
    return predicate.found;
  }

  public static abstract class OperatorPredicate implements Predicate<Operator<?>> {
    final Class<?> clazz = getClass();
    final Set<Operator<?>> visited = new HashSet<Operator<?>>();
    final Set<Operator<?>> found = new HashSet<Operator<?>>();
    private boolean visit(Operator<?> operator) {
      boolean already = !visited.add(operator);
      if (!already && apply(operator)) {
        found.add(operator);
      }
      return already;
    }
    @Override
    public boolean equals(@Nullable Object object) {
      return clazz.isInstance(object);
    }
  }

  public static Set<Operator<?>> findTopOps(Collection<Operator<?>> start) {
  Set<Operator<?>> topOps = OperatorUtils.findOperators(start,
      new OperatorUtils.OperatorPredicate() {
        @Override
        public boolean apply(@Nullable Operator<?> input) {
          return input != null && input.getNumParent() == 0;
        }
      });
    return topOps;
  }

  public static <T> Set<T> findOperatorsUpstream(Operator<?> start, Class<T> clazz) {
    return findOperatorsUpstream(start, clazz, new HashSet<T>());
  }

  public static <T> T findSingleOperatorUpstream(Operator<?> start, Class<T> clazz) {
    Set<T> found = findOperatorsUpstream(start, clazz, new HashSet<T>());
    return found.size() == 1 ? found.iterator().next() : null;
  }

  public static <T> Set<T> findOperatorsUpstream(Collection<Operator<?>> starts, Class<T> clazz) {
    Set<T> found = new HashSet<T>();
    for (Operator<?> start : starts) {
      findOperatorsUpstream(start, clazz, found);
    }
    return found;
  }

  @SuppressWarnings("unchecked")
  private static <T> Set<T> findOperatorsUpstream(Operator<?> start, Class<T> clazz, Set<T> found) {
    if (clazz.isInstance(start)) {
      found.add((T) start);
    }
    if (start.getParentOperators() != null) {
      for (Operator<?> parent : start.getParentOperators()) {
        findOperatorsUpstream(parent, clazz, found);
      }
    }
    return found;
  }

  public static void setChildrenCollector(List<Operator<? extends OperatorDesc>> childOperators, OutputCollector out) {
    if (childOperators == null) {
      return;
    }
    for (Operator<? extends OperatorDesc> op : childOperators) {
      if (op.getName().equals(ReduceSinkOperator.getOperatorName())) {
        op.setOutputCollector(out);
      } else {
        setChildrenCollector(op.getChildOperators(), out);
      }
    }
  }

  public static void setChildrenCollector(List<Operator<? extends OperatorDesc>> childOperators, Map<String, OutputCollector> outMap) {
    if (childOperators == null) {
      return;
    }
    for (Operator<? extends OperatorDesc> op : childOperators) {
      if(op.getName().equals(ReduceSinkOperator.getOperatorName())) {
        ReduceSinkOperator rs = ((ReduceSinkOperator)op);
        if (outMap.containsKey(rs.getConf().getOutputName())) {
          LOG.info("Setting output collector: " + rs + " --> " 
            + rs.getConf().getOutputName());
          rs.setOutputCollector(outMap.get(rs.getConf().getOutputName()));
        }
      } else {
        setChildrenCollector(op.getChildOperators(), outMap);
      }
    }
  }

  public static void iterateParents(Operator<?> operator, Function<Operator<?>> function) {
    iterateParents(operator, function, new HashSet<Operator<?>>());
  }

  private static void iterateParents(Operator<?> operator, Function<Operator<?>> function, Set<Operator<?>> visited) {
    if (!visited.add(operator)) {
      return;
    }
    function.apply(operator);
    if (operator.getNumParent() > 0) {
      for (Operator<?> parent : operator.getParentOperators()) {
        iterateParents(parent, function, visited);
      }
    }
  }
}
