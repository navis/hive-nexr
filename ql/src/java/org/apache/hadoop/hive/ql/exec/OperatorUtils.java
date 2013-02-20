/**
 * Copyright 2010 The Apache Software Foundation
 *
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

import java.util.Arrays;
import java.util.List;

public class OperatorUtils {

  private static final List<Class<?>> FORWARD = Arrays.<Class<?>>asList(
      SelectOperator.class, ForwardOperator.class,
      ExtractOperator.class, FilterOperator.class);

  public static <T> T findSingleParent(Operator<?> current, Class<T> target) {
    return findSingleParent(current, FORWARD, target);
  }

  public static <T> T findSingleParent(Operator<?> current, List<Class<?>> allowed, Class<T> target) {
    if (current == null || target.isInstance(current)) {
      return (T) current;
    }
    if (allowed != null && !allowed.contains(current.getClass())) {
      return null;
    }
    Operator<?> parent = getSingleParent(current);
    return parent != null ? findSingleParent(parent, allowed, target) : null;
  }

  public static Operator<?> getSingleParent(Operator<?> current) {
    if (current.getParentOperators() != null && current.getParentOperators().size() == 1) {
      return current.getParentOperators().get(0);
    }
    return null;
  }

  public static <T> T findSingleChild(Operator<?> current, Class<T> target) {
    return findSingleChild(current, FORWARD, target);
  }

  public static <T> T findSingleChild(Operator<?> current, List<Class<?>> allowed, Class<T> target) {
    if (current == null || target.isInstance(current)) {
      return (T) current;
    }
    if (allowed != null && !allowed.contains(current.getClass())) {
      return null;
    }
    Operator<?> child = getSingleChild(current);
    return child != null ? findSingleChild(child, allowed, target) : null;
  }

  public static Operator<?> getSingleChild(Operator<?> current) {
    if (current.getChildOperators() != null && current.getChildOperators().size() == 1) {
      return current.getChildOperators().get(0);
    }
    return null;
  }
}
