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

package org.apache.hadoop.hive.ql.optimizer;

import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.exec.Operator;
import static org.apache.hadoop.hive.ql.optimizer.InMemoryProcessOptimizer.*;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import java.io.Serializable;
import java.util.Map;

public class SimpleFetchOptimizer implements Transform {

  private final InMemoryProcessOptimizer optimizer = new InMemoryProcessOptimizer();

  public ParseContext transform(ParseContext pctx) throws SemanticException {
    int mode;
    Map<String, Operator<? extends Serializable>> topOps = pctx.getTopOps();
    if (pctx.getQB().isSimpleSelectQuery() && topOps.size() == 1) {
      mode = pctx.isSet(ConfVars.HIVEAGGRESIVEFETCHTASKCONVERSION) ? SIMPLE_AGGRESSIVE : SIMPLE;
    } else if (pctx.isSet(ConfVars.HIVENOMRCONVERSION)) {
      mode = FULL_AGGRESSIVE;
    } else {
      return pctx;
    }
    for (;mode >= 0; mode = nextMode(pctx, mode)) {
      if (optimizer.transform(pctx, mode)) {
        break;
      }
    }
    return pctx;
  }

  private int nextMode(ParseContext pctx, int current) {
    switch (current) {
      case SIMPLE:
        if (pctx.isSet(ConfVars.HIVEAGGRESIVEFETCHTASKCONVERSION)) {
          return SIMPLE_AGGRESSIVE;
        }
      case SIMPLE_AGGRESSIVE:
        if (pctx.isSet(ConfVars.HIVENOMRCONVERSION)) {
          return FULL_AGGRESSIVE;
        }
    }
    return -1;
  }
}
