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

import static org.apache.hadoop.hive.ql.optimizer.SimpleFetchOptimizer.ALL;
import static org.apache.hadoop.hive.ql.optimizer.SimpleFetchOptimizer.MINIMAL;
import static org.apache.hadoop.hive.ql.optimizer.SimpleFetchOptimizer.MORE;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class FetchConversionOptimizer implements Transform {

  private final SimpleFetchOptimizer optimizer = new SimpleFetchOptimizer();

  public ParseContext transform(ParseContext pctx) throws SemanticException {
    int conversion = getConversionMode(pctx);
    for (int mode = MINIMAL; mode <= conversion; mode++) {
      if (optimizer.transform(pctx, mode)) {
        break;
      }
    }
    return pctx;
  }

  private int getConversionMode(ParseContext pctx) {
    String conversion = HiveConf.getVar(pctx.getConf(), ConfVars.HIVEFETCHTASKCONVERSION);
    if (conversion.equalsIgnoreCase("MINIMAL")) {
      return MINIMAL;
    } else if (conversion.equalsIgnoreCase("MORE")) {
      return MORE;
    } else if (conversion.equalsIgnoreCase("ALL")) {
      return ALL;
    } else {
      throw new IllegalArgumentException(conversion);
    }
  }
}

