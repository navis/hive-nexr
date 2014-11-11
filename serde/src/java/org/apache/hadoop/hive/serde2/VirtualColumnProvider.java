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

package org.apache.hadoop.hive.serde2;

import java.util.Collections;
import java.util.List;

/**
 * Implemented by Deserializer to provide custom virtual column(s).
 */
public interface VirtualColumnProvider {

  /**
   * Return virtual columns supported by deserializer.
   * @return
   */
  List<VirtualColumn> getVirtualColumns();

  /**
   * return value of virtual column provided, which should be conform with OI of the VC.
   * @param vc a virtual column, provided by {@link #getVirtualColumns}
   * @return
   */
  Object evaluate(VirtualColumn vc);


  // dummy implementation
  VirtualColumnProvider NULL = new VirtualColumnProvider() {
    @Override
    public List<VirtualColumn> getVirtualColumns() {
      return Collections.emptyList();
    }
    @Override
    public Object evaluate(VirtualColumn vc) {
      return null;
    }
  };
}
