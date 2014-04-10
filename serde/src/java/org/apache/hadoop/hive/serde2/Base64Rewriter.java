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

import org.apache.commons.codec.binary.Base64;

import java.io.IOException;

public class Base64Rewriter extends AbstractFieldRewriter {

  @Override
  public void encode(int index, ByteStream.Input input, ByteStream.Output output)
      throws IOException {
    output.write(Base64.encodeBase64(input.toBytes()));
  }

  @Override
  public void decode(int index, ByteStream.Input input, ByteStream.Output output)
      throws IOException {
    output.write(Base64.decodeBase64(input.toBytes()));
  }
}
