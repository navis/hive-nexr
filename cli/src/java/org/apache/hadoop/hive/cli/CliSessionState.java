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

package org.apache.hadoop.hive.cli;

import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hive.common.io.CachingPrintStream;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * SessionState for hive cli.
 *
 */
public class CliSessionState extends SessionState {
  /**
   * -database option if any that the session has been invoked with.
   */
  public String database;

  /**
   * -e option if any that the session has been invoked with.
   */
  public String execString;

  /**
   * -f option if any that the session has been invoked with.
   */
  public String fileName;

  /**
   * properties set from -hiveconf via cmdline.
   */
  public Properties cmdProperties = new Properties();

  /**
   * -i option if any that the session has been invoked with.
   */
  public List<String> initFiles = new ArrayList<String>();

  public CliSessionState(HiveConf conf) {
    super(conf);
    this.in = System.in;
    try {
      this.out = new PrintStream(System.out, true, "UTF-8");
      this.info = new PrintStream(System.err, true, "UTF-8");
      this.err = new CachingPrintStream(System.err, true, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    try {
      super.close();
    } catch (IOException ioe) {
      ioe.printStackTrace();
    } 
  }
}
