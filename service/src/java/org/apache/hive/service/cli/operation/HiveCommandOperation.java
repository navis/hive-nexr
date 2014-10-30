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

package org.apache.hive.service.cli.operation;

import org.apache.hadoop.hive.ql.processors.CommandProcessor;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.io.IOUtils;
import org.apache.hive.service.cli.session.HiveSession;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.Map;

/**
 * Executes a HiveCommand
 */
public class HiveCommandOperation extends ExecuteStatementOperation {

  protected HiveCommandOperation(HiveSession parentSession, String statement,
      CommandProcessor processor, Map<String, String> confOverlay, boolean runAsync) {
    super(parentSession, statement, processor, confOverlay, runAsync);
  }

  @Override
  protected void beforeRun() {
    super.beforeRun();
    setupSessionIO(parentSession.getSessionState());
  }

  @Override
  protected void afterRun() {
    tearDownSessionIO(parentSession.getSessionState());
    super.afterRun();
  }

  private void setupSessionIO(SessionState sessionState) {
    try {
      LOG.info("Putting temp output to file " + sessionState.getTmpOutputFile().toString());
      sessionState.in = null; // hive server's session input stream is not used
      // open a per-session file in auto-flush mode for writing temp results
      sessionState.out = new PrintStream(new FileOutputStream(sessionState.getTmpOutputFile()), true, "UTF-8");
      // TODO: for hadoop jobs, progress is printed out to session.err,
      // we should find a way to feed back job progress to client
      sessionState.err = new PrintStream(System.err, true, "UTF-8");
    } catch (IOException e) {
      LOG.error("Error in creating temp output file ", e);
      try {
        sessionState.in = null;
        sessionState.out = new PrintStream(System.out, true, "UTF-8");
        sessionState.err = new PrintStream(System.err, true, "UTF-8");
      } catch (UnsupportedEncodingException ee) {
        LOG.error("Error creating PrintStream", e);
        ee.printStackTrace();
        sessionState.out = null;
        sessionState.err = null;
      }
    }
  }

  private void tearDownSessionIO(SessionState sessionState) {
    IOUtils.cleanup(LOG, sessionState.out);
    IOUtils.cleanup(LOG, sessionState.err);
  }
}
