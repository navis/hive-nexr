package org.apache.hadoop.hive.ql.udf;

import org.apache.hive.common.util.ShutdownHookManager;
import org.rosuda.JRI.Rengine;

public class JRIEngine {
  
  private static Rengine ENGINE;
  
  static {
    ShutdownHookManager.addShutdownHook(
        new Runnable() { public void run() { shutdown(); } }, 0);
  }
  
  public static synchronized Rengine init() {
    if (ENGINE != null) {
      return ENGINE;
    }
    Rengine engine = new Rengine(new String[]{"--vanilla"}, false, null);
    if (!engine.waitForR()) {
      throw new RuntimeException("Failed to init r engine");
    }
    return ENGINE = engine;
  }

  public static synchronized void shutdown() {
    if (ENGINE != null) {
      ENGINE.end();
      ENGINE = null;
    }
  }
}
