package org.apache.hive.beeline;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import jline.ConsoleReader;
import jline.History;
import jline.SimpleCompletor;
import org.apache.hive.jdbc.HiveConnection;
import org.apache.hive.jdbc.Utils;
import org.apache.hive.service.OperationInfo;
import org.apache.hive.service.SessionInfo;
import org.apache.hive.service.cli.CLIServiceClient;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.thrift.THandleIdentifier;
import org.apache.hive.service.cli.thrift.ThriftCLIServiceClient;

public class AdminConsole implements Closeable {

  public void close() throws IOException {
  }

  private static enum COMMAND {
    sessions, operations, cancelop, closeop, closesession, quit
  }

  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      System.err.println("hconsole <hiveserver2 address, for example hive2://localhost:1000>");
      System.exit(1);
    }
    Utils.JdbcConnectionParams connParams = Utils.parseURI(URI.create(args[0]));
    HiveConnection connection = new HiveConnection();
    connection.initialize(connParams, new Properties());
    AdminConsole colsole = new AdminConsole(new ThriftCLIServiceClient(connection.getClient()));
    try {
      colsole.execute();
    } finally {
      colsole.close();
    }
  }

  private final CLIServiceClient client;

  private AdminConsole(CLIServiceClient client) {
    this.client = client;
  }

  public void execute() throws Exception {

    ConsoleReader reader = new ConsoleReader();
    reader.setBellEnabled(false);

    try {
      File userHome = new File(System.getProperty("user.home"));
      if (userHome.exists()) {
        reader.setHistory(new History(new File(userHome, ".hadminhistory")));
      }
    } catch (Exception e) {
      System.err.println("WARNING: Encountered an error while trying to initialize Hive's " +
          "history file.  History will not be available during this session.");
      System.err.println(e.getMessage());
    }

    SimpleCompletor completor = new SimpleCompletor(new String[0]);
    for (COMMAND command : COMMAND.values()) {
      completor.addCandidateString(command.name());
    }

    reader.addCompletor(completor);

    String line;
    while ((line = reader.readLine(">")) != null) {
      line = line.trim();
      if (line.isEmpty()) {
        continue;
      }
      String[] command;
      if (line.equals("!")) {
        command = prev;
      } else {
        command = line.toLowerCase().split("(\\s*,\\s*)|(\\s+)");
      }
      try {
        if (!executeCommand(command)) {
          break;
        }
        prev = command;
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  private List<OperationInfo> operations;
  private List<SessionInfo> sessions;

  private String[] prev;

  private boolean executeCommand(String[] commands) throws Exception {
    if (commands[0].equals("sessions")) {
      sessions = client.getSessions();
      for (int i = 0; i < sessions.size(); i++) {
        System.out.println(toString(i, sessions.get(i)));
      }
    } else if (commands[0].equals("operations")) {
      operations = client.getOperations();
      for (int i = 0; i < operations.size(); i++) {
        System.out.println(toString(i, operations.get(i)));
      }
    } else if (commands[0].equals("cancelop")) {
      if (commands.length > 1 && operations != null) {
        OperationInfo op = operations.get(Integer.valueOf(commands[1]));
        client.cancelOperation(new OperationHandle(op.getOperationHandle()));
      }
    } else if (commands[0].equals("closeop")) {
      if (commands.length > 1 && operations != null) {
        OperationInfo op = operations.get(Integer.valueOf(commands[1]));
        client.closeOperation(new OperationHandle(op.getOperationHandle()));
      }
    } else if (commands[0].equals("closesession")) {
      if (commands.length > 1 && sessions != null) {
        SessionInfo session = sessions.get(Integer.valueOf(commands[1]));
        client.closeSession(new SessionHandle(session.getSessionHandle()));
      }
    } else if (commands[0].equals("help")) {
      System.out.println("Supported commands : " + Arrays.toString(COMMAND.values()));
    } else if (commands[0].equals("quit")) {
      return false;
    } else {
      System.out.println("invalid command " + commands[0]);
    }
    return true;
  }

  private String toString(int index, OperationInfo operation) {
    StringBuilder builder = new StringBuilder();
    if (index >= 0) {
      builder.append(index).append(' ');
    }
    builder.append('[').append(operation.getStatus()).append(']');
    builder.append(' ').append(toString(operation.getOperationHandle().getOperationId()));
    if (operation.getQuery() != null) {
      builder.append(" = ").append(operation.getQuery());
    }
    if (operation.getStartTime() > 0) {
      builder.append(" [").append(new Date(operation.getStartTime())).append(']');
    }
    return builder.toString();
  }

  private String toString(int index, SessionInfo session) {
    StringBuilder builder = new StringBuilder();
    if (index >= 0) {
      builder.append(index).append(' ');
    }
    builder.append(toString(session.getSessionHandle().getSessionId()));
    if (session.getStartTime() > 0) {
      builder.append(" [").append(new Date(session.getStartTime())).append(']');
    }
    return builder.toString();
  }

  private String toString(THandleIdentifier handle) {
    ByteBuffer bb = ByteBuffer.wrap(handle.getGuid());
    return new UUID(bb.getLong(), bb.getLong()).toString();
  }
}
