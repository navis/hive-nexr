package org.apache.hadoop.hive.rdbms;

public class UnsupportedDatabaseException extends Exception {

  private String errorMessage;

  public UnsupportedDatabaseException() {
  }

  public UnsupportedDatabaseException(String message) {
    super(message);
    errorMessage = message;
  }

  public UnsupportedDatabaseException(String message, Throwable cause) {
    super(message, cause);
    errorMessage = message;
  }

  public UnsupportedDatabaseException(Throwable cause) {
    super(cause);
  }

  public String getErrorMessage() {
    return errorMessage;
  }
}
