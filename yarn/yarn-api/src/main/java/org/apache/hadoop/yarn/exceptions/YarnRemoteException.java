package org.apache.hadoop.yarn.exceptions;

import java.io.IOException;

public abstract class YarnRemoteException extends IOException {
  private static final long serialVersionUID = 1L;
  
  public YarnRemoteException() {
    super();
  }
  
  public YarnRemoteException(String message, Throwable cause) {
    super(message, cause);
  }
  
  public YarnRemoteException(Throwable cause) {
    super(cause);
  }
  
  public YarnRemoteException(String message) {
    super(message);
  }
  
  public abstract String getRemoteTrace();
  
  public abstract YarnRemoteException getCause();
}