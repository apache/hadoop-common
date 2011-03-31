package org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords;

import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;

public interface FailedLocalizationRequest {
  public abstract String getUser();
  public abstract LocalResource getResource();
  public abstract YarnRemoteException getException();
  
  public abstract void setUser(String user);
  public abstract void setResource(LocalResource resource);
  public abstract void setException(YarnRemoteException exception);

}
