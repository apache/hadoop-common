package org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords;

import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.URL;

public interface SuccessfulLocalizationRequest {
  public abstract String getUser();
  public abstract LocalResource getResource();
  public abstract URL getPath();
  
  public abstract void setUser(String user);
  public abstract void setResource(LocalResource resource);
  public abstract void setPath(URL path);
}
