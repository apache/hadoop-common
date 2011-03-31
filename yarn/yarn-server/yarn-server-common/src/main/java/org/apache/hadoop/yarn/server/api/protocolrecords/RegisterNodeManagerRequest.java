package org.apache.hadoop.yarn.server.api.protocolrecords;

import org.apache.hadoop.yarn.api.records.Resource;

public interface RegisterNodeManagerRequest {
  public abstract String getNode();
  public abstract Resource getResource();
  
  public abstract void setNode(String node);
  public abstract void setResource(Resource resource);
}
