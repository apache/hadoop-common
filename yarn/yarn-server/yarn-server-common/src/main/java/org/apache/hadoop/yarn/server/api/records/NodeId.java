package org.apache.hadoop.yarn.server.api.records;

public interface NodeId {
  public abstract int getId();
  
  public abstract void setId(int id);
}
