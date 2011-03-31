package org.apache.hadoop.yarn.api.records;

public interface ContainerStatus {
  public abstract ContainerId getContainerId();
  public abstract ContainerState getState();
  public abstract int getExitStatus();
  
  public abstract void setContainerId(ContainerId containerId);
  public abstract void setState(ContainerState state);
  public abstract void setExitStatus(int exitStatus);
}
