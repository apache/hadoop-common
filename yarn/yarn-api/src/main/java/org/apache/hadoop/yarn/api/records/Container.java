package org.apache.hadoop.yarn.api.records;


public interface Container extends Comparable<Container> {
  public abstract ContainerId getId();
  public abstract String getHostName();
  public abstract Resource getResource();
  public abstract ContainerState getState();
  public abstract ContainerToken getContainerToken();
  
  public abstract void setId(ContainerId id);
  public abstract void setHostName(String hostName);
  public abstract void setResource(Resource resource);
  public abstract void setState(ContainerState state);
  public abstract void setContainerToken(ContainerToken containerToken);
  
}
