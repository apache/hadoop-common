package org.apache.hadoop.yarn.api.records;

import java.util.List;
//TODO Check if this can replace AMRMProtocolResponse

public interface AMResponse {
  public abstract boolean getReboot();
  public abstract int getResponseId();
  
  public abstract List<Container> getContainerList();
  public abstract Container getContainer(int index);
  public abstract int getContainerCount();

  public abstract void setReboot(boolean reboot);
  public abstract void setResponseId(int responseId);
  
  public abstract void addAllContainers(List<Container> containers);
  public abstract void addContainer(Container container);
  public abstract void removeContainer(int index);
  public abstract void clearContainers();
}