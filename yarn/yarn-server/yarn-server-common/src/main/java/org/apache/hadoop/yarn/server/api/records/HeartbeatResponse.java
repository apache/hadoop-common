package org.apache.hadoop.yarn.server.api.records;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;

public interface HeartbeatResponse {
  public abstract int getResponseId();
  public abstract boolean getReboot();
  
  public abstract List<Container> getContainersToCleanupList();
  public abstract Container getContainerToCleanup(int index);
  public abstract int getContainersToCleanupCount();
  
  public abstract List<ApplicationId> getApplicationsToCleanupList();
  public abstract ApplicationId getApplicationsToCleanup(int index);
  public abstract int getApplicationsToCleanupCount();
  
  public abstract void setResponseId(int responseId);
  public abstract void setReboot(boolean reboot);
  
  public abstract void addAllContainersToCleanup(List<Container> containers);
  public abstract void addContainerToCleanup(Container container);
  public abstract void removeContainerToCleanup(int index);
  public abstract void clearContainersToCleanup();
  
  public abstract void addAllApplicationsToCleanup(List<ApplicationId> applications);
  public abstract void addApplicationToCleanup(ApplicationId applicationId);
  public abstract void removeApplicationToCleanup(int index);
  public abstract void clearApplicationsToCleanup();
}
