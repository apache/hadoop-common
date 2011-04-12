package org.apache.hadoop.yarn.api.records;

public interface Application {
  public ApplicationId getApplicationId();
  public void setApplicationId(ApplicationId applicationId);
  public String getUser();
  public void setUser(String user);
  public String getQueue();
  public void setQueue(String queue);
  public String getName();
  public void setName(String name);
  public ApplicationStatus getStatus();
  public void setStatus(ApplicationStatus status);
  public ApplicationState getState();
  public void setState(ApplicationState state);
  public String getMasterHost();
  public void setMasterHost(String masterHost);
  public int getMasterPort();
  public void setMasterPort(int masterPort);
}
