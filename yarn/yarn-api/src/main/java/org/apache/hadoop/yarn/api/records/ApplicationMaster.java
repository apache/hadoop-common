package org.apache.hadoop.yarn.api.records;

public interface ApplicationMaster {
  public abstract ApplicationId getApplicationId();
  public abstract String getHost();
  public abstract int getRpcPort();
  public abstract int getHttpPort();
  public abstract ApplicationStatus getStatus();
  public abstract ApplicationState getState();
  public abstract String getClientToken();
  
  public abstract void setApplicationId(ApplicationId appId);
  public abstract void setHost(String host);
  public abstract void setRpcPort(int rpcPort);
  public abstract void setHttpPort(int httpPort);
  public abstract void setStatus(ApplicationStatus status);
  public abstract void setState(ApplicationState state);
  public abstract void setClientToken(String clientToken);

}
