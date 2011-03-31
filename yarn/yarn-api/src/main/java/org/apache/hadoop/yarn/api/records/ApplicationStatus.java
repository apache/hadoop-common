package org.apache.hadoop.yarn.api.records;

public interface ApplicationStatus {
  public abstract int getResponseId();
  public abstract ApplicationId getApplicationId();
  public abstract float getProgress();
  public abstract long getLastSeen();
  
  public abstract void setResponseId(int id);
  public abstract void setApplicationId(ApplicationId applicationID);
  public abstract void setProgress(float progress);
  public abstract void setLastSeen(long lastSeen);
}
