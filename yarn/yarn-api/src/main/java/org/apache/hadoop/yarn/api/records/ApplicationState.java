package org.apache.hadoop.yarn.api.records;

public enum ApplicationState {
  PENDING, 
  ALLOCATING, 
  ALLOCATED, 
  EXPIRED_PENDING, 
  LAUNCHING, 
  LAUNCHED, 
  RUNNING, 
  PAUSED, 
  CLEANUP, 
  COMPLETED, 
  KILLED, 
  FAILED
}
