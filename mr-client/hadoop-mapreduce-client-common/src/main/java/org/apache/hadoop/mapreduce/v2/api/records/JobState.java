package org.apache.hadoop.mapreduce.v2.api.records;

public enum JobState {
  NEW,
  RUNNING,
  SUCCEEDED,
  FAILED,
  KILL_WAIT,
  KILLED,
  ERROR
}
