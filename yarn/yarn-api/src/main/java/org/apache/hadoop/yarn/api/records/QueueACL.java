package org.apache.hadoop.yarn.api.records;

public enum QueueACL {
  SUBMIT_JOB,
  ADMINISTER_QUEUES,    
  ADMINISTER_JOBS;            // currently unused
}