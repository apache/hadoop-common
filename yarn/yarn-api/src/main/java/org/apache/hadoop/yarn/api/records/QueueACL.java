package org.apache.hadoop.yarn.api.records;

public enum QueueACL {
  SUBMIT_JOB,
  ADMINISTER_QUEUE_JOBS,       // currently unused
  ADMINISTER_QUEUE;            // currently unused
}