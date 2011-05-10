package org.apache.hadoop.mapreduce.v2.jobhistory;

import org.apache.hadoop.mapreduce.v2.api.records.JobId;

/**
 * Maintains information which may be used by the jobHistroy indexing
 * system.
 */
public class JobIndexInfo {
  private long submitTime;
  private long finishTime;
  private String user;
  private String jobName;
  private JobId jobId;
  private int numMaps;
  private int numReduces;
  
  public JobIndexInfo() {
  }
  
  public JobIndexInfo(long submitTime, long finishTime, String user,
      String jobName, JobId jobId, int numMaps, int numReduces) {
    this.submitTime = submitTime;
    this.finishTime = finishTime;
    this.user = user;
    this.jobName = jobName;
    this.jobId = jobId;
    this.numMaps = numMaps;
    this.numReduces = numReduces;
  }
  
  public long getSubmitTime() {
    return submitTime;
  }
  public void setSubmitTime(long submitTime) {
    this.submitTime = submitTime;
  }
  public long getFinishTime() {
    return finishTime;
  }
  public void setFinishTime(long finishTime) {
    this.finishTime = finishTime;
  }
  public String getUser() {
    return user;
  }
  public void setUser(String user) {
    this.user = user;
  }
  public String getJobName() {
    return jobName;
  }
  public void setJobName(String jobName) {
    this.jobName = jobName;
  }
  public JobId getJobId() {
    return jobId;
  }
  public void setJobId(JobId jobId) {
    this.jobId = jobId;
  }
  public int getNumMaps() {
    return numMaps;
  }
  public void setNumMaps(int numMaps) {
    this.numMaps = numMaps;
  }
  public int getNumReduces() {
    return numReduces;
  }
  public void setNumReduces(int numReduces) {
    this.numReduces = numReduces;
  }

  @Override
  public String toString() {
    return "JobIndexInfo [submitTime=" + submitTime + ", finishTime="
        + finishTime + ", user=" + user + ", jobName=" + jobName + ", jobId="
        + jobId + ", numMaps=" + numMaps + ", numReduces=" + numReduces + "]";
  }
  
  
}
