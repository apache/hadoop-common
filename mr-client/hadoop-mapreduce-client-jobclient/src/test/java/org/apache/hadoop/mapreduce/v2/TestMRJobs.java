/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.hadoop.mapreduce.v2;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.FailingMapper;
import org.apache.hadoop.RandomTextWriterJob;
import org.apache.hadoop.SleepJob;
import org.apache.hadoop.RandomTextWriterJob.RandomInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.conf.YARNApplicationConstants;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.YarnServerConfig;
import org.apache.hadoop.yarn.server.nodemanager.NMConfig;
import org.apache.hadoop.yarn.server.resourcemanager.RMConfig;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestMRJobs {

  private static final Log LOG = LogFactory.getLog(TestMRJobs.class);

  private static MiniMRYarnCluster mrCluster;

  @Before
  public void setup() throws InterruptedException, IOException {

    if (!(new File(MiniMRYarnCluster.APPJAR)).exists()) {
      LOG.info("MRAppJar " + MiniMRYarnCluster.APPJAR + " not found. Not running test.");
      return;
    }

    if (mrCluster == null) {
      mrCluster = new MiniMRYarnCluster(getClass().getName());
      mrCluster.init(new Configuration());
      mrCluster.start();
    }
  }

  @Test
  public void testSleepJob() throws IOException, InterruptedException,
      ClassNotFoundException { 

    if (!(new File(MiniMRYarnCluster.APPJAR)).exists()) {
      LOG.info("MRAppJar " + MiniMRYarnCluster.APPJAR + " not found. Not running test.");
      return;
    }

    SleepJob sleepJob = new SleepJob();
    sleepJob.setConf(mrCluster.getConfig());
    //Job with 3 maps and 2 reduces
    Job job = sleepJob.createJob(3, 2, 10000, 1, 5000, 1);
    // TODO: We should not be setting MRAppJar as job.jar. It should be
    // uploaded separately by YarnRunner.
    job.setJar(new File(MiniMRYarnCluster.APPJAR).getAbsolutePath());
    job.waitForCompletion(true);
    Assert.assertEquals(JobStatus.State.SUCCEEDED, job.getJobState());
  }

  @Test
  public void testRandomWriter() throws IOException, InterruptedException,
      ClassNotFoundException {

    if (!(new File(MiniMRYarnCluster.APPJAR)).exists()) {
      LOG.info("MRAppJar " + MiniMRYarnCluster.APPJAR + " not found. Not running test.");
      return;
    }

    RandomTextWriterJob randomWriterJob = new RandomTextWriterJob();
    mrCluster.getConfig().set(RandomTextWriterJob.TOTAL_BYTES, "3072");
    mrCluster.getConfig().set(RandomTextWriterJob.BYTES_PER_MAP, "1024");
    Job job = randomWriterJob.createJob(mrCluster.getConfig());
    FileOutputFormat.setOutputPath(job, new Path(mrCluster.getTestWorkDir().getAbsolutePath(),
        "random-output"));
    // TODO: We should not be setting MRAppJar as job.jar. It should be
    // uploaded separately by YarnRunner.
    job.setJar(new File(MiniMRYarnCluster.APPJAR).getAbsolutePath());
    job.waitForCompletion(true);
    Assert.assertEquals(JobStatus.State.SUCCEEDED, job.getJobState());
  }

  @Test
  public void testFailingMapper() throws IOException, InterruptedException,
      ClassNotFoundException {

    if (!(new File(MiniMRYarnCluster.APPJAR)).exists()) {
      LOG.info("MRAppJar " + MiniMRYarnCluster.APPJAR + " not found. Not running test.");
      return;
    }

    int numMaps = 1;
    mrCluster.getConfig().setInt(MRJobConfig.NUM_MAPS, numMaps);
    
    mrCluster.getConfig().setInt("mapreduce.task.timeout", 10*1000);//reduce the timeout
    mrCluster.getConfig().setInt(MRJobConfig.MAP_MAX_ATTEMPTS, 2); //reduce the no of attempts

    Job job = new Job(mrCluster.getConfig());

    job.setJarByClass(FailingMapper.class);
    job.setJobName("failmapper");

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setInputFormatClass(RandomInputFormat.class);
    job.setMapperClass(FailingMapper.class);

    job.setOutputFormatClass(TextOutputFormat.class);
    job.setNumReduceTasks(0);
    
    FileOutputFormat.setOutputPath(job, new Path(mrCluster.getTestWorkDir().getAbsolutePath(),
        "failmapper-output"));
    // TODO: We should not be setting MRAppJar as job.jar. It should be
    // uploaded separately by YarnRunner.
    job.setJar(new File(MiniMRYarnCluster.APPJAR).getAbsolutePath());
    job.waitForCompletion(true);
    TaskID taskID = new TaskID(job.getJobID(), TaskType.MAP, 0);
    TaskAttemptID aId = new TaskAttemptID(taskID, 0);
    System.out.println("Diagnostics for " + aId + " :");
    for (String diag : job.getTaskDiagnostics(aId)) {
      System.out.println(diag);
    }
    aId = new TaskAttemptID(taskID, 1);
    System.out.println("Diagnostics for " + aId + " :");
    for (String diag : job.getTaskDiagnostics(aId)) {
      System.out.println(diag);
    }
    
    TaskCompletionEvent[] events = job.getTaskCompletionEvents(0, 2);
    Assert.assertEquals(TaskCompletionEvent.Status.FAILED, 
        events[0].getStatus().FAILED);
    Assert.assertEquals(TaskCompletionEvent.Status.FAILED, 
        events[1].getStatus().FAILED);
    Assert.assertEquals(JobStatus.State.FAILED, job.getJobState());
  }

//@Test
  public void testSleepJobWithSecurityOn() throws IOException,
      InterruptedException, ClassNotFoundException {

    if (!(new File(MiniMRYarnCluster.APPJAR)).exists()) {
      return;
    }

    mrCluster.getConfig().set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        "kerberos");
    mrCluster.getConfig().set(RMConfig.RM_KEYTAB, "/etc/krb5.keytab");
    mrCluster.getConfig().set(NMConfig.NM_KEYTAB, "/etc/krb5.keytab");
    mrCluster.getConfig().set(YarnConfiguration.RM_SERVER_PRINCIPAL_KEY,
        "rm/sightbusy-lx@LOCALHOST");
    mrCluster.getConfig().set(YarnServerConfig.NM_SERVER_PRINCIPAL_KEY,
        "nm/sightbusy-lx@LOCALHOST");
    UserGroupInformation.setConfiguration(mrCluster.getConfig());

    // Keep it in here instead of after RM/NM as multiple user logins happen in
    // the same JVM.
    UserGroupInformation user = UserGroupInformation.getCurrentUser();

    LOG.info("User name is " + user.getUserName());
    for (Token<? extends TokenIdentifier> str : user.getTokens()) {
      LOG.info("Token is " + str.encodeToUrlString());
    }
    user.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {  
        SleepJob sleepJob = new SleepJob();
        sleepJob.setConf(mrCluster.getConfig());
        Job job = sleepJob.createJob(3, 0, 10000, 1, 0, 0);
        // //Job with reduces
        // Job job = sleepJob.createJob(3, 2, 10000, 1, 10000, 1);
        // TODO: We should not be setting MRAppJar as job.jar. It should be
        // uploaded separately by YarnRunner.
        job.setJar(new File(MiniMRYarnCluster.APPJAR).getAbsolutePath());
        job.waitForCompletion(true);
        Assert.assertEquals(JobStatus.State.SUCCEEDED, job.getJobState());
        return null;
      }
    });
  }

}
