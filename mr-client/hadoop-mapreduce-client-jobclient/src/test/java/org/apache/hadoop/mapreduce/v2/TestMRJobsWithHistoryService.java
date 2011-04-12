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

import junit.framework.Assert;

import org.apache.avro.ipc.AvroRemoteException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.SleepJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationState;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;

public class TestMRJobsWithHistoryService {

  private static final Log LOG = LogFactory.getLog(TestMRJobs.class);

  private static MiniMRYarnCluster mrCluster;

  @Before
  public void setup() throws InterruptedException, IOException {

    if (!(new File(MiniMRYarnCluster.APPJAR)).exists()) {
      LOG.info("MRAppJar " + MiniMRYarnCluster.APPJAR
               + " not found. Not running test.");
      return;
    }

    if (mrCluster == null) {
      mrCluster = new MiniMRYarnCluster(getClass().getName());
      mrCluster.init(new Configuration());
      mrCluster.start();
    }
  }

  @After
  public void tearDown() {
    if (!(new File(MiniMRYarnCluster.APPJAR)).exists()) {
      LOG.info("MRAppJar " + MiniMRYarnCluster.APPJAR
          + " not found. Not running test.");
      return;
    }

    if (mrCluster != null) {
      mrCluster.stop();
    }
  }

  @Test
  public void testJobHistoryData() throws IOException, InterruptedException,
      AvroRemoteException, ClassNotFoundException {
    if (!(new File(MiniMRYarnCluster.APPJAR)).exists()) {
      LOG.info("MRAppJar " + MiniMRYarnCluster.APPJAR
          + " not found. Not running test.");
      return;
    }

    SleepJob sleepJob = new SleepJob();
    sleepJob.setConf(mrCluster.getConfig());
    // Job with 3 maps and 2 reduces
    Job job = sleepJob.createJob(3, 2, 1000, 1, 500, 1);
    job.setJar(new File(MiniMRYarnCluster.APPJAR).getAbsolutePath());
    job.waitForCompletion(true);
    Counters counterMR = job.getCounters();
    ApplicationId appID = TypeConverter.toYarn(job.getJobID()).getAppId();
    while (true) {
      Thread.sleep(1000);
      if (mrCluster.getResourceManager().getApplicationsManager()
          .getApplication(appID).getState().equals(ApplicationState.COMPLETED))
        break;
    }
    Counters counterHS = job.getCounters();
    //TODO the Assert below worked. need to check
    //Should we compare each field or convert to V2 counter and compare
    LOG.info("CounterHS " + counterHS);
    LOG.info("CounterMR " + counterMR);
    Assert.assertEquals(counterHS, counterMR);
  }

}
