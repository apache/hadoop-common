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

package org.apache.hadoop.mapreduce.v2.util;

import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.ApplicationID;
import org.apache.hadoop.mapreduce.v2.api.JobID;
import org.apache.hadoop.mapreduce.v2.api.TaskID;
import org.apache.hadoop.mapreduce.v2.api.TaskAttemptID;
import org.apache.hadoop.mapreduce.v2.api.TaskType;

import org.junit.Test;
import static org.junit.Assert.*;

public class TestMRApps {

  @Test public void testJobIDtoString() {
    JobID jid = new JobID();
    jid.appID = new ApplicationID();
    assertEquals("job_0_0_0", MRApps.toString(jid));
  }

  @Test public void testToJobID() {
    JobID jid = MRApps.toJobID("job_1_1_1");
    assertEquals(1, jid.appID.clusterTimeStamp);
    assertEquals(1, jid.appID.id);
    assertEquals(1, jid.id);
  }

  @Test(expected=YarnException.class) public void testJobIDShort() {
    MRApps.toJobID("job_0_0");
  }

  @Test public void testTaskIDtoString() {
    TaskID tid = new TaskID();
    tid.jobID = new JobID();
    tid.jobID.appID = new ApplicationID();
    tid.taskType = TaskType.MAP;
    assertEquals("task_0_0_0_m_0", MRApps.toString(tid));
    tid.taskType = TaskType.REDUCE;
    assertEquals("task_0_0_0_r_0", MRApps.toString(tid));
  }

  @Test public void testToTaskID() {
    TaskID tid = MRApps.toTaskID("task_1_2_3_r_4");
    assertEquals(1, tid.jobID.appID.clusterTimeStamp);
    assertEquals(2, tid.jobID.appID.id);
    assertEquals(3, tid.jobID.id);
    assertEquals(TaskType.REDUCE, tid.taskType);
    assertEquals(4, tid.id);

    tid = MRApps.toTaskID("task_1_2_3_m_4");
    assertEquals(TaskType.MAP, tid.taskType);
  }

  @Test(expected=YarnException.class) public void testTaskIDShort() {
    MRApps.toTaskID("task_0_0_0_m");
  }

  @Test(expected=YarnException.class) public void testTaskIDBadType() {
    MRApps.toTaskID("task_0_0_0_x_0");
  }

  @Test public void testTaskAttemptIDtoString() {
    TaskAttemptID taid = new TaskAttemptID();
    taid.taskID = new TaskID();
    taid.taskID.taskType = TaskType.MAP;
    taid.taskID.jobID = new JobID();
    taid.taskID.jobID.appID = new ApplicationID();
    assertEquals("attempt_0_0_0_m_0_0", MRApps.toString(taid));
  }

  @Test public void testToTaskAttemptID() {
    TaskAttemptID taid = MRApps.toTaskAttemptID("attempt_0_1_2_m_3_4");
    assertEquals(0, taid.taskID.jobID.appID.clusterTimeStamp);
    assertEquals(1, taid.taskID.jobID.appID.id);
    assertEquals(2, taid.taskID.jobID.id);
    assertEquals(3, taid.taskID.id);
    assertEquals(4, taid.id);
  }

  @Test(expected=YarnException.class) public void testTaskAttemptIDShort() {
    MRApps.toTaskAttemptID("attempt_0_0_0_m_0");
  }
}
