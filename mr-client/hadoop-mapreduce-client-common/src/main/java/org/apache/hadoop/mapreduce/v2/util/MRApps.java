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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.ApplicationID;
import org.apache.hadoop.mapreduce.v2.api.JobID;
import org.apache.hadoop.mapreduce.v2.api.TaskAttemptID;
import org.apache.hadoop.mapreduce.v2.api.TaskID;
import org.apache.hadoop.mapreduce.v2.api.TaskType;

import static org.apache.hadoop.yarn.util.StringHelper.*;

/**
 * Helper class for MR applications
 */
public class MRApps extends Apps {
  public static final String JOB = "job";
  public static final String TASK = "task";
  public static final String ATTEMPT = "attempt";

  public static String toString(JobID jid) {
    return _join(JOB, jid.appID.clusterTimeStamp, jid.appID.id, jid.id);
  }

  public static JobID toJobID(String jid) {
    Iterator<String> it = _split(jid).iterator();
    return toJobID(JOB, jid, it);
  }

  // mostly useful for parsing task/attempt id like strings
  public static JobID toJobID(String prefix, String s, Iterator<String> it) {
    ApplicationID appID = toAppID(prefix, s, it);
    shouldHaveNext(prefix, s, it);
    JobID jobID = new JobID();
    jobID.appID = appID;
    jobID.id = Integer.parseInt(it.next());
    return jobID;
  }

  public static String toString(TaskID tid) {
    return _join("task", tid.jobID.appID.clusterTimeStamp, tid.jobID.appID.id,
                 tid.jobID.id, taskSymbol(tid.taskType), tid.id);
  }

  public static TaskID toTaskID(String tid) {
    Iterator<String> it = _split(tid).iterator();
    return toTaskID(TASK, tid, it);
  }

  public static TaskID toTaskID(String prefix, String s, Iterator<String> it) {
    JobID jid = toJobID(prefix, s, it);
    shouldHaveNext(prefix, s, it);
    TaskID tid = new TaskID();
    tid.jobID = jid;
    tid.taskType = taskType(it.next());
    shouldHaveNext(prefix, s, it);
    tid.id = Integer.parseInt(it.next());
    return tid;
  }

  public static String toString(TaskAttemptID taid) {
    return _join("attempt", taid.taskID.jobID.appID.clusterTimeStamp,
                 taid.taskID.jobID.appID.id, taid.taskID.jobID.id,
                 taskSymbol(taid.taskID.taskType), taid.taskID.id, taid.id);
  }

  public static TaskAttemptID toTaskAttemptID(String taid) {
    Iterator<String> it = _split(taid).iterator();
    TaskID tid = toTaskID(ATTEMPT, taid, it);
    shouldHaveNext(ATTEMPT, taid, it);
    TaskAttemptID taID = new TaskAttemptID();
    taID.taskID = tid;
    taID.id = Integer.parseInt(it.next());
    return taID;
  }

  public static String taskSymbol(TaskType type) {
    switch (type) {
      case MAP:           return "m";
      case REDUCE:        return "r";
    }
    throw new YarnException("Unknown task type: "+ type.toString());
  }

  public static TaskType taskType(String symbol) {
    // JDK 7 supports switch on strings
    if (symbol.equals("m")) return TaskType.MAP;
    if (symbol.equals("r")) return TaskType.REDUCE;
    throw new YarnException("Unknown task symbol: "+ symbol);
  }

  public static void setInitialClasspath(
      Map<CharSequence, CharSequence> environment) throws IOException {

    // Get yarn mapreduce-app classpath from generated classpath
    // Works if compile time env is same as runtime. For e.g. tests.
    InputStream classpathFileStream =
        Thread.currentThread().getContextClassLoader()
            .getResourceAsStream("mrapp-generated-classpath");
    BufferedReader reader =
        new BufferedReader(new InputStreamReader(classpathFileStream));
    addToClassPath(environment, reader.readLine().trim());

    // If runtime env is different.
    if (System.getenv().get("YARN_HOME") != null) {
      ShellCommandExecutor exec =
         new ShellCommandExecutor(new String[] {
              System.getenv().get("YARN_HOME") + "/bin/yarn",
              "classpath" });
      exec.execute();
      addToClassPath(environment, exec.getOutput().trim());
    }

    // Get yarn mapreduce-app classpath
    if (System.getenv().get("HADOOP_MAPRED_HOME")!= null) {
      ShellCommandExecutor exec =
          new ShellCommandExecutor(new String[] {
              System.getenv().get("HADOOP_MAPRED_HOME") + "/bin/mapred",
              "classpath" });
      exec.execute();
      addToClassPath(environment, exec.getOutput().trim());
    }

    // TODO: Remove duplicates.
  }

  public static void addToClassPath(
      Map<CharSequence, CharSequence> environment, String fileName) {
    CharSequence classpath = environment.get(CLASSPATH);
    if (classpath == null) {
      classpath = fileName;
    } else {
      classpath = classpath + ":" + fileName;
    }
    environment.put(CLASSPATH, classpath);
  }

  public static final String CLASSPATH = "CLASSPATH";
}
