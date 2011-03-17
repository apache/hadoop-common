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

package org.apache.hadoop.mapreduce.v2.app.taskclean;

import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.yarn.event.AbstractEvent;
import org.apache.hadoop.mapreduce.v2.api.TaskAttemptID;

/**
 * This class encapsulates task cleanup event.
 *
 */
public class TaskCleanupEvent extends AbstractEvent<TaskCleaner.EventType> {

  private final TaskAttemptID attemptID;
  private final OutputCommitter committer;
  private final TaskAttemptContext attemptContext;

  public TaskCleanupEvent(TaskAttemptID attemptID, OutputCommitter committer, 
      TaskAttemptContext attemptContext) {
    super(TaskCleaner.EventType.TASK_CLEAN);
    this.attemptID = attemptID;
    this.committer = committer;
    this.attemptContext = attemptContext;
  }

  public TaskAttemptID getAttemptID() {
    return attemptID;
  }

  public OutputCommitter getCommitter() {
    return committer;
  }

  public TaskAttemptContext getAttemptContext() {
    return attemptContext;
  }

}
