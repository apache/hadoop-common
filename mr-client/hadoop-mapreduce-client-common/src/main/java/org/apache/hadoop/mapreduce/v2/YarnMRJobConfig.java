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


import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class YarnMRJobConfig {
  public static final String YARN_MR_PREFIX = "yarn.mapreduce.job.";
  public static final String SPECULATOR_CLASS
      = YARN_MR_PREFIX + "speculator.class";
  public static final String TASK_RUNTIME_ESTIMATOR_CLASS
      = YARN_MR_PREFIX + "task.runtime.estimator.class";
  public static final String TASK_ATTEMPT_PROGRESS_RUNTIME_LINEARIZER_CLASS
      = YARN_MR_PREFIX + "task.runtime.linearizer.class";
  public static final String EXPONENTIAL_SMOOTHING_LAMBDA_MILLISECONDS
      = YARN_MR_PREFIX + "task.runtime.estimator.exponential.smooth.lambda";
  public static final String EXPONENTIAL_SMOOTHING_SMOOTH_RATE
      = YARN_MR_PREFIX + "task.runtime.estimator.exponential.smooth.smoothsrate";
  public static final String RECOVERY_ENABLE
      = YARN_MR_PREFIX + "recovery.enable";
  
  public static final String AM_TASK_LISTENER_THREADS =
    YARN_MR_PREFIX + "task.listener.threads";
  public static final int DEFAULT_AM_TASK_LISTENER_THREADS = 10;

  public static final String AM_JOB_CLIENT_THREADS =
    YARN_MR_PREFIX + "job.client.threads";
  public static final int DEFAULT_AM_JOB_CLIENT_THREADS = 1;
}
