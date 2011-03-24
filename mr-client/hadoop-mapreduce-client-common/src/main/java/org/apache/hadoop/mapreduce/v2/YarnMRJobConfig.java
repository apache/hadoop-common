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
  public static final String SPECULATOR_CLASS
      = "yarn.mapreduce.job.speculator.class";
  public static final String TASK_RUNTIME_ESTIMATOR_CLASS
      = "yarn.mapreduce.job.task.runtime.estimator.class";
  public static final String TASK_ATTEMPT_PROGRESS_RUNTIME_LINEARIZER_CLASS
      = "yarn.mapreduce.job.task.runtime.linearizer.class";
  public static final String EXPONENTIAL_SMOOTHING_LAMBDA_MILLISECONDS
      = "yarn.mapreduce.job.task.runtime.estimator.exponential.smooth.lambda";
  public static final String EXPONENTIAL_SMOOTHING_SMOOTH_RATE
      = "yarn.mapreduce.job.task.runtime.estimator.exponential.smooth.smoothsrate";
  public static final String HS_PREFIX = "yarn.server.historyserver.";

  public static final String DEFAULT_HS_BIND_ADDRESS = "0.0.0.0:10020";

  /** host:port address to which to bind to **/
  public static final String HS_BIND_ADDRESS = HS_PREFIX + "address";

  public static final String HISTORY_STAGING_DIR_KEY =
       "yarn.historyfile.stagingDir";

  public static final String HISTORY_DONE_DIR_KEY =
       "yarn.historyfile.doneDir";
}
