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

  /** Staging Dir for AppMaster **/
  public static final String HISTORY_STAGING_DIR_KEY =
       "yarn.historyfile.stagingDir";

  /** Done Dir for for AppMaster **/
  public static final String HISTORY_INTERMEDIATE_DONE_DIR_KEY =
       "yarn.historyfile.intermediateDoneDir";
  
  /** Done Dir for for AppMaster **/
  public static final String HISTORY_DONE_DIR_KEY =
       "yarn.historyfile.doneDir";
  
  /** Done Dir for history server. **/
  public static final String HISTORY_SERVER_DONE_DIR_KEY = 
       HS_PREFIX + ".historyfile.doneDir";
  
  /**
   * Size of the job list cache.
   */
  public static final String HISTORY_SERVER_JOBLIST_CACHE_SIZE_KEY =
    HS_PREFIX + ".joblist.cache.size";
     
  /**
   * Size of the loaded job cache.
   */
  public static final String HISTORY_SERVER_LOADED_JOB_CACHE_SIZE_KEY = 
    HS_PREFIX + ".loadedjobs.cache.size";
  
  /**
   * Size of the date string cache. Effects the number of directories
   * which will be scanned to find a job.
   */
  public static final String HISTORY_SERVER_DATESTRING_CACHE_SIZE_KEY = 
    HS_PREFIX + ".datestring.cache.size";
  
  /**
   * The time interval in milliseconds for the history server
   * to wake up and scan for files to be moved.
   */
  public static final String HISTORY_SERVER_MOVE_THREAD_INTERVAL = 
    HS_PREFIX + ".move.thread.interval";
  
  /**
   * The number of threads used to move files.
   */
  public static final String HISTORY_SERVER_NUM_MOVE_THREADS = 
    HS_PREFIX + ".move.threads.count";
  
  // Equivalent to 0.20 mapreduce.jobhistory.debug.mode
  public static final String HISTORY_DEBUG_MODE_KEY = HS_PREFIX + ".debug.mode";
  
  public static final String HISTORY_MAXAGE =
	  "yarn.historyfile.maxage";
  
  /**
   * Run interval for the History Cleaner thread.
   */
  public static final String HISTORY_CLEANER_RUN_INTERVAL = 
    HS_PREFIX + ".cleaner.run.interval";
  
  public static final String HS_WEBAPP_BIND_ADDRESS = HS_PREFIX +
      "address.webapp";
  public static final String DEFAULT_HS_WEBAPP_BIND_ADDRESS =
	  "0.0.0.0:19888";

  public static final String RECOVERY_ENABLE
      = "yarn.mapreduce.job.recovery.enable";
}
