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

package org.apache.hadoop.mapreduce.v2.hs;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.YarnMRJobConfig;
import org.apache.hadoop.mapreduce.v2.hs.JobHistory.HistoryCleaner;
import org.apache.hadoop.yarn.service.AbstractService;

public class HistoryCleanerService extends AbstractService {

  private static final Log LOG = LogFactory.getLog(HistoryClientService.class);
  
  static final long DEFAULT_HISTORY_MAX_AGE = 7 * 24 * 60 * 60 * 1000L; //1 week
  static final long DEFAULT_RUN_INTERVAL = 1 * 24 * 60 * 60 * 1000l; //1 day
  
  private ScheduledThreadPoolExecutor scheduledExecutor = null;

  private Configuration conf;

  public HistoryCleanerService(Configuration conf) {
    super("HistoryCleanerService");
    this.conf = conf;
  }

  public void start() {
//    long maxAgeOfHistoryFiles = conf.getLong(
//        YarnMRJobConfig.HISTORY_MAXAGE, DEFAULT_HISTORY_MAX_AGE);
//    scheduledExecutor = new ScheduledThreadPoolExecutor(1);
//    long runInterval = conf.getLong(YarnMRJobConfig.HISTORY_CLEANER_RUN_INTERVAL, DEFAULT_RUN_INTERVAL);
//    HistoryCleaner c;
//    scheduledExecutor.scheduleAtFixedRate(new HistoryCleaner(maxAgeOfHistoryFiles), 30*1000l, runInterval, TimeUnit.MILLISECONDS);
//    super.start();
  }

  /** Shut down JobHistory after stopping the History cleaner */
  @Override
  public void stop() {
//    LOG.info("Interrupting History Cleaner");
//    scheduledExecutor.shutdown();
//    boolean interrupted = false;
//    long currentTime = System.currentTimeMillis();
//    while (!scheduledExecutor.isShutdown() && System.currentTimeMillis() > currentTime + 1000l && !interrupted) {
//      try {
//        Thread.sleep(20);
//      } catch (InterruptedException e) {
//        interrupted = true;
//      }
//    }
//    if (!scheduledExecutor.isShutdown()) {
//      LOG.warn("HistoryCleanerService shutdown may not have succeeded");
//    }
    }

}
