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

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.v2.YarnMRJobConfig;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.conf.YARNApplicationConstants;
import org.apache.hadoop.yarn.service.AbstractService;

public class HistoryCleanerService extends AbstractService {

  private static final Log LOG = LogFactory.getLog(HistoryClientService.class);
  
  static final long DEFAULT_HISTORY_MAX_AGE = 7 * 24 * 60 * 60 * 1000L;
  private FileContext doneDirFc;
  private HistoryCleaner historyCleanerThread = null;

  private Configuration conf;

  public HistoryCleanerService(Configuration conf) {
    super("HistoryCleanerService");
    this.conf = conf;
  }

  public void start() {
    long maxAgeOfHistoryFiles = conf.getLong(
        YarnMRJobConfig.HISTORY_MAXAGE, DEFAULT_HISTORY_MAX_AGE);
    historyCleanerThread  = new HistoryCleaner(maxAgeOfHistoryFiles);
    historyCleanerThread.start();
    super.start();
  }

  /** Shut down JobHistory after stopping the History cleaner */
  @Override
  public void stop() {
    LOG.info("Interrupting History Cleaner");
    historyCleanerThread.interrupt();
    try {
      historyCleanerThread.join();
    } catch (InterruptedException e) {
      LOG.info("Error with shutting down history cleaner thread");
    }
  }
  /**
   * Delete history files older than a specified time duration.
   */
  class HistoryCleaner extends Thread {
    static final long ONE_DAY_IN_MS = 7 * 24 * 60 * 60 * 1000L;
    private long cleanupFrequency;
    private long maxAgeOfHistoryFiles;

    public HistoryCleaner(long maxAge) {
      setName("Thread for cleaning up History files");
      setDaemon(true);
      this.maxAgeOfHistoryFiles = maxAge;
      cleanupFrequency = Math.min(ONE_DAY_IN_MS, maxAgeOfHistoryFiles);
      LOG.info("Job History Cleaner Thread started." +
          " MaxAge is " + 
          maxAge + " ms(" + ((float)maxAge)/(ONE_DAY_IN_MS) + " days)," +
          " Cleanup Frequency is " +
          + cleanupFrequency + " ms (" +
          ((float)cleanupFrequency)/ONE_DAY_IN_MS + " days)");
    }

    @Override
    public void run(){
  
      while (true) {
        try {
          doCleanup(); 
          Thread.sleep(cleanupFrequency);
        }
        catch (InterruptedException e) {
          LOG.info("History Cleaner thread exiting");
          return;
        }
        catch (Throwable t) {
          LOG.warn("History cleaner thread threw an exception", t);
        }
      }
    }

    private void doCleanup() {
      long now = System.currentTimeMillis();
      try {
        String defaultDoneDir = conf.get(
            YARNApplicationConstants.APPS_STAGING_DIR_KEY) + "/history/done";
        String  jobhistoryDir =
          conf.get(YarnMRJobConfig.HISTORY_DONE_DIR_KEY, defaultDoneDir);
        Path done = FileContext.getFileContext(conf).makeQualified(
            new Path(jobhistoryDir));
        doneDirFc = FileContext.getFileContext(done.toUri(), conf);
        RemoteIterator<LocatedFileStatus> historyFiles =
          doneDirFc.util().listFiles(done, true);
        if (historyFiles != null) {
          FileStatus f;
          while (historyFiles.hasNext()) {
            f = historyFiles.next();
            if (now - f.getModificationTime() > maxAgeOfHistoryFiles) {
              doneDirFc.delete(f.getPath(), true); 
              LOG.info("Deleting old history file : " + f.getPath());
            }
          }
        }
      } catch (IOException ie) {
        LOG.info("Error cleaning up history directory" + 
            StringUtils.stringifyException(ie));
      }
    }
  }
}
