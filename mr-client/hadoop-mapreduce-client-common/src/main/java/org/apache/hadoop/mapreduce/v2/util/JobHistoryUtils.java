package org.apache.hadoop.mapreduce.v2.util;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.YarnMRJobConfig;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.yarn.conf.YARNApplicationConstants;

public class JobHistoryUtils {
  public static final FsPermission HISTORY_DIR_PERMISSION =
    FsPermission.createImmutable((short) 0750); // rwxr-x---
  
  public static final FsPermission HISTORY_FILE_PERMISSION =
    FsPermission.createImmutable((short) 0740); // rwxr-----
  
  public static final String CONF_FILE_NAME_SUFFIX = "_conf.xml";
  
  public static final String JOB_HISTORY_FILE_EXTENSION = ".jhist";
  
  public static final int VERSION = 4;

  public static final String LOG_VERSION_STRING = "version-" + VERSION;

  
  public static String getCurrentDoneDir(String doneDirPrefix) {
    return doneDirPrefix + File.separator + LOG_VERSION_STRING + File.separator;
  }

  public static String getConfiguredHistoryLogDirPrefix(Configuration conf) {
    String defaultLogDir = conf.get(
        YARNApplicationConstants.APPS_STAGING_DIR_KEY) + "/history/staging";
    String logDir = conf.get(YarnMRJobConfig.HISTORY_STAGING_DIR_KEY,
      defaultLogDir);
    return logDir;
  }
  
  public static String getConfiguredHistoryDoneDirPrefix(Configuration conf) {
    String defaultDoneDir = conf.get(
        YARNApplicationConstants.APPS_STAGING_DIR_KEY) + "/history/done";
    String  doneDirPrefix =
      conf.get(YarnMRJobConfig.HISTORY_DONE_DIR_KEY,
          defaultDoneDir);
    return doneDirPrefix;
  }
  
  /**
   * Get the job history file path
   */
  public static Path getJobHistoryFile(Path dir, JobId jobId, int attempt) {
    return getJobHistoryFile(dir, TypeConverter.fromYarn(jobId).toString(), attempt);
  }
  
  /**
   * Get the job history file path
   */
  public static Path getJobHistoryFile(Path dir, String jobId, int attempt) {
    return new Path(dir, jobId + "_" + 
        attempt + JOB_HISTORY_FILE_EXTENSION);
  }
}
