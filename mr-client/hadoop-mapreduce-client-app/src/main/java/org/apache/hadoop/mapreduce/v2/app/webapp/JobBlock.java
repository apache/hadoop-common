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

package org.apache.hadoop.mapreduce.v2.app.webapp;

import com.google.inject.Inject;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobReport;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hadoop.yarn.webapp.view.InfoBlock;
import static org.apache.hadoop.mapreduce.v2.app.webapp.AMWebApp.*;
import static org.apache.hadoop.yarn.util.StringHelper.*;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.*;

public class JobBlock extends HtmlBlock {
  final AppContext appContext;
  int runningMaps = 0;
  int pendingMaps = 0;
  int runningReds = 0;
  int pendingReds = 0;

  @Inject JobBlock(AppContext appctx) {
    appContext = appctx;
  }

  @Override protected void render(Block html) {
    String jid = $(JOB_ID);
    if (jid.isEmpty()) {
      html.
        p()._("Sorry, can't do anything without a JobID.")._();
      return;
    }
    JobId jobID = MRApps.toJobID(jid);
    Job job = appContext.getJob(jobID);
    if (job == null) {
      html.
        p()._("Sorry, ", jid, " not found.")._();
      return;
    }
    JobReport jobReport = job.getReport();
    String mapPct = percent(jobReport.getMapProgress());
    String reducePct = percent(jobReport.getReduceProgress());
    int maps = job.getTotalMaps();
    int mapsComplete = job.getCompletedMaps();
    int reduces = job.getTotalReduces();
    int reducesComplete = job.getCompletedReduces();
    countTasks(job);
    info("Job Overview").
        _("Job Name:", job.getName()).
        _("State:", job.getState()).
        _("Started:", new Date(jobReport.getStartTime())).
        _("Elapsed:", StringUtils.formatTime(System.currentTimeMillis()
                                             - jobReport.getStartTime()));
    html.
      _(InfoBlock.class).
      div(_INFO_WRAP).
        table("#job").
          tr().
            th(_TH, "Task Type").
            th(_TH, "Progress").
            th(_TH, "Total").
            th(_TH, "Pending").
            th(_TH, "Running").
            th(_TH, "Complete")._().
          tr(_ODD).
            th().
              a(url("tasks", jid, "m"), "Map")._().
            td().
              div(_PROGRESSBAR).
                $title(join(mapPct, '%')). // tooltip
                div(_PROGRESSBAR_VALUE).
                  $style(join("width:", mapPct, '%'))._()._()._().
            td(String.valueOf(maps)).
            td(String.valueOf(pendingMaps)).
            td(String.valueOf(runningMaps)).
            td(String.valueOf(mapsComplete))._().
          tr(_EVEN).
            th().
              a(url("tasks", jid, "r"), "Reduce")._().
            td().
              div(_PROGRESSBAR).
                $title(join(reducePct, '%')). // tooltip
                div(_PROGRESSBAR_VALUE).
                  $style(join("width:", reducePct, '%'))._()._()._().
            td(String.valueOf(reduces)).
            td(String.valueOf(pendingReds)).
            td(String.valueOf(runningReds)).
            td(String.valueOf(reducesComplete))._()._()._();
  }

  private void countTasks(Job job) {
    Map<TaskId, Task> tasks = job.getTasks();
    for (Task task : tasks.values()) {
      switch (task.getType()) {
        case MAP: switch (task.getState()) {
          case RUNNING:   ++runningMaps;  break;
          case SCHEDULED: ++pendingMaps;  break;
        } break;
        case REDUCE: switch(task.getState()) {
          case RUNNING:   ++runningReds;  break;
          case SCHEDULED: ++pendingReds;  break;
        } break;
      }
    }
  }
}
