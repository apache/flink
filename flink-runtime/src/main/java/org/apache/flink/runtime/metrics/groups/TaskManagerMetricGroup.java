/*
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

package org.apache.flink.runtime.metrics.groups;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.metrics.scope.ScopeFormat;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Map;

/**
 * Special {@link org.apache.flink.metrics.MetricGroup} representing a TaskManager.
 *
 * <p>Contains extra logic for adding jobs with tasks, and removing jobs when they do not contain
 * tasks any more
 */
@Internal
public class TaskManagerMetricGroup extends ComponentMetricGroup<TaskManagerMetricGroup> {

    private final Map<JobID, TaskManagerJobMetricGroup> jobs = new HashMap<>();

    private final String hostname;

    private final String taskManagerId;

    TaskManagerMetricGroup(MetricRegistry registry, String hostname, String taskManagerId) {
        super(
                registry,
                registry.getScopeFormats()
                        .getTaskManagerFormat()
                        .formatScope(hostname, taskManagerId),
                null);
        this.hostname = hostname;
        this.taskManagerId = taskManagerId;
    }

    public static TaskManagerMetricGroup createTaskManagerMetricGroup(
            MetricRegistry metricRegistry, String hostName, ResourceID resourceID) {
        return new TaskManagerMetricGroup(metricRegistry, hostName, resourceID.toString());
    }

    public String hostname() {
        return hostname;
    }

    public String taskManagerId() {
        return taskManagerId;
    }

    @Override
    protected QueryScopeInfo.TaskManagerQueryScopeInfo createQueryServiceMetricInfo(
            CharacterFilter filter) {
        return new QueryScopeInfo.TaskManagerQueryScopeInfo(this.taskManagerId);
    }

    // ------------------------------------------------------------------------
    //  job groups
    // ------------------------------------------------------------------------

    public TaskManagerJobMetricGroup addJob(JobID jobId, String jobName) {
        Preconditions.checkNotNull(jobId);
        String resolvedJobName = jobName == null || jobName.isEmpty() ? jobId.toString() : jobName;
        TaskManagerJobMetricGroup jobGroup;
        synchronized (this) { // synchronization isn't strictly necessary as of FLINK-24864
            jobGroup = jobs.get(jobId);
            if (jobGroup == null) {
                jobGroup = new TaskManagerJobMetricGroup(registry, this, jobId, resolvedJobName);
                jobs.put(jobId, jobGroup);
            }
        }
        return jobGroup;
    }

    @VisibleForTesting
    public TaskManagerJobMetricGroup getJobMetricsGroup(JobID jobId) {
        return jobs.get(jobId);
    }

    public void removeJobMetricsGroup(JobID jobId) {
        if (jobId != null) {
            TaskManagerJobMetricGroup groupToClose;
            synchronized (this) { // synchronization isn't strictly necessary as of FLINK-24864
                groupToClose = jobs.remove(jobId);
            }
            if (groupToClose != null) {
                groupToClose.close();
            }
        }
    }

    public int numRegisteredJobMetricGroups() {
        return jobs.size();
    }

    // ------------------------------------------------------------------------
    //  Component Metric Group Specifics
    // ------------------------------------------------------------------------

    @Override
    protected void putVariables(Map<String, String> variables) {
        variables.put(ScopeFormat.SCOPE_HOST, hostname);
        variables.put(ScopeFormat.SCOPE_TASKMANAGER_ID, taskManagerId);
    }

    @Override
    protected Iterable<? extends ComponentMetricGroup> subComponents() {
        return jobs.values();
    }

    @Override
    protected String getGroupName(CharacterFilter filter) {
        return "taskmanager";
    }
}
