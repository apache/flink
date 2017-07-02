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
import org.apache.flink.api.common.JobID;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.metrics.scope.ScopeFormat;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Map;

/**
 * Special {@link org.apache.flink.metrics.MetricGroup} representing a TaskManager.
 *
 * <p>Contains extra logic for adding jobs with tasks, and removing jobs when they do
 * not contain tasks any more
 */
@Internal
public class TaskManagerMetricGroup extends ComponentMetricGroup<TaskManagerMetricGroup> {

	private final Map<JobID, TaskManagerJobMetricGroup> jobs = new HashMap<>();

	private final String hostname;

	private final String taskManagerId;

	public TaskManagerMetricGroup(MetricRegistry registry, String hostname, String taskManagerId) {
		super(registry, registry.getScopeFormats().getTaskManagerFormat().formatScope(hostname, taskManagerId), null);
		this.hostname = hostname;
		this.taskManagerId = taskManagerId;
	}

	public String hostname() {
		return hostname;
	}

	public String taskManagerId() {
		return taskManagerId;
	}

	@Override
	protected QueryScopeInfo.TaskManagerQueryScopeInfo createQueryServiceMetricInfo(CharacterFilter filter) {
		return new QueryScopeInfo.TaskManagerQueryScopeInfo(this.taskManagerId);
	}

	// ------------------------------------------------------------------------
	//  job groups
	// ------------------------------------------------------------------------

	public TaskMetricGroup addTaskForJob(
			final JobID jobId,
			final String jobName,
			final JobVertexID jobVertexId,
			final ExecutionAttemptID executionAttemptId,
			final String taskName,
			final int subtaskIndex,
			final int attemptNumber) {
		Preconditions.checkNotNull(jobId);

		String resolvedJobName = jobName == null || jobName.isEmpty()
			? jobId.toString()
			: jobName;

		// we cannot strictly lock both our map modification and the job group modification
		// because it might lead to a deadlock
		while (true) {
			// get or create a jobs metric group
			TaskManagerJobMetricGroup currentJobGroup;
			synchronized (this) {
				currentJobGroup = jobs.get(jobId);

				if (currentJobGroup == null || currentJobGroup.isClosed()) {
					currentJobGroup = new TaskManagerJobMetricGroup(registry, this, jobId, resolvedJobName);
					jobs.put(jobId, currentJobGroup);
				}
			}

			// try to add another task. this may fail if we found a pre-existing job metrics
			// group and it is closed concurrently
			TaskMetricGroup taskGroup = currentJobGroup.addTask(
				jobVertexId,
				executionAttemptId,
				taskName,
				subtaskIndex,
				attemptNumber);

			if (taskGroup != null) {
				// successfully added the next task
				return taskGroup;
			}

			// else fall through the loop
		}
	}

	public void removeJobMetricsGroup(JobID jobId, TaskManagerJobMetricGroup group) {
		if (jobId == null || group == null || !group.isClosed()) {
			return;
		}

		synchronized (this) {
			// optimistically remove the currently contained group, and check later if it was correct
			TaskManagerJobMetricGroup containedGroup = jobs.remove(jobId);

			// check if another group was actually contained, and restore that one
			if (containedGroup != null && containedGroup != group) {
				jobs.put(jobId, containedGroup);
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

