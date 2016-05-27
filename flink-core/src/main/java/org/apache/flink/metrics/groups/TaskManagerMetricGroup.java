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

package org.apache.flink.metrics.groups;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.metrics.MetricRegistry;
import org.apache.flink.util.AbstractID;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Special {@link org.apache.flink.metrics.MetricGroup} representing a TaskManager.
 *
 * <p>Contains extra logic for adding jobs with tasks, and removing jobs when they do
 * not contain tasks any more
 */
@Internal
public class TaskManagerMetricGroup extends ComponentMetricGroup {

	public static final String SCOPE_HOST_DESCRIPTOR = "host";
	public static final String SCOPE_TM_DESCRIPTOR = "taskmanager";
	public static final String SCOPE_TM_HOST = Scope.format("host");
	public static final String SCOPE_TM_ID = Scope.format("tm_id");
	public static final String DEFAULT_SCOPE_TM_COMPONENT = Scope.concat(SCOPE_TM_HOST, "taskmanager", SCOPE_TM_ID);
	public static final String DEFAULT_SCOPE_TM = DEFAULT_SCOPE_TM_COMPONENT;

	// ------------------------------------------------------------------------
	
	private final Map<JobID, JobMetricGroup> jobs = new HashMap<>();

	public TaskManagerMetricGroup(MetricRegistry registry, String host, String taskManagerId) {
		super(registry, null, registry.getScopeConfig().getTaskManagerFormat());

		this.formats.put(SCOPE_TM_HOST, checkNotNull(host));
		this.formats.put(SCOPE_TM_ID, checkNotNull(taskManagerId));
	}

	// ------------------------------------------------------------------------
	//  job groups
	// ------------------------------------------------------------------------

	public TaskMetricGroup addTaskForJob(
			JobID jobId,
			String jobName,
			AbstractID vertexID,
			AbstractID executionId,
			int subtaskIndex,
			String taskName) {
		
		// we cannot strictly lock both our map modification and the job group modification
		// because it might lead to a deadlock
		while (true) {
			// get or create a jobs metric group
			JobMetricGroup currentJobGroup;
			synchronized (this) {
				currentJobGroup = jobs.get(jobId);
				
				if (currentJobGroup == null || currentJobGroup.isClosed()) {
					currentJobGroup = new JobMetricGroup(registry, this, jobId, jobName);
					jobs.put(jobId, currentJobGroup);
				}
			}
		
			// try to add another task. this may fail if we found a pre-existing job metrics
			// group and it is closed concurrently
			TaskMetricGroup taskGroup = currentJobGroup.addTask(vertexID, executionId, subtaskIndex, taskName);
			if (taskGroup != null) {
				// successfully added the next task
				return taskGroup;
			}
			
			// else fall through the loop
		}
	}
	
	public void removeJobMetricsGroup(JobID jobId, JobMetricGroup group) {
		if (jobId == null || group == null || !group.isClosed()) {
			return;
		}
		
		synchronized (this) {
			// optimistically remove the currently contained group, and check later if it was correct
			JobMetricGroup containedGroup = jobs.remove(jobId);
			
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
	//  component group behavior
	// ------------------------------------------------------------------------

	@Override
	protected String getScopeFormat(Scope.ScopeFormat format) {
		return format.getTaskManagerFormat();
	}

	@Override
	protected Iterable<? extends ComponentMetricGroup> subComponents() {
		return jobs.values();
	}
}

