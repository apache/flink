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
import org.apache.flink.metrics.groups.scope.ScopeFormat.TaskManagerJobScopeFormat;
import org.apache.flink.util.AbstractID;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Special {@link org.apache.flink.metrics.MetricGroup} representing everything belonging to
 * a specific job, running on the TaskManager.
 *
 * <p>Contains extra logic for adding Tasks ({@link TaskMetricGroup}).
 */
@Internal
public class TaskManagerJobMetricGroup extends JobMetricGroup {

	/** The metrics group that contains this group */
	private final TaskManagerMetricGroup parent;

	/** Map from execution attempt ID (task identifier) to task metrics */
	private final Map<AbstractID, TaskMetricGroup> tasks = new HashMap<>();

	// ------------------------------------------------------------------------

	public TaskManagerJobMetricGroup(
		MetricRegistry registry,
		TaskManagerMetricGroup parent,
		JobID jobId,
		@Nullable String jobName) {

		this(registry, checkNotNull(parent), registry.getScopeFormats().getTaskManagerJobFormat(), jobId, jobName);
	}

	public TaskManagerJobMetricGroup(
		MetricRegistry registry,
		TaskManagerMetricGroup parent,
		TaskManagerJobScopeFormat scopeFormat,
		JobID jobId,
		@Nullable String jobName) {

		super(registry, jobId, jobName, scopeFormat.formatScope(parent, jobId, jobName));

		this.parent = checkNotNull(parent);
	}

	public final TaskManagerMetricGroup parent() {
		return parent;
	}

	// ------------------------------------------------------------------------
	//  adding / removing tasks
	// ------------------------------------------------------------------------

	public TaskMetricGroup addTask(
		AbstractID vertexId,
		AbstractID executionId,
		String taskName,
		int subtaskIndex,
		int attemptNumber) {

		checkNotNull(executionId);

		synchronized (this) {
			if (!isClosed()) {
				TaskMetricGroup task = new TaskMetricGroup(registry, this,
					vertexId, executionId, taskName, subtaskIndex, attemptNumber);
				tasks.put(executionId, task);
				return task;
			} else {
				return null;
			}
		}
	}

	public void removeTaskMetricGroup(AbstractID executionId) {
		checkNotNull(executionId);

		boolean removeFromParent = false;
		synchronized (this) {
			if (!isClosed() && tasks.remove(executionId) != null && tasks.isEmpty()) {
				// this call removed the last task. close this group.
				removeFromParent = true;
				close();
			}
		}

		// IMPORTANT: removing from the parent must not happen while holding the this group's lock,
		//      because it would violate the "first parent then subgroup" lock acquisition order
		if (removeFromParent) {
			parent.removeJobMetricsGroup(jobId, this);
		}
	}

	@Override
	protected Iterable<? extends ComponentMetricGroup> subComponents() {
		return tasks.values();
	}
}
