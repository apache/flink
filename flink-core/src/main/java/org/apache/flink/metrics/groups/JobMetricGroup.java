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
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.MetricRegistry;
import org.apache.flink.util.AbstractID;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.metrics.groups.TaskManagerMetricGroup.DEFAULT_SCOPE_TM;

/**
 * Special {@link org.apache.flink.metrics.MetricGroup} representing a Job.
 * <p<
 * Contains extra logic for adding tasks.
 */
@Internal
public class JobMetricGroup extends ComponentMetricGroup {
	public static final String SCOPE_JOB_DESCRIPTOR = "job";
	public static final String SCOPE_JOB_ID = Scope.format("job_id");
	public static final String SCOPE_JOB_NAME = Scope.format("job_name");
	public static final String DEFAULT_SCOPE_JOB_COMPONENT = Scope.concat(SCOPE_JOB_NAME);
	public static final String DEFAULT_SCOPE_JOB = Scope.concat(DEFAULT_SCOPE_TM, DEFAULT_SCOPE_JOB_COMPONENT);

	private Map<AbstractID, TaskMetricGroup> tasks = new HashMap<>();

	public JobMetricGroup(MetricRegistry registry, TaskManagerMetricGroup taskManager, JobID id, String name) {
		super(registry, taskManager, registry.getScopeConfig().getJobFormat());
		this.formats.put(SCOPE_JOB_ID, id.toString());
		this.formats.put(SCOPE_JOB_NAME, name);
	}

	public TaskMetricGroup addTask(AbstractID id, AbstractID attemptID, int subtaskIndex, String name) {
		TaskMetricGroup task = new TaskMetricGroup(this.registry, this, id, attemptID, subtaskIndex, name);
		tasks.put(id, task);
		return task;
	}

	@Override
	public void close() {
		super.close();
		for (MetricGroup group : tasks.values()) {
			group.close();
		}
		tasks.clear();
	}

	@Override
	protected String getScopeFormat(Scope.ScopeFormat format) {
		return format.getJobFormat();
	}
}
