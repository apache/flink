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

import java.util.HashMap;
import java.util.Map;

/**
 * Special {@link org.apache.flink.metrics.MetricGroup} representing a TaskManager.
 * <p<
 * Contains extra logic for adding jobs.
 */
@Internal
public class TaskManagerMetricGroup extends ComponentMetricGroup {
	public static final String SCOPE_HOST_DESCRIPTOR = "host";
	public static final String SCOPE_TM_DESCRIPTOR = "taskmanager";
	public static final String SCOPE_TM_HOST = Scope.format("host");
	public static final String SCOPE_TM_ID = Scope.format("tm_id");
	public static final String DEFAULT_SCOPE_TM_COMPONENT = Scope.concat(SCOPE_TM_HOST, "taskmanager", SCOPE_TM_ID);
	public static final String DEFAULT_SCOPE_TM = DEFAULT_SCOPE_TM_COMPONENT;

	private Map<JobID, JobMetricGroup> jobs = new HashMap<>();

	public TaskManagerMetricGroup(MetricRegistry registry, String host, String id) {
		super(registry, null, registry.getScopeConfig().getTaskManagerFormat());
		this.formats.put(SCOPE_TM_HOST, host);
		this.formats.put(SCOPE_TM_ID, id);
	}

	public JobMetricGroup addJob(JobID id, String name) {
		JobMetricGroup task = new JobMetricGroup(this.registry, this, id, name);
		jobs.put(id, task);
		return task;
	}

	@Override
	public void close() {
		super.close();
		for (MetricGroup group : jobs.values()) {
			group.close();
		}
		jobs.clear();
	}

	@Override
	protected String getScopeFormat(Scope.ScopeFormat format) {
		return format.getTaskManagerFormat();
	}
}

