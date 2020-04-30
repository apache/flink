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

import org.apache.flink.api.common.JobID;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.metrics.scope.ScopeFormat;

import java.util.HashMap;
import java.util.Map;

/**
 * Special {@link org.apache.flink.metrics.MetricGroup} representing a JobManager.
 *
 * <p>Contains extra logic for adding jobs with tasks, and removing jobs when they do
 * not contain tasks any more
 */
public class JobManagerMetricGroup extends ComponentMetricGroup<JobManagerMetricGroup> {

	private final Map<JobID, JobManagerJobMetricGroup> jobs = new HashMap<>();

	private final String hostname;

	public JobManagerMetricGroup(MetricRegistry registry, String hostname) {
		super(registry, registry.getScopeFormats().getJobManagerFormat().formatScope(hostname), null);
		this.hostname = hostname;
	}

	public String hostname() {
		return hostname;
	}

	@Override
	protected QueryScopeInfo.JobManagerQueryScopeInfo createQueryServiceMetricInfo(CharacterFilter filter) {
		return new QueryScopeInfo.JobManagerQueryScopeInfo();
	}

	// ------------------------------------------------------------------------
	//  job groups
	// ------------------------------------------------------------------------

	public JobManagerJobMetricGroup addJob(JobGraph job) {
		JobID jobId = job.getJobID();
		String jobName = job.getName();
		// get or create a jobs metric group
		JobManagerJobMetricGroup currentJobGroup;
		synchronized (this) {
			if (!isClosed()) {
				currentJobGroup = jobs.get(jobId);

				if (currentJobGroup == null || currentJobGroup.isClosed()) {
					currentJobGroup = new JobManagerJobMetricGroup(registry, this, jobId, jobName);
					jobs.put(jobId, currentJobGroup);
				}
				return currentJobGroup;
			} else {
				return null;
			}
		}
	}

	public void removeJob(JobID jobId) {
		if (jobId == null) {
			return;
		}

		synchronized (this) {
			JobManagerJobMetricGroup containedGroup = jobs.remove(jobId);
			if (containedGroup != null) {
				containedGroup.close();
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
	}

	@Override
	protected Iterable<? extends ComponentMetricGroup> subComponents() {
		return jobs.values();
	}

	@Override
	protected String getGroupName(CharacterFilter filter) {
		return "jobmanager";
	}
}

