/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.resource.batch.schedule;

import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * RunningUnit consist of logical jobVertex.
 */
public class LogicalJobVertexRunningUnit {

	private final Set<LogicalJobVertex> jobVertexSet = new LinkedHashSet<>();

	// input depend on the jobVertexSet.
	private final Set<JobVertexID> inputDependSet = new HashSet<>();

	private final Set<JobVertexID> receivedInputDependSet = new HashSet<>();

	private final Set<LogicalJobVertexRunningUnit> joinDependSet = new HashSet<>();

	private final Set<LogicalJobVertexRunningUnit> haveJoinedDependSet = new HashSet<>();

	public void addJoinDepend(LogicalJobVertexRunningUnit runningUnit) {
		joinDependSet.add(runningUnit);
	}

	public void addInputDepend(JobVertexID jobVertexID) {
		inputDependSet.add(jobVertexID);
	}

	public LogicalJobVertexRunningUnit(Set<LogicalJobVertex> jobVertexSet) {
		this.jobVertexSet.addAll(jobVertexSet);
	}

	// exclude deployed tasks.
	public List<LogicalJobVertex> getToScheduleJobVertices() {
		return jobVertexSet.stream().filter(j -> !j.allTasksDeploying()).collect(Collectors.toList());
	}

	// check all jobs whether all tasks deployed.
	public boolean allTasksDeploying() {
		for (LogicalJobVertex job : jobVertexSet) {
			if (!job.allTasksDeploying()) {
				return false;
			}
		}
		return true;
	}

	public void receiveInput(JobVertexID jobVertexID) {
		receivedInputDependSet.add(jobVertexID);
	}

	public void joinDependDeploying(LogicalJobVertexRunningUnit runningUnit) {
		haveJoinedDependSet.add(runningUnit);
	}

	public Set<LogicalJobVertex> getJobVertexSet() {
		return jobVertexSet;
	}

	public boolean allDependReady() {
		return receivedInputDependSet.size() == inputDependSet.size()
				&& haveJoinedDependSet.size() == joinDependSet.size();
	}

	public void reset() {
		receivedInputDependSet.clear();
		haveJoinedDependSet.clear();
	}
}
