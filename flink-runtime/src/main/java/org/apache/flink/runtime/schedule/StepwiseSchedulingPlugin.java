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

package org.apache.flink.runtime.schedule;

import org.apache.flink.runtime.event.ExecutionVertexFailoverEvent;
import org.apache.flink.runtime.event.ExecutionVertexStateChangedEvent;
import org.apache.flink.runtime.event.ResultPartitionConsumableEvent;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.ExecutionVertexID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This plugin starts source vertices first and downstream vertices are started based on their consumable inputs.
 */
public class StepwiseSchedulingPlugin implements GraphManagerPlugin {

	private VertexScheduler scheduler;

	private JobGraph jobGraph;

	private VertexInputTracker inputTracker;

	@Override
	public void open(VertexScheduler scheduler, JobGraph jobGraph, SchedulingConfig config) {
		checkNotNull(config);

		this.scheduler = checkNotNull(scheduler);
		this.jobGraph = checkNotNull(jobGraph);
		this.inputTracker = new VertexInputTracker(jobGraph, scheduler, config);
	}

	@Override
	public void close() {

	}

	@Override
	public void reset() {

	}

	@Override
	public void onSchedulingStarted() {
		final List<ExecutionVertexID> verticesToSchedule = new ArrayList<>();
		for (JobVertex vertex : jobGraph.getVerticesSortedTopologicallyFromSources()) {
			if (vertex.isInputVertex()) {
				for (int i = 0; i < vertex.getParallelism(); i++) {
					verticesToSchedule.add(new ExecutionVertexID(vertex.getID(), i));
				}
			}
		}
		scheduleOneByOne(verticesToSchedule);
	}

	@Override
	public void onResultPartitionConsumable(ResultPartitionConsumableEvent event) {
		final List<ExecutionVertexID> verticesToSchedule = new ArrayList<>();
		final Collection<Collection<ExecutionVertexID>> consumerVertices = jobGraph
			.getResultPartitionConsumerExecutionVertices(event.getResultID(), event.getPartitionNumber());
		for (Collection<ExecutionVertexID> executionVertexIDs : consumerVertices) {
			for (ExecutionVertexID executionVertexID : executionVertexIDs) {
				if (isReadyToSchedule(executionVertexID)) {
					verticesToSchedule.add(executionVertexID);
				}
			}
		}
		scheduleOneByOne(verticesToSchedule);
	}

	@Override
	public void onExecutionVertexStateChanged(ExecutionVertexStateChangedEvent event) {

	}

	@Override
	public void onExecutionVertexFailover(ExecutionVertexFailoverEvent event) {
		final List<ExecutionVertexID> verticesToRestartNow = new ArrayList<>();
		for (ExecutionVertexID executionVertexID : event.getAffectedExecutionVertexIDs()) {
			if (isReadyToSchedule(executionVertexID)) {
				verticesToRestartNow.add(executionVertexID);
			}
		}
		scheduleOneByOne(verticesToRestartNow);
	}

	private boolean isReadyToSchedule(ExecutionVertexID vertexID) {
		ExecutionVertexStatus vertexStatus = scheduler.getExecutionVertexStatus(vertexID);

		// only CREATED vertices can be scheduled
		if (vertexStatus.getExecutionState() != ExecutionState.CREATED) {
			return false;
		}

		// source vertices can be scheduled at once
		if (jobGraph.findVertexByID(vertexID.getJobVertexID()).isInputVertex()) {
			return true;
		}

		// query whether the inputs are ready overall
		return inputTracker.areInputsReady(vertexID);
	}

	/**
	 * Schedule execution vertices one by one. These vertices will allocate resource and get to deploy individually.
	 */
	private void scheduleOneByOne(final List<ExecutionVertexID> verticesToSchedule) {
		for (ExecutionVertexID executionVertexID : verticesToSchedule) {
			scheduler.scheduleExecutionVertices(Collections.singleton(executionVertexID));
		}
	}
}
