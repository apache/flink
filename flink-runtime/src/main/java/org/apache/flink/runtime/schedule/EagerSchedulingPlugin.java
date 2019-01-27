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
import org.apache.flink.runtime.jobgraph.ExecutionVertexID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This plugin starts all vertices once the scheduling is started.
 */
public class EagerSchedulingPlugin implements GraphManagerPlugin {

	private static final Logger LOG = LoggerFactory.getLogger(EagerSchedulingPlugin.class);

	private VertexScheduler scheduler;

	private JobGraph jobGraph;

	@Override
	public void open(VertexScheduler scheduler, JobGraph jobGraph, SchedulingConfig config) {
		checkNotNull(config);

		this.scheduler = checkNotNull(scheduler);
		this.jobGraph = checkNotNull(jobGraph);
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
			for (int i = 0; i < vertex.getParallelism(); i++) {
				verticesToSchedule.add(new ExecutionVertexID(vertex.getID(), i));
			}
		}
		scheduler.scheduleExecutionVertices(verticesToSchedule);
	}

	@Override
	public void onResultPartitionConsumable(ResultPartitionConsumableEvent event) {
		LOG.warn("Ignore the result partition consumable event {} as the schedule mode is EAGER.", event);
	}

	@Override
	public void onExecutionVertexStateChanged(ExecutionVertexStateChangedEvent event) {

	}

	@Override
	public void onExecutionVertexFailover(ExecutionVertexFailoverEvent event) {
		scheduler.scheduleExecutionVertices(event.getAffectedExecutionVertexIDs());
	}

	@Override
	public boolean allowLazyDeployment() {
		return false;
	}
}
