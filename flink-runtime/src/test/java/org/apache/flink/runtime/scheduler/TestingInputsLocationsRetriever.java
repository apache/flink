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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * A simple inputs locations retriever for testing purposes.
 */
public class TestingInputsLocationsRetriever implements InputsLocationsRetriever {

	private final Map<ExecutionVertexID, Collection<Collection<ExecutionVertexID>>> vertexToUpstreams;

	private final Map<ExecutionVertexID, CompletableFuture<TaskManagerLocation>> vertexToTaskManagerLocations;

	public TestingInputsLocationsRetriever(JobVertex... jobVertices) {
		this.vertexToUpstreams = new HashMap<>();
		this.vertexToTaskManagerLocations = new HashMap<>();

		initVertexToUpstreamsRelation(jobVertices);
		initVertexTaskManagerLocationFutures(jobVertices);
	}

	@Override
	public Collection<Collection<ExecutionVertexID>> getConsumedResultPartitionsProducers(ExecutionVertexID executionVertexId) {
		return vertexToUpstreams.getOrDefault(executionVertexId, Collections.emptyList());
	}

	@Override
	public Optional<CompletableFuture<TaskManagerLocation>> getTaskManagerLocation(ExecutionVertexID executionVertexId) {
		return Optional.ofNullable(vertexToTaskManagerLocations.get(executionVertexId));
	}

	public int getTotalNumberOfVertices() {
		return vertexToTaskManagerLocations.size();
	}

	public Collection<ExecutionVertexID> getAllExecutionVertices() {
		return  vertexToTaskManagerLocations.keySet();
	}

	private void initVertexToUpstreamsRelation(JobVertex... jobVertices) {

		for (JobVertex jobVertex : jobVertices) {
			for (JobEdge jobEdge : jobVertex.getInputs()) {
				JobVertex upstream = jobEdge.getSource().getProducer();

				Collection<ExecutionVertexID> upstreams = new ArrayList<>();
				if (jobEdge.getDistributionPattern() == DistributionPattern.ALL_TO_ALL) {
					for (int i = 0; i < upstream.getParallelism(); i++) {
						upstreams.add(new ExecutionVertexID(upstream.getID(), i));
					}
				}

				for (int i = 0; i < jobVertex.getParallelism(); i++) {
					if (jobEdge.getDistributionPattern() == DistributionPattern.POINTWISE) {
						upstreams = new ArrayList<>();
						if (jobVertex.getParallelism() > upstream.getParallelism()) {
							int times = (int) Math.ceil(jobVertex.getParallelism() / upstream.getParallelism());
							upstreams.add(new ExecutionVertexID(upstream.getID(), i / times));
						} else {
							int times = (int) Math.ceil(upstream.getParallelism() / jobVertex.getParallelism());
							for (int j = i * times; j < (i + 1) * times && j < upstream.getParallelism(); j++) {
								upstreams.add(new ExecutionVertexID(upstream.getID(), j));
							}
						}
					}
					ExecutionVertexID executionVertexId = new ExecutionVertexID(jobVertex.getID(), i);
					Collection<Collection<ExecutionVertexID>> existingUpstreams =
							vertexToUpstreams.computeIfAbsent(executionVertexId, k -> new ArrayList<>());
					existingUpstreams.add(upstreams);
				}
			}
		}
	}

	private void initVertexTaskManagerLocationFutures(JobVertex... jobVertices) {

		for (JobVertex jobVertex : jobVertices) {
			for (int i = 0; i < jobVertex.getParallelism(); i++) {
				ExecutionVertexID executionVertexId = new ExecutionVertexID(jobVertex.getID(), i);
				vertexToTaskManagerLocations.put(executionVertexId, new CompletableFuture<>());
			}
		}
	}
}
