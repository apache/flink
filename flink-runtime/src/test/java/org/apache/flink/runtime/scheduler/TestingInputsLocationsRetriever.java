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

import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * A simple inputs locations retriever for testing purposes.
 */
class TestingInputsLocationsRetriever implements InputsLocationsRetriever {

	private final Map<ExecutionVertexID, List<ExecutionVertexID>> producersByConsumer;

	private final Map<ExecutionVertexID, CompletableFuture<TaskManagerLocation>> taskManagerLocationsByVertex = new HashMap<>();

	TestingInputsLocationsRetriever(final Map<ExecutionVertexID, List<ExecutionVertexID>> producersByConsumer) {
		this.producersByConsumer = new HashMap<>(producersByConsumer);
	}

	@Override
	public Collection<Collection<ExecutionVertexID>> getConsumedResultPartitionsProducers(final ExecutionVertexID executionVertexId) {
		final Map<JobVertexID, List<ExecutionVertexID>> executionVerticesByJobVertex =
				producersByConsumer.getOrDefault(executionVertexId, Collections.emptyList())
						.stream()
						.collect(Collectors.groupingBy(ExecutionVertexID::getJobVertexId));

		return new ArrayList<>(executionVerticesByJobVertex.values());
	}

	@Override
	public Optional<CompletableFuture<TaskManagerLocation>> getTaskManagerLocation(final ExecutionVertexID executionVertexId) {
		return Optional.ofNullable(taskManagerLocationsByVertex.get(executionVertexId));
	}

	public void markScheduled(final ExecutionVertexID executionVertexId) {
		taskManagerLocationsByVertex.put(executionVertexId, new CompletableFuture<>());
	}

	public void assignTaskManagerLocation(final ExecutionVertexID executionVertexId) {
		taskManagerLocationsByVertex.compute(executionVertexId, (key, future) -> {
			if (future == null) {
				return CompletableFuture.completedFuture(new LocalTaskManagerLocation());
			}
			future.complete(new LocalTaskManagerLocation());
			return future;
		});
	}

	static class Builder {

		private final Map<ExecutionVertexID, List<ExecutionVertexID>> producersByConsumer = new HashMap<>();

		public Builder connectConsumerToProducer(final ExecutionVertexID consumer, final ExecutionVertexID producer) {
			producersByConsumer
				.computeIfAbsent(consumer, (key) -> new ArrayList<>())
				.add(producer);
			return this;
		}

		public TestingInputsLocationsRetriever build() {
			return new TestingInputsLocationsRetriever(producersByConsumer);
		}

	}
}
