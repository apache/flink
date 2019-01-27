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

package org.apache.flink.runtime.accumulators;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.accumulators.AbstractAccumulatorRegistry;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.preaggregatedaccumulators.AccumulatorAggregationManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Main accumulator registry which encapsulates user-defined accumulators.
 */
public class AccumulatorRegistry extends AbstractAccumulatorRegistry {

	protected static final Logger LOG = LoggerFactory.getLogger(AccumulatorRegistry.class);

	private final JobID jobID;
	private final JobVertexID jobVertexID;
	private final int subtaskIndex;
	private final ExecutionAttemptID taskID;
	private final AccumulatorAggregationManager accumulatorAggregationManager;

	/* User-defined pre-aggregated accumulator values stored for the executing task. */
	private final Map<String, Accumulator<?, ?>> preAggregatedUserAccumulators =
		new ConcurrentHashMap<>(4);

	public AccumulatorRegistry(JobID jobID,
								JobVertexID jobVertexID,
								int subtaskIndex,
								ExecutionAttemptID taskID,
								AccumulatorAggregationManager accumulatorAggregationManager) {
		this.jobID = jobID;
		this.jobVertexID = jobVertexID;
		this.subtaskIndex = subtaskIndex;
		this.taskID = taskID;
		this.accumulatorAggregationManager = accumulatorAggregationManager;
	}

	/**
	 * Creates a snapshot of this accumulator registry.
	 * @return a serialized accumulator map
	 */
	public AccumulatorSnapshot getSnapshot() {
		try {
			return new AccumulatorSnapshot(jobID, taskID, userAccumulators);
		} catch (Throwable e) {
			LOG.warn("Failed to serialize accumulators for task.", e);
			return null;
		}
	}

	@Override
	public <V, A extends Serializable> void addPreAggregatedAccumulator(String name, Accumulator<V, A> accumulator) {
		if (preAggregatedUserAccumulators.containsKey(name)) {
			throw new UnsupportedOperationException("The accumulator '" + name
				+ "' already exists and cannot be added.");
		}
		preAggregatedUserAccumulators.put(name, accumulator);

		accumulatorAggregationManager.registerPreAggregatedAccumulator(jobID, jobVertexID, subtaskIndex, name);
	}

	@Override
	public Map<String, Accumulator<?, ?>> getPreAggregatedAccumulators() {
		return Collections.unmodifiableMap(preAggregatedUserAccumulators);
	}

	@Override
	public void commitPreAggregatedAccumulator(String name) {
		if (!preAggregatedUserAccumulators.containsKey(name)) {
			throw new UnsupportedOperationException("The accumulator '" + name
				+ "' does not exists.");
		}

		Accumulator<?, ?> accumulator = preAggregatedUserAccumulators.remove(name);
		accumulatorAggregationManager.commitPreAggregatedAccumulator(jobID, jobVertexID, subtaskIndex, name, accumulator);
	}

	@Override
	public <V, A extends Serializable> CompletableFuture<Accumulator<V, A>> queryPreAggregatedAccumulator(String name) {
		return accumulatorAggregationManager.queryPreAggregatedAccumulator(jobID, name);
	}
}
