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
package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.AccumulatorHelper;
import org.apache.flink.runtime.accumulators.AccumulatorRegistry;
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.checkpoint.stats.CheckpointStatsTracker;
import org.apache.flink.runtime.checkpoint.stats.OperatorCheckpointStats;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import scala.Option;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.runtime.executiongraph.ExecutionJobVertex.getAggregateJobVertexState;

public class ArchivedExecutionJobVertex implements AccessExecutionJobVertex, Serializable {

	private static final long serialVersionUID = -5768187638639437957L;
	private final ArchivedExecutionVertex[] taskVertices;

	private final JobVertexID id;

	private final String name;

	private final int parallelism;

	private final int maxParallelism;

	private final Map<AccumulatorRegistry.Metric, Accumulator<?, ?>> aggregatedMetricAccumulators;
	private final Option<OperatorCheckpointStats> checkpointStats;
	private final StringifiedAccumulatorResult[] archivedUserAccumulators;

	public ArchivedExecutionJobVertex(ExecutionJobVertex jobVertex) {
		this.taskVertices = new ArchivedExecutionVertex[jobVertex.getTaskVertices().length];
		for (int x = 0; x < taskVertices.length; x++) {
			taskVertices[x] = jobVertex.getTaskVertices()[x].archive();
		}

		aggregatedMetricAccumulators = jobVertex.getAggregatedMetricAccumulators();

		Map<String, Accumulator<?, ?>> tmpArchivedUserAccumulators = new HashMap<>();
		for (ExecutionVertex vertex : jobVertex.getTaskVertices()) {
			Map<String, Accumulator<?, ?>> next = vertex.getCurrentExecutionAttempt().getUserAccumulators();
			if (next != null) {
				AccumulatorHelper.mergeInto(tmpArchivedUserAccumulators, next);
			}
		}
		archivedUserAccumulators = jobVertex.getAggregatedUserAccumulatorsStringified();

		this.id = jobVertex.getJobVertexId();
		this.name = jobVertex.getJobVertex().getName();
		this.parallelism = jobVertex.getParallelism();
		this.maxParallelism = jobVertex.getMaxParallelism();
		CheckpointStatsTracker tracker = jobVertex.getGraph().getCheckpointStatsTracker();
		checkpointStats = tracker != null
			? tracker.getOperatorStats(this.id)
			: Option.<OperatorCheckpointStats>empty();
	}

	// --------------------------------------------------------------------------------------------
	//   Accessors
	// --------------------------------------------------------------------------------------------

	@Override
	public String getName() {
		return name;
	}

	@Override
	public int getParallelism() {
		return parallelism;
	}

	@Override
	public int getMaxParallelism() {
		return maxParallelism;
	}

	@Override
	public JobVertexID getJobVertexId() {
		return id;
	}

	@Override
	public ArchivedExecutionVertex[] getTaskVertices() {
		return taskVertices;
	}

	@Override
	public ExecutionState getAggregateState() {
		int[] num = new int[ExecutionState.values().length];
		for (ArchivedExecutionVertex vertex : this.taskVertices) {
			num[vertex.getExecutionState().ordinal()]++;
		}

		return getAggregateJobVertexState(num, parallelism);
	}

	public Map<AccumulatorRegistry.Metric, Accumulator<?, ?>> getAggregatedMetricAccumulators() {
		return this.aggregatedMetricAccumulators;
	}

	// --------------------------------------------------------------------------------------------
	//  Static / pre-assigned input splits
	// --------------------------------------------------------------------------------------------

	@Override
	public Option<OperatorCheckpointStats> getCheckpointStats() {
		return checkpointStats;
	}

	@Override
	public StringifiedAccumulatorResult[] getAggregatedUserAccumulatorsStringified() {
		return archivedUserAccumulators;
	}
}
