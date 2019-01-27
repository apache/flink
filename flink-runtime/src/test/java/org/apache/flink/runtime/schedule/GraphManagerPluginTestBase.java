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

import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.ExecutionVertexID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.util.TestLogger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Test base for the {@link GraphManagerPlugin}.
 */
public class GraphManagerPluginTestBase extends TestLogger {

	protected static boolean compareVertices(Collection<ExecutionVertexID> vertices1, Collection<ExecutionVertexID> vertices2) {
		checkNotNull(vertices1);
		checkNotNull(vertices2);
		return vertices1.size() == vertices2.size() && vertices1.containsAll(vertices2)  && vertices2.containsAll(vertices1);
	}

	/**
	 * VertexScheduler for test purposes.
	 */
	protected static class TestExecutionVertexScheduler implements VertexScheduler {

		private ExecutionGraph executionGraph;

		private Map<ExecutionVertexID, ExecutionVertex> vertices;

		private Collection<ExecutionVertexID> scheduledVertices = new ArrayList<>();

		public TestExecutionVertexScheduler(ExecutionGraph eg, Collection<ExecutionVertex> evs) {
			this.executionGraph = eg;

			this.vertices = new HashMap<>();
			for (ExecutionVertex ev : evs) {
				vertices.put(ev.getExecutionVertexID(), ev);
			}
		}

		@Override
		public void scheduleExecutionVertices(Collection<ExecutionVertexID> executionVertexIDs) {
			scheduledVertices.addAll(executionVertexIDs);
		}

		@Override
		public ExecutionVertexStatus getExecutionVertexStatus(ExecutionVertexID executionVertexID) {
			ExecutionVertex ev = vertices.get(executionVertexID);
			return new ExecutionVertexStatus(executionVertexID, ev.getExecutionState());
		}

		@Override
		public ResultPartitionStatus getResultPartitionStatus(IntermediateDataSetID resultID, int partitionNumber) {
			boolean consumable = executionGraph.getAllIntermediateResults().get(resultID)
				.getPartitions()[partitionNumber].isConsumable();

			return new ResultPartitionStatus(resultID, partitionNumber, consumable);
		}

		@Override
		public double getResultConsumablePartitionRatio(IntermediateDataSetID resultID) {
			return executionGraph.getAllIntermediateResults().get(resultID).getResultConsumablePartitionRatio();
		}

		public Collection<ExecutionVertexID> getScheduledVertices() {
			return scheduledVertices;
		}

		public void clearScheduledVertices() {
			scheduledVertices.clear();
		}
	}
}
