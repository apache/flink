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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.accumulators.AccumulatorRegistry;
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.StandaloneCompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.savepoint.HeapSavepointStore;
import org.apache.flink.runtime.checkpoint.stats.CheckpointStats;
import org.apache.flink.runtime.checkpoint.stats.CheckpointStatsTracker;
import org.apache.flink.runtime.checkpoint.stats.JobCheckpointStats;
import org.apache.flink.runtime.checkpoint.stats.OperatorCheckpointStats;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.api.common.ArchivedExecutionConfig;
import org.apache.flink.runtime.executiongraph.restart.NoRestartStrategy;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.SerializedValue;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Option;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ArchivedExecutionGraphTest {
	private static JobVertexID v1ID = new JobVertexID();
	private static JobVertexID v2ID = new JobVertexID();

	private static ExecutionAttemptID executionWithAccumulatorsID;

	private static ExecutionGraph runtimeGraph;

	@BeforeClass
	public static void setupExecutionGraph() throws Exception {
		// -------------------------------------------------------------------------------------------------------------
		// Setup
		// -------------------------------------------------------------------------------------------------------------

		v1ID = new JobVertexID();
		v2ID = new JobVertexID();

		JobVertex v1 = new JobVertex("v1", v1ID);
		JobVertex v2 = new JobVertex("v2", v2ID);

		v1.setParallelism(1);
		v2.setParallelism(2);

		List<JobVertex> vertices = new ArrayList<JobVertex>(Arrays.asList(v1, v2));

		ExecutionConfig config = new ExecutionConfig();

		config.setExecutionMode(ExecutionMode.BATCH_FORCED);
		config.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());
		config.setParallelism(4);
		config.enableObjectReuse();
		config.setGlobalJobParameters(new TestJobParameters());

		runtimeGraph = new ExecutionGraph(
			TestingUtils.defaultExecutionContext(),
			new JobID(),
			"test job",
			new Configuration(),
			new SerializedValue<>(config),
			AkkaUtils.getDefaultTimeout(),
			new NoRestartStrategy());
		runtimeGraph.attachJobGraph(vertices);

		runtimeGraph.enableSnapshotCheckpointing(
			100,
			100,
			100,
			1,
			Collections.<ExecutionJobVertex>emptyList(),
			Collections.<ExecutionJobVertex>emptyList(),
			Collections.<ExecutionJobVertex>emptyList(),
			new StandaloneCheckpointIDCounter(),
			new StandaloneCompletedCheckpointStore(1, null),
			new HeapSavepointStore(),
			new TestCheckpointStatsTracker());

		Map<AccumulatorRegistry.Metric, Accumulator<?, ?>> flinkAccumulators = new HashMap<>();
		flinkAccumulators.put(AccumulatorRegistry.Metric.NUM_BYTES_IN, new LongCounter(32));

		Map<String, Accumulator<?, ?>> userAccumulators = new HashMap<>();
		userAccumulators.put("userAcc", new LongCounter(64));

		Execution executionWithAccumulators = runtimeGraph.getJobVertex(v1ID).getTaskVertices()[0].getCurrentExecutionAttempt();
		executionWithAccumulators.setAccumulators(flinkAccumulators, userAccumulators);
		executionWithAccumulatorsID = executionWithAccumulators.getAttemptId();

		runtimeGraph.getJobVertex(v2ID).getTaskVertices()[0].getCurrentExecutionAttempt().fail(new RuntimeException("This exception was thrown on purpose."));
	}

	@Test
	public void testArchive() throws IOException, ClassNotFoundException {
		ArchivedExecutionGraph archivedGraph = runtimeGraph.archive();

		compareExecutionGraph(runtimeGraph, archivedGraph);
	}

	@Test
	public void testSerialization() throws IOException, ClassNotFoundException {
		ArchivedExecutionGraph archivedGraph = runtimeGraph.archive();

		verifySerializability(archivedGraph);
	}

	private static void compareExecutionGraph(AccessExecutionGraph runtimeGraph, AccessExecutionGraph archivedGraph) throws IOException, ClassNotFoundException {
		assertTrue(archivedGraph.isArchived());
		// -------------------------------------------------------------------------------------------------------------
		// ExecutionGraph
		// -------------------------------------------------------------------------------------------------------------
		assertEquals(runtimeGraph.getJsonPlan(), archivedGraph.getJsonPlan());
		assertEquals(runtimeGraph.getJobID(), archivedGraph.getJobID());
		assertEquals(runtimeGraph.getJobName(), archivedGraph.getJobName());
		assertEquals(runtimeGraph.getState(), archivedGraph.getState());
		assertEquals(runtimeGraph.getFailureCauseAsString(), archivedGraph.getFailureCauseAsString());
		assertEquals(runtimeGraph.getStatusTimestamp(JobStatus.CREATED), archivedGraph.getStatusTimestamp(JobStatus.CREATED));
		assertEquals(runtimeGraph.getStatusTimestamp(JobStatus.RUNNING), archivedGraph.getStatusTimestamp(JobStatus.RUNNING));
		assertEquals(runtimeGraph.getStatusTimestamp(JobStatus.FAILING), archivedGraph.getStatusTimestamp(JobStatus.FAILING));
		assertEquals(runtimeGraph.getStatusTimestamp(JobStatus.FAILED), archivedGraph.getStatusTimestamp(JobStatus.FAILED));
		assertEquals(runtimeGraph.getStatusTimestamp(JobStatus.CANCELLING), archivedGraph.getStatusTimestamp(JobStatus.CANCELLING));
		assertEquals(runtimeGraph.getStatusTimestamp(JobStatus.CANCELED), archivedGraph.getStatusTimestamp(JobStatus.CANCELED));
		assertEquals(runtimeGraph.getStatusTimestamp(JobStatus.FINISHED), archivedGraph.getStatusTimestamp(JobStatus.FINISHED));
		assertEquals(runtimeGraph.getStatusTimestamp(JobStatus.RESTARTING), archivedGraph.getStatusTimestamp(JobStatus.RESTARTING));
		assertEquals(runtimeGraph.getStatusTimestamp(JobStatus.SUSPENDED), archivedGraph.getStatusTimestamp(JobStatus.SUSPENDED));
		assertEquals(runtimeGraph.isStoppable(), archivedGraph.isStoppable());

		// -------------------------------------------------------------------------------------------------------------
		// JobCheckpointStats
		// -------------------------------------------------------------------------------------------------------------
		JobCheckpointStats runtimeStats = runtimeGraph.getCheckpointStatsTracker().getJobStats().get();
		JobCheckpointStats archivedStats = archivedGraph.getCheckpointStatsTracker().getJobStats().get();

		assertEquals(runtimeStats.getAverageDuration(), archivedStats.getAverageDuration());
		assertEquals(runtimeStats.getMinDuration(), archivedStats.getMinDuration());
		assertEquals(runtimeStats.getMaxDuration(), archivedStats.getMaxDuration());
		assertEquals(runtimeStats.getAverageStateSize(), archivedStats.getAverageStateSize());
		assertEquals(runtimeStats.getMinStateSize(), archivedStats.getMinStateSize());
		assertEquals(runtimeStats.getMaxStateSize(), archivedStats.getMaxStateSize());
		assertEquals(runtimeStats.getCount(), archivedStats.getCount());
		assertEquals(runtimeStats.getRecentHistory(), archivedStats.getRecentHistory());

		// -------------------------------------------------------------------------------------------------------------
		// ArchivedExecutionConfig
		// -------------------------------------------------------------------------------------------------------------
		ArchivedExecutionConfig runtimeConfig = runtimeGraph.getArchivedExecutionConfig();
		ArchivedExecutionConfig archivedConfig = archivedGraph.getArchivedExecutionConfig();

		assertEquals(runtimeConfig.getExecutionMode(), archivedConfig.getExecutionMode());
		assertEquals(runtimeConfig.getParallelism(), archivedConfig.getParallelism());
		assertEquals(runtimeConfig.getObjectReuseEnabled(), archivedConfig.getObjectReuseEnabled());
		assertEquals(runtimeConfig.getRestartStrategyDescription(), archivedConfig.getRestartStrategyDescription());
		assertNotNull(archivedConfig.getGlobalJobParameters().get("hello"));
		assertEquals(runtimeConfig.getGlobalJobParameters().get("hello"), archivedConfig.getGlobalJobParameters().get("hello"));

		// -------------------------------------------------------------------------------------------------------------
		// StringifiedAccumulators
		// -------------------------------------------------------------------------------------------------------------
		compareStringifiedAccumulators(runtimeGraph.getAccumulatorResultsStringified(), archivedGraph.getAccumulatorResultsStringified());
		compareSerializedAccumulators(runtimeGraph.getAccumulatorsSerialized(), archivedGraph.getAccumulatorsSerialized());
		compareFlinkAccumulators(runtimeGraph.getFlinkAccumulators().get(executionWithAccumulatorsID), archivedGraph.getFlinkAccumulators().get(executionWithAccumulatorsID));

		// -------------------------------------------------------------------------------------------------------------
		// JobVertices
		// -------------------------------------------------------------------------------------------------------------
		Map<JobVertexID, ? extends AccessExecutionJobVertex> runtimeVertices = runtimeGraph.getAllVertices();
		Map<JobVertexID, ? extends AccessExecutionJobVertex> archivedVertices = archivedGraph.getAllVertices();

		for (Map.Entry<JobVertexID, ? extends AccessExecutionJobVertex> vertex : runtimeVertices.entrySet()) {
			compareExecutionJobVertex(vertex.getValue(), archivedVertices.get(vertex.getKey()));
		}

		Iterator<? extends AccessExecutionJobVertex> runtimeTopologicalVertices = runtimeGraph.getVerticesTopologically().iterator();
		Iterator<? extends AccessExecutionJobVertex> archiveTopologicaldVertices = archivedGraph.getVerticesTopologically().iterator();

		while (runtimeTopologicalVertices.hasNext()) {
			assertTrue(archiveTopologicaldVertices.hasNext());
			compareExecutionJobVertex(runtimeTopologicalVertices.next(), archiveTopologicaldVertices.next());
		}

		// -------------------------------------------------------------------------------------------------------------
		// OperatorCheckpointStats
		// -------------------------------------------------------------------------------------------------------------
		CheckpointStatsTracker runtimeTracker = runtimeGraph.getCheckpointStatsTracker();
		CheckpointStatsTracker archivedTracker = archivedGraph.getCheckpointStatsTracker();
		compareOperatorCheckpointStats(runtimeTracker.getOperatorStats(v1ID).get(), archivedTracker.getOperatorStats(v1ID).get());
		compareOperatorCheckpointStats(runtimeTracker.getOperatorStats(v2ID).get(), archivedTracker.getOperatorStats(v2ID).get());

		// -------------------------------------------------------------------------------------------------------------
		// ExecutionVertices
		// -------------------------------------------------------------------------------------------------------------
		Iterator<? extends AccessExecutionVertex> runtimeExecutionVertices = runtimeGraph.getAllExecutionVertices().iterator();
		Iterator<? extends AccessExecutionVertex> archivedExecutionVertices = archivedGraph.getAllExecutionVertices().iterator();

		while (runtimeExecutionVertices.hasNext()) {
			assertTrue(archivedExecutionVertices.hasNext());
			compareExecutionVertex(runtimeExecutionVertices.next(), archivedExecutionVertices.next());
		}
	}

	private static void compareExecutionJobVertex(AccessExecutionJobVertex runtimeJobVertex, AccessExecutionJobVertex archivedJobVertex) {
		assertEquals(runtimeJobVertex.getName(), archivedJobVertex.getName());
		assertEquals(runtimeJobVertex.getParallelism(), archivedJobVertex.getParallelism());
		assertEquals(runtimeJobVertex.getMaxParallelism(), archivedJobVertex.getMaxParallelism());
		assertEquals(runtimeJobVertex.getJobVertexId(), archivedJobVertex.getJobVertexId());
		assertEquals(runtimeJobVertex.getAggregateState(), archivedJobVertex.getAggregateState());

		compareOperatorCheckpointStats(runtimeJobVertex.getCheckpointStats().get(), archivedJobVertex.getCheckpointStats().get());

		compareStringifiedAccumulators(runtimeJobVertex.getAggregatedUserAccumulatorsStringified(), archivedJobVertex.getAggregatedUserAccumulatorsStringified());
		compareFlinkAccumulators(runtimeJobVertex.getAggregatedMetricAccumulators(), archivedJobVertex.getAggregatedMetricAccumulators());

		AccessExecutionVertex[] runtimeExecutionVertices = runtimeJobVertex.getTaskVertices();
		AccessExecutionVertex[] archivedExecutionVertices = archivedJobVertex.getTaskVertices();
		assertEquals(runtimeExecutionVertices.length, archivedExecutionVertices.length);
		for (int x = 0; x < runtimeExecutionVertices.length; x++) {
			compareExecutionVertex(runtimeExecutionVertices[x], archivedExecutionVertices[x]);
		}
	}

	private static void compareExecutionVertex(AccessExecutionVertex runtimeVertex, AccessExecutionVertex archivedVertex) {
		assertEquals(runtimeVertex.getTaskNameWithSubtaskIndex(), archivedVertex.getTaskNameWithSubtaskIndex());
		assertEquals(runtimeVertex.getParallelSubtaskIndex(), archivedVertex.getParallelSubtaskIndex());
		assertEquals(runtimeVertex.getExecutionState(), archivedVertex.getExecutionState());
		assertEquals(runtimeVertex.getStateTimestamp(ExecutionState.CREATED), archivedVertex.getStateTimestamp(ExecutionState.CREATED));
		assertEquals(runtimeVertex.getStateTimestamp(ExecutionState.SCHEDULED), archivedVertex.getStateTimestamp(ExecutionState.SCHEDULED));
		assertEquals(runtimeVertex.getStateTimestamp(ExecutionState.DEPLOYING), archivedVertex.getStateTimestamp(ExecutionState.DEPLOYING));
		assertEquals(runtimeVertex.getStateTimestamp(ExecutionState.RUNNING), archivedVertex.getStateTimestamp(ExecutionState.RUNNING));
		assertEquals(runtimeVertex.getStateTimestamp(ExecutionState.FINISHED), archivedVertex.getStateTimestamp(ExecutionState.FINISHED));
		assertEquals(runtimeVertex.getStateTimestamp(ExecutionState.CANCELING), archivedVertex.getStateTimestamp(ExecutionState.CANCELING));
		assertEquals(runtimeVertex.getStateTimestamp(ExecutionState.CANCELED), archivedVertex.getStateTimestamp(ExecutionState.CANCELED));
		assertEquals(runtimeVertex.getStateTimestamp(ExecutionState.FAILED), archivedVertex.getStateTimestamp(ExecutionState.FAILED));
		assertEquals(runtimeVertex.getFailureCauseAsString(), archivedVertex.getFailureCauseAsString());
		assertEquals(runtimeVertex.getCurrentAssignedResourceLocation(), archivedVertex.getCurrentAssignedResourceLocation());

		compareExecution(runtimeVertex.getCurrentExecutionAttempt(), archivedVertex.getCurrentExecutionAttempt());
	}

	private static void compareExecution(AccessExecution runtimeExecution, AccessExecution archivedExecution) {
		assertEquals(runtimeExecution.getAttemptId(), archivedExecution.getAttemptId());
		assertEquals(runtimeExecution.getAttemptNumber(), archivedExecution.getAttemptNumber());
		assertArrayEquals(runtimeExecution.getStateTimestamps(), archivedExecution.getStateTimestamps());
		assertEquals(runtimeExecution.getState(), archivedExecution.getState());
		assertEquals(runtimeExecution.getAssignedResourceLocation(), archivedExecution.getAssignedResourceLocation());
		assertEquals(runtimeExecution.getFailureCauseAsString(), archivedExecution.getFailureCauseAsString());
		assertEquals(runtimeExecution.getStateTimestamp(ExecutionState.CREATED), archivedExecution.getStateTimestamp(ExecutionState.CREATED));
		assertEquals(runtimeExecution.getStateTimestamp(ExecutionState.SCHEDULED), archivedExecution.getStateTimestamp(ExecutionState.SCHEDULED));
		assertEquals(runtimeExecution.getStateTimestamp(ExecutionState.DEPLOYING), archivedExecution.getStateTimestamp(ExecutionState.DEPLOYING));
		assertEquals(runtimeExecution.getStateTimestamp(ExecutionState.RUNNING), archivedExecution.getStateTimestamp(ExecutionState.RUNNING));
		assertEquals(runtimeExecution.getStateTimestamp(ExecutionState.FINISHED), archivedExecution.getStateTimestamp(ExecutionState.FINISHED));
		assertEquals(runtimeExecution.getStateTimestamp(ExecutionState.CANCELING), archivedExecution.getStateTimestamp(ExecutionState.CANCELING));
		assertEquals(runtimeExecution.getStateTimestamp(ExecutionState.CANCELED), archivedExecution.getStateTimestamp(ExecutionState.CANCELED));
		assertEquals(runtimeExecution.getStateTimestamp(ExecutionState.FAILED), archivedExecution.getStateTimestamp(ExecutionState.FAILED));
		compareStringifiedAccumulators(runtimeExecution.getUserAccumulatorsStringified(), archivedExecution.getUserAccumulatorsStringified());
		compareFlinkAccumulators(runtimeExecution.getFlinkAccumulators(), archivedExecution.getFlinkAccumulators());
		assertEquals(runtimeExecution.getParallelSubtaskIndex(), archivedExecution.getParallelSubtaskIndex());
	}

	private static void compareStringifiedAccumulators(StringifiedAccumulatorResult[] runtimeAccs, StringifiedAccumulatorResult[] archivedAccs) {
		assertEquals(runtimeAccs.length, archivedAccs.length);

		for (int x = 0; x < runtimeAccs.length; x++) {
			StringifiedAccumulatorResult runtimeResult = runtimeAccs[x];
			StringifiedAccumulatorResult archivedResult = archivedAccs[x];

			assertEquals(runtimeResult.getName(), archivedResult.getName());
			assertEquals(runtimeResult.getType(), archivedResult.getType());
			assertEquals(runtimeResult.getValue(), archivedResult.getValue());
		}
	}

	private static void compareSerializedAccumulators(Map<String, SerializedValue<Object>> runtimeAccs, Map<String, SerializedValue<Object>> archivedAccs) throws IOException, ClassNotFoundException {
		assertEquals(runtimeAccs.size(), archivedAccs.size());
		for (Map.Entry<String, SerializedValue<Object>> runtimeAcc : runtimeAccs.entrySet()) {
			long runtimeUserAcc = (long) runtimeAcc.getValue().deserializeValue(ClassLoader.getSystemClassLoader());
			long archivedUserAcc = (long) archivedAccs.get(runtimeAcc.getKey()).deserializeValue(ClassLoader.getSystemClassLoader());

			assertEquals(runtimeUserAcc, archivedUserAcc);
		}
	}

	private static void compareFlinkAccumulators(Map<AccumulatorRegistry.Metric, Accumulator<?, ?>> runtimeAccs, Map<AccumulatorRegistry.Metric, Accumulator<?, ?>> archivedAccs) {
		assertEquals(runtimeAccs == null, archivedAccs == null);
		if (runtimeAccs != null && archivedAccs != null) {
			assertEquals(runtimeAccs.size(), archivedAccs.size());
			for (Map.Entry<AccumulatorRegistry.Metric, Accumulator<?, ?>> runtimeAcc : runtimeAccs.entrySet()) {
				Accumulator<?, ?> archivedAcc = archivedAccs.get(runtimeAcc.getKey());

				assertEquals(runtimeAcc.getValue().getLocalValue(), archivedAcc.getLocalValue());
			}
		}
	}

	private static void compareOperatorCheckpointStats(OperatorCheckpointStats runtimeStats, OperatorCheckpointStats archivedStats) {
		assertEquals(runtimeStats.getNumberOfSubTasks(), archivedStats.getNumberOfSubTasks());
		assertEquals(runtimeStats.getCheckpointId(), archivedStats.getCheckpointId());
		assertEquals(runtimeStats.getDuration(), archivedStats.getDuration());
		assertEquals(runtimeStats.getStateSize(), archivedStats.getStateSize());
		assertEquals(runtimeStats.getTriggerTimestamp(), archivedStats.getTriggerTimestamp());
		assertEquals(runtimeStats.getSubTaskDuration(0), archivedStats.getSubTaskDuration(0));
		assertEquals(runtimeStats.getSubTaskStateSize(0), archivedStats.getSubTaskStateSize(0));
	}

	private static void verifySerializability(ArchivedExecutionGraph graph) throws IOException, ClassNotFoundException {
		ArchivedExecutionGraph copy = CommonTestUtils.createCopySerializable(graph);
		compareExecutionGraph(graph, copy);
	}


	private static class TestCheckpointStatsTracker implements CheckpointStatsTracker {

		@Override
		public void onCompletedCheckpoint(CompletedCheckpoint checkpoint) {
		}

		@Override
		public Option<JobCheckpointStats> getJobStats() {
			return Option.<JobCheckpointStats>apply(new TestJobCheckpointStats());
		}

		@Override
		public Option<OperatorCheckpointStats> getOperatorStats(JobVertexID operatorId) {
			return Option.<OperatorCheckpointStats>apply(new TestOperatorCheckpointStats(operatorId.getUpperPart()));
		}
	}

	private static class TestJobCheckpointStats implements JobCheckpointStats {
		private static final long serialVersionUID = -2630234917947292836L;

		@Override
		public List<CheckpointStats> getRecentHistory() {
			return Collections.emptyList();
		}

		@Override
		public long getCount() {
			return 1;
		}

		@Override
		public long getMinDuration() {
			return 2;
		}

		@Override
		public long getMaxDuration() {
			return 4;
		}

		@Override
		public long getAverageDuration() {
			return 3;
		}

		@Override
		public long getMinStateSize() {
			return 5;
		}

		@Override
		public long getMaxStateSize() {
			return 7;
		}

		@Override
		public long getAverageStateSize() {
			return 6;
		}
	}

	private static class TestOperatorCheckpointStats extends OperatorCheckpointStats {
		private static final long serialVersionUID = -2798640928349528644L;

		public TestOperatorCheckpointStats(long offset) {
			super(1 + offset, 2 + offset, 3 + offset, 4 + offset, new long[][]{new long[]{5 + offset, 6 + offset}});
		}
	}

	private static class TestJobParameters extends ExecutionConfig.GlobalJobParameters {
		private static final long serialVersionUID = -8118611781035212808L;
		private Map<String, String> parameters;

		private TestJobParameters() {
			this.parameters = new HashMap<>();
			this.parameters.put("hello", "world");
		}

		@Override
		public Map<String, String> toMap() {
			return parameters;
		}
	}
}
