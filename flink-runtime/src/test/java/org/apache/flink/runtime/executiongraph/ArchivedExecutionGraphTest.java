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

import org.apache.flink.api.common.ArchivedExecutionConfig;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.runtime.checkpoint.CheckpointStatsSnapshot;
import org.apache.flink.runtime.checkpoint.CheckpointStatsTracker;
import org.apache.flink.runtime.checkpoint.MasterTriggerRestoreHook;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.StandaloneCompletedCheckpointStore;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.util.OptionalFailure;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TestLogger;

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Tests for the {@link ArchivedExecutionGraph}.
 */
public class ArchivedExecutionGraphTest extends TestLogger {

	private static ExecutionGraph runtimeGraph;

	@BeforeClass
	public static void setupExecutionGraph() throws Exception {
		// -------------------------------------------------------------------------------------------------------------
		// Setup
		// -------------------------------------------------------------------------------------------------------------

		JobVertexID v1ID = new JobVertexID();
		JobVertexID v2ID = new JobVertexID();

		JobVertex v1 = new JobVertex("v1", v1ID);
		JobVertex v2 = new JobVertex("v2", v2ID);

		v1.setParallelism(1);
		v2.setParallelism(2);

		v1.setInvokableClass(AbstractInvokable.class);
		v2.setInvokableClass(AbstractInvokable.class);

		JobGraph jobGraph = new JobGraph(v1, v2);
		ExecutionConfig config = new ExecutionConfig();

		config.setExecutionMode(ExecutionMode.BATCH_FORCED);
		config.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());
		config.setParallelism(4);
		config.enableObjectReuse();
		config.setGlobalJobParameters(new TestJobParameters());

		jobGraph.setExecutionConfig(config);

		runtimeGraph = TestingExecutionGraphBuilder
			.newBuilder()
			.setJobGraph(jobGraph)
			.build();

		runtimeGraph.start(ComponentMainThreadExecutorServiceAdapter.forMainThread());

		List<ExecutionJobVertex> jobVertices = new ArrayList<>();
		jobVertices.add(runtimeGraph.getJobVertex(v1ID));
		jobVertices.add(runtimeGraph.getJobVertex(v2ID));

		CheckpointStatsTracker statsTracker = new CheckpointStatsTracker(
				0,
				jobVertices,
				mock(CheckpointCoordinatorConfiguration.class),
				new UnregisteredMetricsGroup());

		CheckpointCoordinatorConfiguration chkConfig = new CheckpointCoordinatorConfiguration(
			100,
			100,
			100,
			1,
			CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
			true,
			false,
			false,
			0);

		runtimeGraph.enableCheckpointing(
			chkConfig,
			Collections.<ExecutionJobVertex>emptyList(),
			Collections.<ExecutionJobVertex>emptyList(),
			Collections.<ExecutionJobVertex>emptyList(),
			Collections.<MasterTriggerRestoreHook<?>>emptyList(),
			new StandaloneCheckpointIDCounter(),
			new StandaloneCompletedCheckpointStore(1),
			new MemoryStateBackend(),
			statsTracker);

		runtimeGraph.setJsonPlan("{}");

		runtimeGraph.getJobVertex(v2ID).getTaskVertices()[0].getCurrentExecutionAttempt().fail(new RuntimeException("This exception was thrown on purpose."));
	}

	@Test
	public void testArchive() throws IOException, ClassNotFoundException {
		ArchivedExecutionGraph archivedGraph = ArchivedExecutionGraph.createFrom(runtimeGraph);

		compareExecutionGraph(runtimeGraph, archivedGraph);
	}

	@Test
	public void testSerialization() throws IOException, ClassNotFoundException {
		ArchivedExecutionGraph archivedGraph = ArchivedExecutionGraph.createFrom(runtimeGraph);

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
		assertEquals(runtimeGraph.getFailureInfo().getExceptionAsString(), archivedGraph.getFailureInfo().getExceptionAsString());
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
		// CheckpointStats
		// -------------------------------------------------------------------------------------------------------------
		CheckpointStatsSnapshot runtimeSnapshot = runtimeGraph.getCheckpointStatsSnapshot();
		CheckpointStatsSnapshot archivedSnapshot = archivedGraph.getCheckpointStatsSnapshot();

		assertEquals(runtimeSnapshot.getSummaryStats().getEndToEndDurationStats().getAverage(), archivedSnapshot.getSummaryStats().getEndToEndDurationStats().getAverage());
		assertEquals(runtimeSnapshot.getSummaryStats().getEndToEndDurationStats().getMinimum(), archivedSnapshot.getSummaryStats().getEndToEndDurationStats().getMinimum());
		assertEquals(runtimeSnapshot.getSummaryStats().getEndToEndDurationStats().getMaximum(), archivedSnapshot.getSummaryStats().getEndToEndDurationStats().getMaximum());

		assertEquals(runtimeSnapshot.getSummaryStats().getStateSizeStats().getAverage(), archivedSnapshot.getSummaryStats().getStateSizeStats().getAverage());
		assertEquals(runtimeSnapshot.getSummaryStats().getStateSizeStats().getMinimum(), archivedSnapshot.getSummaryStats().getStateSizeStats().getMinimum());
		assertEquals(runtimeSnapshot.getSummaryStats().getStateSizeStats().getMaximum(), archivedSnapshot.getSummaryStats().getStateSizeStats().getMaximum());

		assertEquals(runtimeSnapshot.getCounts().getTotalNumberOfCheckpoints(), archivedSnapshot.getCounts().getTotalNumberOfCheckpoints());
		assertEquals(runtimeSnapshot.getCounts().getNumberOfCompletedCheckpoints(), archivedSnapshot.getCounts().getNumberOfCompletedCheckpoints());
		assertEquals(runtimeSnapshot.getCounts().getNumberOfInProgressCheckpoints(), archivedSnapshot.getCounts().getNumberOfInProgressCheckpoints());

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

		compareStringifiedAccumulators(runtimeJobVertex.getAggregatedUserAccumulatorsStringified(), archivedJobVertex.getAggregatedUserAccumulatorsStringified());

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

	private static void compareSerializedAccumulators(
			Map<String, SerializedValue<OptionalFailure<Object>>> runtimeAccs,
			Map<String, SerializedValue<OptionalFailure<Object>>> archivedAccs) throws IOException, ClassNotFoundException {
		assertEquals(runtimeAccs.size(), archivedAccs.size());
		for (Entry<String, SerializedValue<OptionalFailure<Object>>> runtimeAcc : runtimeAccs.entrySet()) {
			long runtimeUserAcc = (long) runtimeAcc.getValue().deserializeValue(ClassLoader.getSystemClassLoader()).getUnchecked();
			long archivedUserAcc = (long) archivedAccs.get(runtimeAcc.getKey()).deserializeValue(ClassLoader.getSystemClassLoader()).getUnchecked();

			assertEquals(runtimeUserAcc, archivedUserAcc);
		}
	}

	private static void verifySerializability(ArchivedExecutionGraph graph) throws IOException, ClassNotFoundException {
		ArchivedExecutionGraph copy = CommonTestUtils.createCopySerializable(graph);
		compareExecutionGraph(graph, copy);
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
