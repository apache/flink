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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.accumulators.AccumulatorSnapshot;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.blob.PermanentBlobService;
import org.apache.flink.runtime.blob.VoidBlobWriter;
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointRecoveryFactory;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.failover.RestartAllStrategy;
import org.apache.flink.runtime.executiongraph.restart.NoRestartStrategy;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.instance.SimpleSlot;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler;
import org.apache.flink.runtime.jobmanager.slots.ActorTaskManagerGateway;
import org.apache.flink.runtime.jobmaster.SlotOwner;
import org.apache.flink.runtime.operators.BatchTask;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.testutils.DirectScheduledExecutorService;
import org.apache.flink.util.TestLogger;

import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static junit.framework.TestCase.assertTrue;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.getInstance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link ExecutionGraph} deployment.
 */
public class ExecutionGraphDeploymentTest extends TestLogger {

	/**
	 * BLOB server instance to use for the job graph.
	 */
	protected BlobWriter blobWriter = VoidBlobWriter.getInstance();

	/**
	 * Permanent BLOB cache instance to use for the actor gateway that handles the {@link
	 * TaskDeploymentDescriptor} loading (may be <tt>null</tt>).
	 */
	protected PermanentBlobService blobCache = null;

	/**
	 * Checks that the job information for the given ID has been offloaded successfully (if
	 * offloading is used).
	 *
	 * @param eg           the execution graph that was created
	 */
	protected void checkJobOffloaded(ExecutionGraph eg) throws Exception {
		assertTrue(eg.getJobInformationOrBlobKey().isLeft());
	}

	/**
	 * Checks that the task information for the job vertex has been offloaded successfully (if
	 * offloading is used).
	 *
	 * @param eg           the execution graph that was created
	 * @param jobVertexId  job vertex ID
	 */
	protected void checkTaskOffloaded(ExecutionGraph eg, JobVertexID jobVertexId) throws Exception {
		assertTrue(eg.getJobVertex(jobVertexId).getTaskInformationOrBlobKey().isLeft());
	}

	@Test
	public void testBuildDeploymentDescriptor() {
		try {
			final JobID jobId = new JobID();

			final JobVertexID jid1 = new JobVertexID();
			final JobVertexID jid2 = new JobVertexID();
			final JobVertexID jid3 = new JobVertexID();
			final JobVertexID jid4 = new JobVertexID();

			JobVertex v1 = new JobVertex("v1", jid1);
			JobVertex v2 = new JobVertex("v2", jid2);
			JobVertex v3 = new JobVertex("v3", jid3);
			JobVertex v4 = new JobVertex("v4", jid4);

			v1.setParallelism(10);
			v2.setParallelism(10);
			v3.setParallelism(10);
			v4.setParallelism(10);

			v1.setInvokableClass(BatchTask.class);
			v2.setInvokableClass(BatchTask.class);
			v3.setInvokableClass(BatchTask.class);
			v4.setInvokableClass(BatchTask.class);

			v2.connectNewDataSetAsInput(v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
			v3.connectNewDataSetAsInput(v2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
			v4.connectNewDataSetAsInput(v2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

			final JobInformation expectedJobInformation = new DummyJobInformation(
				jobId,
				"some job");

			ExecutionGraph eg = new ExecutionGraph(
				expectedJobInformation,
				TestingUtils.defaultExecutor(),
				TestingUtils.defaultExecutor(),
				AkkaUtils.getDefaultTimeout(),
				new NoRestartStrategy(),
				new RestartAllStrategy.Factory(),
				new Scheduler(TestingUtils.defaultExecutionContext()),
				ExecutionGraph.class.getClassLoader(),
				blobWriter,
				AkkaUtils.getDefaultTimeout());

			checkJobOffloaded(eg);

			List<JobVertex> ordered = Arrays.asList(v1, v2, v3, v4);

			eg.attachJobGraph(ordered);

			ExecutionJobVertex ejv = eg.getAllVertices().get(jid2);
			ExecutionVertex vertex = ejv.getTaskVertices()[3];

			ExecutionGraphTestUtils.SimpleActorGatewayWithTDD instanceGateway =
				new ExecutionGraphTestUtils.SimpleActorGatewayWithTDD(
					TestingUtils.directExecutionContext(),
					blobCache);

			final Instance instance = getInstance(new ActorTaskManagerGateway(instanceGateway));

			final SimpleSlot slot = instance.allocateSimpleSlot();

			assertEquals(ExecutionState.CREATED, vertex.getExecutionState());

			vertex.deployToSlot(slot);

			assertEquals(ExecutionState.DEPLOYING, vertex.getExecutionState());
			checkTaskOffloaded(eg, vertex.getJobvertexId());

			TaskDeploymentDescriptor descr = instanceGateway.lastTDD;
			assertNotNull(descr);

			JobInformation jobInformation =
				descr.getSerializedJobInformation().deserializeValue(getClass().getClassLoader());
			TaskInformation taskInformation =
				descr.getSerializedTaskInformation().deserializeValue(getClass().getClassLoader());

			assertEquals(jobId, descr.getJobId());
			assertEquals(jobId, jobInformation.getJobId());
			assertEquals(jid2, taskInformation.getJobVertexId());
			assertEquals(3, descr.getSubtaskIndex());
			assertEquals(10, taskInformation.getNumberOfSubtasks());
			assertEquals(BatchTask.class.getName(), taskInformation.getInvokableClassName());
			assertEquals("v2", taskInformation.getTaskName());

			Collection<ResultPartitionDeploymentDescriptor> producedPartitions = descr.getProducedPartitions();
			Collection<InputGateDeploymentDescriptor> consumedPartitions = descr.getInputGates();

			assertEquals(2, producedPartitions.size());
			assertEquals(1, consumedPartitions.size());

			Iterator<ResultPartitionDeploymentDescriptor> iteratorProducedPartitions = producedPartitions.iterator();
			Iterator<InputGateDeploymentDescriptor> iteratorConsumedPartitions = consumedPartitions.iterator();

			assertEquals(10, iteratorProducedPartitions.next().getNumberOfSubpartitions());
			assertEquals(10, iteratorProducedPartitions.next().getNumberOfSubpartitions());
			assertEquals(10, iteratorConsumedPartitions.next().getInputChannelDeploymentDescriptors().length);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testRegistrationOfExecutionsFinishing() {
		try {
			final JobVertexID jid1 = new JobVertexID();
			final JobVertexID jid2 = new JobVertexID();

			JobVertex v1 = new JobVertex("v1", jid1);
			JobVertex v2 = new JobVertex("v2", jid2);

			Map<ExecutionAttemptID, Execution> executions = setupExecution(v1, 7650, v2, 2350).f1;

			for (Execution e : executions.values()) {
				e.markFinished();
			}

			assertEquals(0, executions.size());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testRegistrationOfExecutionsFailing() {
		try {

			final JobVertexID jid1 = new JobVertexID();
			final JobVertexID jid2 = new JobVertexID();

			JobVertex v1 = new JobVertex("v1", jid1);
			JobVertex v2 = new JobVertex("v2", jid2);

			Map<ExecutionAttemptID, Execution> executions = setupExecution(v1, 7, v2, 6).f1;

			for (Execution e : executions.values()) {
				e.markFailed(null);
			}

			assertEquals(0, executions.size());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testRegistrationOfExecutionsFailedExternally() {
		try {

			final JobVertexID jid1 = new JobVertexID();
			final JobVertexID jid2 = new JobVertexID();

			JobVertex v1 = new JobVertex("v1", jid1);
			JobVertex v2 = new JobVertex("v2", jid2);

			Map<ExecutionAttemptID, Execution> executions = setupExecution(v1, 7, v2, 6).f1;

			for (Execution e : executions.values()) {
				e.fail(null);
			}

			assertEquals(0, executions.size());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	/**
	 * Verifies that {@link ExecutionGraph#updateState(TaskExecutionState)} updates the accumulators and metrics for an
	 * execution that failed or was canceled.
	 */
	@Test
	public void testAccumulatorsAndMetricsForwarding() throws Exception {
		final JobVertexID jid1 = new JobVertexID();
		final JobVertexID jid2 = new JobVertexID();

		JobVertex v1 = new JobVertex("v1", jid1);
		JobVertex v2 = new JobVertex("v2", jid2);

		Tuple2<ExecutionGraph, Map<ExecutionAttemptID, Execution>> graphAndExecutions = setupExecution(v1, 1, v2, 1);
		ExecutionGraph graph = graphAndExecutions.f0;
		
		// verify behavior for canceled executions
		Execution execution1 = graphAndExecutions.f1.values().iterator().next();

		IOMetrics ioMetrics = new IOMetrics(0, 0, 0, 0, 0, 0.0, 0.0, 0.0, 0.0, 0.0);
		Map<String, Accumulator<?, ?>> accumulators = new HashMap<>();
		accumulators.put("acc", new IntCounter(4));
		AccumulatorSnapshot accumulatorSnapshot = new AccumulatorSnapshot(graph.getJobID(), execution1.getAttemptId(), accumulators);
		
		TaskExecutionState state = new TaskExecutionState(graph.getJobID(), execution1.getAttemptId(), ExecutionState.CANCELED, null, accumulatorSnapshot, ioMetrics);
		
		graph.updateState(state);
		
		assertEquals(ioMetrics, execution1.getIOMetrics());
		assertNotNull(execution1.getUserAccumulators());
		assertEquals(4, execution1.getUserAccumulators().get("acc").getLocalValue());
		
		// verify behavior for failed executions
		Execution execution2 = graphAndExecutions.f1.values().iterator().next();

		IOMetrics ioMetrics2 = new IOMetrics(0, 0, 0, 0, 0, 0.0, 0.0, 0.0, 0.0, 0.0);
		Map<String, Accumulator<?, ?>> accumulators2 = new HashMap<>();
		accumulators2.put("acc", new IntCounter(8));
		AccumulatorSnapshot accumulatorSnapshot2 = new AccumulatorSnapshot(graph.getJobID(), execution2.getAttemptId(), accumulators2);

		TaskExecutionState state2 = new TaskExecutionState(graph.getJobID(), execution2.getAttemptId(), ExecutionState.FAILED, null, accumulatorSnapshot2, ioMetrics2);

		graph.updateState(state2);

		assertEquals(ioMetrics2, execution2.getIOMetrics());
		assertNotNull(execution2.getUserAccumulators());
		assertEquals(8, execution2.getUserAccumulators().get("acc").getLocalValue());
	}

	/**
	 * Verifies that {@link Execution#cancelingComplete(Map, IOMetrics)} and {@link Execution#markFailed(Throwable, Map, IOMetrics)}
	 * store the given accumulators and metrics correctly.
	 */
	@Test
	public void testAccumulatorsAndMetricsStorage() throws Exception {
		final JobVertexID jid1 = new JobVertexID();
		final JobVertexID jid2 = new JobVertexID();

		JobVertex v1 = new JobVertex("v1", jid1);
		JobVertex v2 = new JobVertex("v2", jid2);

		Map<ExecutionAttemptID, Execution> executions = setupExecution(v1, 1, v2, 1).f1;
		
		IOMetrics ioMetrics = new IOMetrics(0, 0, 0, 0, 0, 0.0, 0.0, 0.0, 0.0, 0.0);
		Map<String, Accumulator<?, ?>> accumulators = Collections.emptyMap();

		Execution execution1 = executions.values().iterator().next();
		execution1.cancel();
		execution1.cancelingComplete(accumulators, ioMetrics);
		
		assertEquals(ioMetrics, execution1.getIOMetrics());
		assertEquals(accumulators, execution1.getUserAccumulators());

		Execution execution2 = executions.values().iterator().next();
		execution2.markFailed(new Throwable(), accumulators, ioMetrics);

		assertEquals(ioMetrics, execution2.getIOMetrics());
		assertEquals(accumulators, execution2.getUserAccumulators());
	}

	@Test
	public void testRegistrationOfExecutionsCanceled() {
		try {

			final JobVertexID jid1 = new JobVertexID();
			final JobVertexID jid2 = new JobVertexID();

			JobVertex v1 = new JobVertex("v1", jid1);
			JobVertex v2 = new JobVertex("v2", jid2);

			Map<ExecutionAttemptID, Execution> executions = setupExecution(v1, 19, v2, 37).f1;

			for (Execution e : executions.values()) {
				e.cancel();
				e.cancelingComplete();
			}

			assertEquals(0, executions.size());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	/**
	 * Tests that a blocking batch job fails if there are not enough resources left to schedule the
	 * succeeding tasks. This test case is related to [FLINK-4296] where finished producing tasks
	 * swallow the fail exception when scheduling a consumer task.
	 */
	@Test
	public void testNoResourceAvailableFailure() throws Exception {
		final JobID jobId = new JobID();
		JobVertex v1 = new JobVertex("source");
		JobVertex v2 = new JobVertex("sink");

		int dop1 = 1;
		int dop2 = 1;

		v1.setParallelism(dop1);
		v2.setParallelism(dop2);

		v1.setInvokableClass(BatchTask.class);
		v2.setInvokableClass(BatchTask.class);

		v2.connectNewDataSetAsInput(v1, DistributionPattern.POINTWISE, ResultPartitionType.BLOCKING);

		Scheduler scheduler = new Scheduler(TestingUtils.directExecutionContext());
		for (int i = 0; i < dop1; i++) {
			scheduler.newInstanceAvailable(
				ExecutionGraphTestUtils.getInstance(
					new ActorTaskManagerGateway(
						new ExecutionGraphTestUtils.SimpleActorGateway(
							TestingUtils.directExecutionContext()))));
		}

		final JobInformation jobInformation = new DummyJobInformation(
			jobId,
			"failing test job");

		// execution graph that executes actions synchronously
		ExecutionGraph eg = new ExecutionGraph(
			jobInformation,
			new DirectScheduledExecutorService(),
			TestingUtils.defaultExecutor(),
			AkkaUtils.getDefaultTimeout(),
			new NoRestartStrategy(),
			new RestartAllStrategy.Factory(),
			scheduler,
			ExecutionGraph.class.getClassLoader(),
			blobWriter,
			AkkaUtils.getDefaultTimeout());

		checkJobOffloaded(eg);

		eg.setQueuedSchedulingAllowed(false);

		List<JobVertex> ordered = Arrays.asList(v1, v2);
		eg.attachJobGraph(ordered);

		assertEquals(dop1, scheduler.getNumberOfAvailableSlots());

		// schedule, this triggers mock deployment
		eg.scheduleForExecution();

		ExecutionAttemptID attemptID = eg.getJobVertex(v1.getID()).getTaskVertices()[0].getCurrentExecutionAttempt().getAttemptId();
		eg.updateState(new TaskExecutionState(jobId, attemptID, ExecutionState.RUNNING));
		eg.updateState(new TaskExecutionState(jobId, attemptID, ExecutionState.FINISHED, null));

		assertEquals(JobStatus.FAILED, eg.getState());
	}

	// ------------------------------------------------------------------------
	//  retained checkpoints config test
	// ------------------------------------------------------------------------

	@Test
	public void testSettingDefaultMaxNumberOfCheckpointsToRetain() throws Exception {
		final Configuration jobManagerConfig = new Configuration();

		final ExecutionGraph eg = createExecutionGraph(jobManagerConfig);

		assertEquals(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS.defaultValue().intValue(),
				eg.getCheckpointCoordinator().getCheckpointStore().getMaxNumberOfRetainedCheckpoints());
	}

	@Test
	public void testSettingMaxNumberOfCheckpointsToRetain() throws Exception {

		final int maxNumberOfCheckpointsToRetain = 10;
		final Configuration jobManagerConfig = new Configuration();
		jobManagerConfig.setInteger(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS,
			maxNumberOfCheckpointsToRetain);

		final ExecutionGraph eg = createExecutionGraph(jobManagerConfig);

		assertEquals(maxNumberOfCheckpointsToRetain,
			eg.getCheckpointCoordinator().getCheckpointStore().getMaxNumberOfRetainedCheckpoints());
	}

	private Tuple2<ExecutionGraph, Map<ExecutionAttemptID, Execution>> setupExecution(JobVertex v1, int dop1, JobVertex v2, int dop2) throws Exception {
		final JobID jobId = new JobID();

		v1.setParallelism(dop1);
		v2.setParallelism(dop2);

		v1.setInvokableClass(BatchTask.class);
		v2.setInvokableClass(BatchTask.class);

		Scheduler scheduler = new Scheduler(TestingUtils.defaultExecutionContext());
		for (int i = 0; i < dop1 + dop2; i++) {
			scheduler.newInstanceAvailable(
					ExecutionGraphTestUtils.getInstance(
							new ActorTaskManagerGateway(
									new ExecutionGraphTestUtils.SimpleActorGateway(
											TestingUtils.directExecutionContext()))));
		}

		final JobInformation jobInformation = new DummyJobInformation(
			jobId,
			"some job");

		// execution graph that executes actions synchronously
		ExecutionGraph eg = new ExecutionGraph(
			jobInformation,
			new DirectScheduledExecutorService(),
			TestingUtils.defaultExecutor(),
			AkkaUtils.getDefaultTimeout(),
			new NoRestartStrategy(),
			new RestartAllStrategy.Factory(),
			scheduler,
			ExecutionGraph.class.getClassLoader(),
			blobWriter,
			AkkaUtils.getDefaultTimeout());
		checkJobOffloaded(eg);
		
		eg.setQueuedSchedulingAllowed(false);

		List<JobVertex> ordered = Arrays.asList(v1, v2);
		eg.attachJobGraph(ordered);

		assertEquals(dop1 + dop2, scheduler.getNumberOfAvailableSlots());

		// schedule, this triggers mock deployment
		eg.scheduleForExecution();

		Map<ExecutionAttemptID, Execution> executions = eg.getRegisteredExecutions();
		assertEquals(dop1 + dop2, executions.size());

		return new Tuple2<>(eg, executions);
	}

	@Test
	public void testSettingIllegalMaxNumberOfCheckpointsToRetain() throws Exception {

		final int negativeMaxNumberOfCheckpointsToRetain = -10;

		final Configuration jobManagerConfig = new Configuration();
		jobManagerConfig.setInteger(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS,
			negativeMaxNumberOfCheckpointsToRetain);

		final ExecutionGraph eg = createExecutionGraph(jobManagerConfig);

		assertNotEquals(negativeMaxNumberOfCheckpointsToRetain,
			eg.getCheckpointCoordinator().getCheckpointStore().getMaxNumberOfRetainedCheckpoints());

		assertEquals(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS.defaultValue().intValue(),
			eg.getCheckpointCoordinator().getCheckpointStore().getMaxNumberOfRetainedCheckpoints());
	}

	/**
	 * Tests that eager scheduling will wait until all input locations have been set before
	 * scheduling a task.
	 */
	@Test
	public void testEagerSchedulingWaitsOnAllInputPreferredLocations() throws Exception {
		final int parallelism = 2;
		final ProgrammedSlotProvider slotProvider = new ProgrammedSlotProvider(parallelism);

		final Time timeout = Time.hours(1L);
		final JobVertexID sourceVertexId = new JobVertexID();
		final JobVertex sourceVertex = new JobVertex("Test source", sourceVertexId);
		sourceVertex.setInvokableClass(NoOpInvokable.class);
		sourceVertex.setParallelism(parallelism);

		final JobVertexID sinkVertexId = new JobVertexID();
		final JobVertex sinkVertex = new JobVertex("Test sink", sinkVertexId);
		sinkVertex.setInvokableClass(NoOpInvokable.class);
		sinkVertex.setParallelism(parallelism);

		sinkVertex.connectNewDataSetAsInput(sourceVertex, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

		final Map<JobVertexID, CompletableFuture<LogicalSlot>[]> slotFutures = new HashMap<>(2);

		for (JobVertexID jobVertexID : Arrays.asList(sourceVertexId, sinkVertexId)) {
			CompletableFuture<LogicalSlot>[] slotFutureArray = new CompletableFuture[parallelism];

			for (int i = 0; i < parallelism; i++) {
				slotFutureArray[i] = new CompletableFuture<>();
			}

			slotFutures.put(jobVertexID, slotFutureArray);
			slotProvider.addSlots(jobVertexID, slotFutureArray);
		}

		final ScheduledExecutorService scheduledExecutorService = new ScheduledThreadPoolExecutor(3);

		final ExecutionGraph executionGraph = ExecutionGraphTestUtils.createExecutionGraph(
			new JobID(),
			slotProvider,
			new NoRestartStrategy(),
			scheduledExecutorService,
			timeout,
			sourceVertex,
			sinkVertex);

		executionGraph.setScheduleMode(ScheduleMode.EAGER);
		executionGraph.scheduleForExecution();

		// all tasks should be in state SCHEDULED
		for (ExecutionVertex executionVertex : executionGraph.getAllExecutionVertices()) {
			assertEquals(ExecutionState.SCHEDULED, executionVertex.getCurrentExecutionAttempt().getState());
		}

		// wait until the source vertex slots have been requested
		assertTrue(slotProvider.getSlotRequestedFuture(sourceVertexId, 0).get());
		assertTrue(slotProvider.getSlotRequestedFuture(sourceVertexId, 1).get());

		// check that the sinks have not requested their slots because they need the location
		// information of the sources
		assertFalse(slotProvider.getSlotRequestedFuture(sinkVertexId, 0).isDone());
		assertFalse(slotProvider.getSlotRequestedFuture(sinkVertexId, 1).isDone());

		final TaskManagerLocation localTaskManagerLocation = new LocalTaskManagerLocation();

		final SimpleSlot sourceSlot1 = createSlot(localTaskManagerLocation, 0);

		final SimpleSlot sourceSlot2 = createSlot(localTaskManagerLocation, 1);

		final SimpleSlot sinkSlot1 = createSlot(localTaskManagerLocation, 0);

		final SimpleSlot sinkSlot2 = createSlot(localTaskManagerLocation, 1);

		slotFutures.get(sourceVertexId)[0].complete(sourceSlot1);
		slotFutures.get(sourceVertexId)[1].complete(sourceSlot2);

		// wait until the sink vertex slots have been requested after we completed the source slots
		assertTrue(slotProvider.getSlotRequestedFuture(sinkVertexId, 0).get());
		assertTrue(slotProvider.getSlotRequestedFuture(sinkVertexId, 1).get());

		slotFutures.get(sinkVertexId)[0].complete(sinkSlot1);
		slotFutures.get(sinkVertexId)[1].complete(sinkSlot2);

		for (ExecutionVertex executionVertex : executionGraph.getAllExecutionVertices()) {
			ExecutionGraphTestUtils.waitUntilExecutionState(executionVertex.getCurrentExecutionAttempt(), ExecutionState.DEPLOYING, 5000L);
		}
	}

	private SimpleSlot createSlot(TaskManagerLocation taskManagerLocation, int index) {
		return new SimpleSlot(
			mock(SlotOwner.class),
			taskManagerLocation,
			index,
			new SimpleAckingTaskManagerGateway());
	}

	@SuppressWarnings("serial")
	public static class FailingFinalizeJobVertex extends JobVertex {

		public FailingFinalizeJobVertex(String name, JobVertexID id) {
			super(name, id);
		}

		@Override
		public void finalizeOnMaster(ClassLoader cl) throws Exception {
			throw new Exception();
		}
	}

	private ExecutionGraph createExecutionGraph(Configuration configuration) throws Exception {
		final ScheduledExecutorService executor = TestingUtils.defaultExecutor();

		final JobID jobId = new JobID();
		final JobGraph jobGraph = new JobGraph(jobId, "test");
		jobGraph.setSnapshotSettings(
			new JobCheckpointingSettings(
				Collections.<JobVertexID>emptyList(),
				Collections.<JobVertexID>emptyList(),
				Collections.<JobVertexID>emptyList(),
				new CheckpointCoordinatorConfiguration(
					100,
					10 * 60 * 1000,
					0,
					1,
					CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
					false),
				null));

		final Time timeout = Time.seconds(10L);
		return ExecutionGraphBuilder.buildGraph(
			null,
			jobGraph,
			configuration,
			executor,
			executor,
			new ProgrammedSlotProvider(1),
			getClass().getClassLoader(),
			new StandaloneCheckpointRecoveryFactory(),
			timeout,
			new NoRestartStrategy(),
			new UnregisteredMetricsGroup(),
			1,
			blobWriter,
			timeout,
			LoggerFactory.getLogger(getClass()));
	}
}
