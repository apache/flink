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
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.blob.PermanentBlobService;
import org.apache.flink.runtime.blob.VoidBlobWriter;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointRecoveryFactory;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.deployment.ResultPartitionLocationTrackerProxy;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.failover.FailoverRegion;
import org.apache.flink.runtime.executiongraph.failover.FailoverStrategy;
import org.apache.flink.runtime.executiongraph.failover.RestartAllStrategy;
import org.apache.flink.runtime.executiongraph.restart.NoRestartStrategy;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.executiongraph.utils.SimpleSlotProvider;
import org.apache.flink.runtime.instance.BaseTestingActorGateway;
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.instance.SimpleSlot;
import org.apache.flink.runtime.instance.SimpleSlotContext;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.GraphManager;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.SlotOwner;
import org.apache.flink.runtime.jobmaster.failover.OperationLogManager;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.TaskMessages.CancelTask;
import org.apache.flink.runtime.messages.TaskMessages.FailIntermediateResultPartitions;
import org.apache.flink.runtime.messages.TaskMessages.SubmitTask;
import org.apache.flink.runtime.schedule.GraphManagerPlugin;
import org.apache.flink.runtime.schedule.GraphManagerPluginFactory;
import org.apache.flink.runtime.schedule.SchedulingConfig;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.util.SerializedValue;

import akka.actor.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.lang.reflect.Field;
import java.net.InetAddress;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

import scala.concurrent.ExecutionContext;
import scala.concurrent.ExecutionContext$;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

/**
 * A collection of utility methods for testing the ExecutionGraph and its related classes.
 */
public class ExecutionGraphTestUtils {

	private static final Logger TEST_LOGGER = LoggerFactory.getLogger(ExecutionGraphTestUtils.class);

	// ------------------------------------------------------------------------
	//  reaching states
	// ------------------------------------------------------------------------

	/**
	 * Waits until the job has reached a certain state.
	 *
	 * <p>This method is based on polling and might miss very fast state transitions!
	 */
	public static void waitUntilJobStatus(ExecutionGraph eg, JobStatus status, long maxWaitMillis)
			throws TimeoutException {

		checkNotNull(eg);
		checkNotNull(status);
		checkArgument(maxWaitMillis >= 0);

		// this is a poor implementation - we may want to improve it eventually
		final long deadline = maxWaitMillis == 0 ? Long.MAX_VALUE : System.nanoTime() + (maxWaitMillis * 1_000_000);

		while (eg.getState() != status && System.nanoTime() < deadline) {
			try {
				Thread.sleep(2);
			} catch (InterruptedException ignored) {}
		}

		if (System.nanoTime() >= deadline) {
			throw new TimeoutException();
		}
	}

	/**
	 * Waits until the Execution has reached a certain state.
	 *
	 * <p>This method is based on polling and might miss very fast state transitions!
	 */
	public static void waitUntilExecutionState(Execution execution, ExecutionState state, long maxWaitMillis)
			throws TimeoutException {

		checkNotNull(execution);
		checkNotNull(state);
		checkArgument(maxWaitMillis >= 0);

		// this is a poor implementation - we may want to improve it eventually
		final long deadline = maxWaitMillis == 0 ? Long.MAX_VALUE : System.nanoTime() + (maxWaitMillis * 1_000_000);

		while (execution.getState() != state && System.nanoTime() < deadline) {
			try {
				Thread.sleep(2);
			} catch (InterruptedException ignored) {}
		}

		if (System.nanoTime() >= deadline) {
			throw new TimeoutException();
		}
	}

	/**
	 * Waits until the Execution vertex has reached a certain state.
	 *
	 * <p>This method is based on polling and might miss very fast state transitions!
	 */
	public static void waitUntilExecutionVertexState(ExecutionVertex ev, ExecutionState state, long maxWaitMillis)
			throws TimeoutException {

		checkNotNull(ev);
		checkNotNull(state);
		checkArgument(maxWaitMillis >= 0);

		// this is a poor implementation - we may want to improve it eventually
		final long deadline = maxWaitMillis == 0 ? Long.MAX_VALUE : System.nanoTime() + (maxWaitMillis * 1_000_000);

		while (ev.getExecutionState() != state && System.nanoTime() < deadline) {
			try {
				Thread.sleep(2);
			} catch (InterruptedException ignored) {}
		}

		if (System.nanoTime() >= deadline) {
			throw new TimeoutException();
		}
	}


	/**
	 * Waits until all executions fulfill the given predicate.
	 *
	 * @param executionGraph for which to check the executions
	 * @param executionPredicate predicate which is to be fulfilled
	 * @param maxWaitMillis timeout for the wait operation
	 * @throws TimeoutException if the executions did not reach the target state in time
	 */
	public static void waitForAllExecutionsPredicate(
			ExecutionGraph executionGraph,
			Predicate<Execution> executionPredicate,
			long maxWaitMillis) throws TimeoutException {
		final Iterable<ExecutionVertex> allExecutionVertices = executionGraph.getAllExecutionVertices();

		final Deadline deadline = Deadline.fromNow(Duration.ofMillis(maxWaitMillis));
		boolean predicateResult;

		do {
			predicateResult = true;
			for (ExecutionVertex executionVertex : allExecutionVertices) {
				final Execution currentExecution = executionVertex.getCurrentExecutionAttempt();

				if (currentExecution == null || !executionPredicate.test(currentExecution)) {
					predicateResult = false;
					break;
				}
			}

			if (!predicateResult) {
				try {
					Thread.sleep(2L);
				} catch (InterruptedException ignored) {
					Thread.currentThread().interrupt();
				}
			}
		} while (!predicateResult && deadline.hasTimeLeft());

		if (!predicateResult) {
			throw new TimeoutException("Not all executions fulfilled the predicate in time.");
		}
	}

	/**
	 * Predicate which is true if the given {@link Execution} has a resource assigned.
	 */
	static final Predicate<Execution> hasResourceAssigned = (Execution execution) -> execution.getAssignedResource() != null;

	public static Predicate<Execution> isInExecutionState(ExecutionState executionState) {
		return (Execution execution) -> execution.getState() == executionState;
	}

	public static void waitUntilFailoverRegionState(FailoverRegion region, JobStatus status, long maxWaitMillis)
			throws TimeoutException {

		checkNotNull(region);
		checkNotNull(status);
		checkArgument(maxWaitMillis >= 0);

		// this is a poor implementation - we may want to improve it eventually
		final long deadline = maxWaitMillis == 0 ? Long.MAX_VALUE : System.nanoTime() + (maxWaitMillis * 1_000_000);

		while (region.getState() != status && System.nanoTime() < deadline) {
			try {
				Thread.sleep(2);
			} catch (InterruptedException ignored) {}
		}

		if (System.nanoTime() >= deadline) {
			throw new TimeoutException();
		}
	}

	public static void failExecutionGraph(ExecutionGraph executionGraph, Exception cause) {
		executionGraph.getAllExecutionVertices().iterator().next().fail(cause);
		assertEquals(JobStatus.FAILING, executionGraph.getState());

		for (ExecutionVertex vertex : executionGraph.getAllExecutionVertices()) {
			vertex.getCurrentExecutionAttempt().cancelingComplete();
		}
	}

	/**
	 * Takes all vertices in the given ExecutionGraph and switches their current
	 * execution to RUNNING.
	 */
	public static void switchAllVerticesToRunning(ExecutionGraph eg) {
		for (ExecutionVertex vertex : eg.getAllExecutionVertices()) {
			vertex.getCurrentExecutionAttempt().switchToRunning();
		}
	}

	/**
	 * Takes all vertices in the given ExecutionGraph and attempts to move them
	 * from CANCELING to CANCELED.
	 */
	public static void completeCancellingForAllVertices(ExecutionGraph eg) {
		for (ExecutionVertex vertex : eg.getAllExecutionVertices()) {
			vertex.getCurrentExecutionAttempt().cancelingComplete();
		}
	}

	/**
	 * Takes all vertices in the given ExecutionGraph and switches their current
	 * execution to FINISHED.
	 */
	public static void finishAllVertices(ExecutionGraph eg) {
		for (ExecutionVertex vertex : eg.getAllExecutionVertices()) {
			vertex.getCurrentExecutionAttempt().markFinished();
		}
	}

	/**
	 * Checks that all execution are in state DEPLOYING and then switches them
	 * to state RUNNING
	 */
	public static void switchToRunning(ExecutionGraph eg) {
		// check that all execution are in state DEPLOYING
		for (ExecutionVertex ev : eg.getAllExecutionVertices()) {
			final Execution exec = ev.getCurrentExecutionAttempt();
			final ExecutionState executionState = exec.getState();
			assert executionState == ExecutionState.DEPLOYING : "Expected executionState to be DEPLOYING, was: " + executionState;
		}

		// switch executions to RUNNING
		for (ExecutionVertex ev : eg.getAllExecutionVertices()) {
			final Execution exec = ev.getCurrentExecutionAttempt();
			exec.switchToRunning();
		}
	}

	// ------------------------------------------------------------------------
	//  state modifications
	// ------------------------------------------------------------------------

	public static void setVertexState(ExecutionVertex vertex, ExecutionState state) {
		try {
			Execution exec = vertex.getCurrentExecutionAttempt();

			Field f = Execution.class.getDeclaredField("state");
			f.setAccessible(true);
			f.set(exec, state);
		}
		catch (Exception e) {
			throw new RuntimeException("Modifying the state failed", e);
		}
	}

	public static void setVertexResource(ExecutionVertex vertex, SimpleSlot slot) {
		Execution exec = vertex.getCurrentExecutionAttempt();

		if (!exec.tryAssignResource(slot)) {
			throw new RuntimeException("Could not assign resource.");
		}
	}

	// ------------------------------------------------------------------------
	//  Mocking Slots
	// ------------------------------------------------------------------------

	public static SimpleSlot createMockSimpleSlot(TaskManagerGateway gateway) {
		final TaskManagerLocation location = new TaskManagerLocation(
				ResourceID.generate(), InetAddress.getLoopbackAddress(), 6572);

		final SimpleSlotContext allocatedSlot = new SimpleSlotContext(
			new AllocationID(),
			location,
			0,
			gateway);

		return new SimpleSlot(
			allocatedSlot,
			mock(SlotOwner.class),
			0);
	}

	// ------------------------------------------------------------------------
	//  Mocking ExecutionGraph
	// ------------------------------------------------------------------------

	/**
	 * Creates an execution graph with on job vertex of parallelism 10 that does no restarts.
	 */
	public static ExecutionGraph createSimpleTestGraph() throws Exception {
		return createSimpleTestGraph(new NoRestartStrategy());
	}

	/**
	 * Creates an execution graph with on job vertex of parallelism 10, using the given
	 * restart strategy.
	 */
	public static ExecutionGraph createSimpleTestGraph(RestartStrategy restartStrategy) throws Exception {
		JobVertex vertex = createNoOpVertex(10);

		return createSimpleTestGraph(new JobID(), new SimpleAckingTaskManagerGateway(), restartStrategy, vertex);
	}

	/**
	 * Creates an execution graph containing the given vertices.
	 *
	 * <p>The execution graph uses {@link NoRestartStrategy} as the restart strategy.
	 */
	public static ExecutionGraph createSimpleTestGraph(JobID jid, JobVertex... vertices) throws Exception {
		return createSimpleTestGraph(jid, new SimpleAckingTaskManagerGateway(), new NoRestartStrategy(), vertices);
	}

	/**
	 * Creates an execution graph containing the given vertices and the given restart strategy.
	 */
	public static ExecutionGraph createSimpleTestGraph(
			JobID jid,
			TaskManagerGateway taskManagerGateway,
			RestartStrategy restartStrategy,
			JobVertex... vertices) throws Exception {

		int numSlotsNeeded = 0;
		for (JobVertex vertex : vertices) {
			numSlotsNeeded += vertex.getParallelism();
		}

		SlotProvider slotProvider = new SimpleSlotProvider(jid, numSlotsNeeded, taskManagerGateway);

		return createSimpleTestGraph(jid, slotProvider, restartStrategy, vertices);
	}

	public static ExecutionGraph createSimpleTestGraph(
			JobID jid,
			SlotProvider slotProvider,
			RestartStrategy restartStrategy,
			JobVertex... vertices) throws Exception {

		return createSimpleTestGraph(jid, slotProvider, restartStrategy, TestingUtils.defaultExecutor(), vertices);
	}

	public static ExecutionGraph createSimpleTestGraph(
			JobID jid,
			SlotProvider slotProvider,
			RestartStrategy restartStrategy,
			ScheduledExecutorService executor,
			JobVertex... vertices) throws Exception {

		return createSimpleTestGraph(
			jid,
			new Configuration(),
			slotProvider,
			restartStrategy,
			executor,
			vertices);
	}

	public static ExecutionGraph createSimpleTestGraph(
		JobID jid,
		Configuration jobManagerConfiguration,
		SlotProvider slotProvider,
		RestartStrategy restartStrategy,
		ScheduledExecutorService executor,
		JobVertex... vertices) throws Exception {

		checkNotNull(jid);
		checkNotNull(vertices);

		return createExecutionGraph(
			new JobGraph(jid, "test job", vertices),
			jobManagerConfiguration,
			slotProvider,
			restartStrategy,
			executor,
			Time.seconds(10L));
	}

	public static ExecutionGraph createExecutionGraph(
		JobGraph jobGraph,
		TaskManagerGateway taskManagerGateway,
		RestartStrategy restartStrategy) throws Exception {

		int numSlotsNeeded = 0;
		for (JobVertex vertex : jobGraph.getVerticesSortedTopologicallyFromSources()) {
			numSlotsNeeded += vertex.getParallelism();
		}

		SlotProvider slotProvider = new SimpleSlotProvider(jobGraph.getJobID(), numSlotsNeeded, taskManagerGateway);

		return createExecutionGraph(jobGraph, slotProvider, restartStrategy);
	}

	public static ExecutionGraph createExecutionGraph(
		JobGraph jobGraph,
		SlotProvider slotProvider,
		RestartStrategy restartStrategy) throws Exception {

		return createExecutionGraph(
			jobGraph,
			slotProvider,
			restartStrategy,
			TestingUtils.defaultExecutor());
	}

	public static ExecutionGraph createExecutionGraph(
		JobGraph jobGraph,
		SlotProvider slotProvider,
		RestartStrategy restartStrategy,
		ScheduledExecutorService executor) throws Exception {

		return createExecutionGraph(
			jobGraph,
			slotProvider,
			restartStrategy,
			executor,
			Time.seconds(10L));
	}

	public static ExecutionGraph createExecutionGraph(
		JobGraph jobGraph,
		SlotProvider slotProvider,
		RestartStrategy restartStrategy,
		ScheduledExecutorService executor,
		Time timeout) throws Exception {

		return createExecutionGraph(
			jobGraph,
			new Configuration(),
			slotProvider,
			restartStrategy,
			executor,
			timeout);
	}

	public static ExecutionGraph createExecutionGraph(
		JobGraph jobGraph,
		Configuration jobManagerConfiguration,
		SlotProvider slotProvider,
		RestartStrategy restartStrategy,
		ScheduledExecutorService executor,
		Time timeout) throws Exception {
		return createExecutionGraph(
			null,
			jobGraph,
			jobManagerConfiguration,
			executor,
			executor,
			slotProvider,
			ExecutionGraphTestUtils.class.getClassLoader(),
			new StandaloneCheckpointRecoveryFactory(),
			timeout,
			restartStrategy,
			new UnregisteredMetricsGroup(),
			1,
			VoidBlobWriter.getInstance(),
			timeout,
			TEST_LOGGER
		);
	}

	/**
	 * Create an ExecutionGraph with ExecutionGraphBuilder.
	 */
	public static ExecutionGraph createExecutionGraph(
		@Nullable ExecutionGraph prior,
		JobGraph jobGraph,
		Configuration jobManagerConfig,
		ScheduledExecutorService futureExecutor,
		Executor ioExecutor,
		SlotProvider slotProvider,
		ClassLoader classLoader,
		CheckpointRecoveryFactory recoveryFactory,
		Time rpcTimeout,
		RestartStrategy restartStrategy,
		MetricGroup metrics,
		int parallelismForAutoMax,
		BlobWriter blobWriter,
		Time allocationTimeout,
		Logger log) throws Exception {

		ExecutionGraph eg = ExecutionGraphBuilder.buildGraph(
			prior,
			jobGraph,
			jobManagerConfig,
			futureExecutor,
			ioExecutor,
			slotProvider,
			classLoader,
			recoveryFactory,
			rpcTimeout,
			restartStrategy,
			metrics,
			parallelismForAutoMax,
			blobWriter,
			new ResultPartitionLocationTrackerProxy(jobGraph.getJobConfiguration()),
			allocationTimeout,
			log);
		Configuration conf = new Configuration(jobGraph.getJobConfiguration());
		conf.addAll(jobGraph.getSchedulingConfiguration());
		GraphManagerPlugin graphManagerPlugin = GraphManagerPluginFactory.createGraphManagerPlugin(conf, classLoader);
		GraphManager graphManager = new GraphManager(
				graphManagerPlugin,
				mock(JobMasterGateway.class),
				mock(OperationLogManager.class),
				eg);
		graphManager.open(jobGraph, new SchedulingConfig(conf, ExecutionGraphTestUtils.class.getClassLoader()));
		eg.setGraphManager(graphManager);
		return eg;
	}

	/**
	 * Create an ExecutionGraph without extra decorations in ExecutionGraphBuilder, such as checkpoint and metrics.
	 */
	public static ExecutionGraph createExecutionGraphDirectly(
		ScheduledExecutorService futureExecutor,
		Executor ioExecutor,
		JobID jobId,
		String jobName,
		Configuration jobConfig,
		SerializedValue<ExecutionConfig> serializedConfig,
		Time timeout,
		RestartStrategy restartStrategy,
		SlotProvider slotProvider,
		List<JobVertex> vertices) throws Exception {

		return createExecutionGraphDirectly(
			new JobInformation(
				jobId,
				jobName,
				serializedConfig,
				jobConfig,
				Collections.emptyList(),
				Collections.emptyList()),
			futureExecutor,
			ioExecutor,
			timeout,
			restartStrategy,
			new RestartAllStrategy.Factory(),
			slotProvider,
			vertices);
	}

	/**
	 * Create an ExecutionGraph without extra decorations in ExecutionGraphBuilder, such as checkpoint and metrics.
	 */
	public static ExecutionGraph createExecutionGraphDirectly(
		JobInformation jobInformation,
		ScheduledExecutorService futureExecutor,
		Executor ioExecutor,
		Time timeout,
		RestartStrategy restartStrategy,
		FailoverStrategy.Factory failoverStrategy,
		SlotProvider slotProvider,
		List<JobVertex> vertices) throws Exception {

		return createExecutionGraphDirectly(
			jobInformation,
			futureExecutor,
			ioExecutor,
			timeout,
			restartStrategy,
			failoverStrategy,
			slotProvider,
			VoidBlobWriter.getInstance(),
			vertices);
	}

	/**
	 * Create an ExecutionGraph without extra decorations in ExecutionGraphBuilder, such as checkpoint and metrics.
	 */
	public static ExecutionGraph createExecutionGraphDirectly(
		JobGraph jobGraph,
		ScheduledExecutorService futureExecutor,
		Executor ioExecutor,
		Time timeout,
		RestartStrategy restartStrategy,
		FailoverStrategy.Factory failoverStrategy,
		SlotProvider slotProvider,
		BlobWriter blobWriter) throws Exception {

		final JobInformation jobInformation = new JobInformation(
			jobGraph.getJobID(),
			jobGraph.getName(),
			jobGraph.getSerializedExecutionConfig(),
			jobGraph.getJobConfiguration(),
			jobGraph.getUserJarBlobKeys(),
			jobGraph.getClasspaths());
		jobInformation.getJobConfiguration().addAll(jobGraph.getSchedulingConfiguration());

		return createExecutionGraphDirectly(
			jobInformation,
			futureExecutor,
			ioExecutor,
			timeout,
			restartStrategy,
			failoverStrategy,
			slotProvider,
			blobWriter,
			jobGraph.getVerticesSortedTopologicallyFromSources());
	}

	/**
	 * Create an ExecutionGraph without extra decorations in ExecutionGraphBuilder, such as checkpoint and metrics.
	 */
	public static ExecutionGraph createExecutionGraphDirectly(
		JobInformation jobInformation,
		ScheduledExecutorService futureExecutor,
		Executor ioExecutor,
		Time timeout,
		RestartStrategy restartStrategy,
		FailoverStrategy.Factory failoverStrategy,
		SlotProvider slotProvider,
		BlobWriter blobWriter,
		List<JobVertex> vertices) throws Exception {

		JobGraph jobGraph = new JobGraph(jobInformation.getJobId(), jobInformation.getJobName());
		jobGraph.getJobConfiguration().addAll(jobInformation.getJobConfiguration());
		jobGraph.addVertices(vertices);

		ClassLoader classLoader = ExecutionGraph.class.getClassLoader();
		ExecutionGraph eg = new ExecutionGraph(
			jobInformation,
			futureExecutor,
			ioExecutor,
			timeout,
			restartStrategy,
			failoverStrategy,
			slotProvider,
			classLoader,
			blobWriter,
			new ResultPartitionLocationTrackerProxy(jobInformation.getJobConfiguration()),
			timeout,
			new Configuration());

		eg.attachJobGraph(vertices);

		Configuration conf = new Configuration(jobGraph.getJobConfiguration());
		conf.addAll(jobGraph.getSchedulingConfiguration());
		GraphManagerPlugin graphManagerPlugin = GraphManagerPluginFactory.createGraphManagerPlugin(conf, classLoader);
		GraphManager graphManager = new GraphManager(
				graphManagerPlugin,
				mock(JobMasterGateway.class),
				mock(OperationLogManager.class),
				eg);
		graphManager.open(jobGraph, new SchedulingConfig(conf, ExecutionGraphTestUtils.class.getClassLoader()));
		eg.setGraphManager(graphManager);

		return eg;
	}

	public static ExecutionGraph createExecutionGraphWithSharingSlots(
			Configuration jobManagerConfiguration,
			String graphManagerPluginType,
			SlotProvider slotProvider,
			int[] parallelism,
			ResourceSpec[] resources,
			DistributionPattern[] distributionPatterns) throws Exception {

		SlotSharingGroup slotSharingGroup = new SlotSharingGroup();

		JobVertex[] vertices = new JobVertex[parallelism.length];
		for (int i = 0; i < parallelism.length; ++i) {
			vertices[i] = new JobVertex("vertex " + i);
			vertices[i].setInvokableClass(NoOpInvokable.class);
			vertices[i].setParallelism(parallelism[i]);
			vertices[i].setResources(resources[i], resources[i]);

			vertices[i].setSlotSharingGroup(slotSharingGroup);

			if (i >= 1) {
				vertices[i].connectNewDataSetAsInput(vertices[i - 1], distributionPatterns[i - 1], ResultPartitionType.PIPELINED);
			}
		}

		JobGraph jobGraph = new JobGraph(vertices);
		jobGraph.getSchedulingConfiguration().setString(JobManagerOptions.GRAPH_MANAGER_PLUGIN, graphManagerPluginType);

		return ExecutionGraphTestUtils.createExecutionGraph(
			jobGraph,
			jobManagerConfiguration,
			slotProvider,
			new NoRestartStrategy(),
			TestingUtils.defaultExecutor(),
			Time.seconds(10));
	}

	public static JobVertex createNoOpVertex(int parallelism) {
		JobVertex vertex = new JobVertex("vertex");
		vertex.setInvokableClass(NoOpInvokable.class);
		vertex.setParallelism(parallelism);
		return vertex;
	}

	// ------------------------------------------------------------------------
	//  utility mocking methods
	// ------------------------------------------------------------------------

	public static Instance getInstance(final TaskManagerGateway gateway) throws Exception {
		return getInstance(gateway, 1);
	}

	public static Instance getInstance(final TaskManagerGateway gateway, final int numberOfSlots) throws Exception {
		ResourceID resourceID = ResourceID.generate();
		HardwareDescription hardwareDescription = new HardwareDescription(4, 2L*1024*1024*1024, 1024*1024*1024, 512*1024*1024);
		InetAddress address = InetAddress.getByName("127.0.0.1");
		TaskManagerLocation connection = new TaskManagerLocation(resourceID, address, 10001);

		return new Instance(gateway, connection, new InstanceID(), hardwareDescription, numberOfSlots);
	}

	public static JobVertex createJobVertex(String task1, int numTasks, Class<NoOpInvokable> invokable) {
		JobVertex groupVertex = new JobVertex(task1);
		groupVertex.setInvokableClass(invokable);
		groupVertex.setParallelism(numTasks);
		return groupVertex;
	}

	@SuppressWarnings("serial")
	public static class SimpleActorGateway extends BaseTestingActorGateway {

		public SimpleActorGateway(ExecutionContext executionContext){
			super(executionContext);
		}

		public void handleSubmittedTask(Object message) {}

		public void handleCanceledTask(Object message) {}

		@Override
		public Object handleMessage(Object message) {
			if (message instanceof SubmitTask) {
				handleSubmittedTask(message);
				return Acknowledge.get();
			} else if(message instanceof CancelTask) {
				handleCanceledTask(message);
				return Acknowledge.get();
			} else if(message instanceof FailIntermediateResultPartitions) {
				return new Object();
			} else {
				return null;
			}
		}
	}

	@SuppressWarnings("serial")
	public static class SimpleActorGatewayWithTDD extends SimpleActorGateway {

		public TaskDeploymentDescriptor lastTDD;
		private final PermanentBlobService blobCache;

		public SimpleActorGatewayWithTDD(ExecutionContext executionContext, PermanentBlobService blobCache) {
			super(executionContext);
			this.blobCache = blobCache;
		}

		@Override
		public Object handleMessage(Object message) {
			if(message instanceof SubmitTask) {
				SubmitTask submitTask = (SubmitTask) message;
				lastTDD = submitTask.tasks();
				try {
					lastTDD.loadBigData(blobCache);
					return Acknowledge.get();
				} catch (Exception e) {
					e.printStackTrace();
					return new Status.Failure(e);
				}
			} else {
				return super.handleMessage(message);
			}
		}
	}

	@SuppressWarnings("serial")
	public static class SimpleFailingActorGateway extends BaseTestingActorGateway {

		public SimpleFailingActorGateway(ExecutionContext executionContext) {
			super(executionContext);
		}

		@Override
		public Object handleMessage(Object message) throws Exception {
			if(message instanceof SubmitTask) {
				throw new Exception(ERROR_MESSAGE);
			} else if (message instanceof CancelTask) {
				CancelTask cancelTask = (CancelTask) message;

				return Acknowledge.get();
			} else {
				return null;
			}
		}
	}

	public static final String ERROR_MESSAGE = "test_failure_error_message";

	public static ExecutionJobVertex getExecutionVertex(
			JobVertexID id, ScheduledExecutorService executor)
		throws Exception {

		JobVertex ajv = new JobVertex("TestVertex", id);
		ajv.setInvokableClass(mock(AbstractInvokable.class).getClass());

		ExecutionGraph graph = new ExecutionGraph(
			new JobInformation(
				new JobID(),
				"test job",
				new SerializedValue<>(new ExecutionConfig()),
				new Configuration(),
				Collections.emptyList(),
				Collections.emptyList()),
			executor,
			executor,
			AkkaUtils.getDefaultTimeout(),
			new NoRestartStrategy(),
			new RestartAllStrategy.Factory(),
			new Scheduler(ExecutionContext$.MODULE$.fromExecutor(executor)),
			ExecutionGraph.class.getClassLoader(),
			VoidBlobWriter.getInstance(),
			new ResultPartitionLocationTrackerProxy(new Configuration()),
			AkkaUtils.getDefaultTimeout(),
			new Configuration());

		return spy(new ExecutionJobVertex(graph, ajv, 1, AkkaUtils.getDefaultTimeout()));
	}

	public static ExecutionJobVertex getExecutionVertex(JobVertexID id) throws Exception {
		return getExecutionVertex(id, TestingUtils.defaultExecutor());
	}

	// ------------------------------------------------------------------------
	//  graph vertex verifications
	// ------------------------------------------------------------------------

	/**
	 * Verifies the generated {@link ExecutionJobVertex} for a given {@link JobVertex} in a {@link ExecutionGraph}
	 *
	 * @param executionGraph the generated execution graph
	 * @param originJobVertex the vertex to verify for
	 * @param inputJobVertices upstream vertices of the verified vertex, used to check inputs of generated vertex
	 * @param outputJobVertices downstream vertices of the verified vertex, used to
	 *                          check produced data sets of generated vertex
	 */
	public static void verifyGeneratedExecutionJobVertex(
			ExecutionGraph executionGraph,
			JobVertex originJobVertex,
			@Nullable List<JobVertex> inputJobVertices,
			@Nullable List<JobVertex> outputJobVertices) {

		ExecutionJobVertex ejv = executionGraph.getAllVertices().get(originJobVertex.getID());
		assertNotNull(ejv);

		// verify basic properties
		assertEquals(originJobVertex.getParallelism(), ejv.getParallelism());
		assertEquals(executionGraph.getJobID(), ejv.getJobId());
		assertEquals(originJobVertex.getID(), ejv.getJobVertexId());
		assertEquals(originJobVertex, ejv.getJobVertex());

		// verify produced data sets
		if (outputJobVertices == null) {
			assertEquals(0, ejv.getProducedDataSets().length);
		} else {
			assertEquals(outputJobVertices.size(), ejv.getProducedDataSets().length);
			for (int i = 0; i < outputJobVertices.size(); i++) {
				assertEquals(originJobVertex.getProducedDataSets().get(i).getId(), ejv.getProducedDataSets()[i].getId());
				assertEquals(originJobVertex.getParallelism(), ejv.getProducedDataSets()[0].getPartitions().length);
			}
		}

		// verify task vertices for their basic properties and their inputs
		assertEquals(originJobVertex.getParallelism(), ejv.getTaskVertices().length);

		int subtaskIndex = 0;
		for (ExecutionVertex ev : ejv.getTaskVertices()) {
			assertEquals(executionGraph.getJobID(), ev.getJobId());
			assertEquals(originJobVertex.getID(), ev.getJobvertexId());

			assertEquals(originJobVertex.getParallelism(), ev.getTotalNumberOfParallelSubtasks());
			assertEquals(subtaskIndex, ev.getParallelSubtaskIndex());

			if (inputJobVertices == null) {
				assertEquals(0, ev.getNumberOfInputs());
			} else {
				assertEquals(inputJobVertices.size(), ev.getNumberOfInputs());

				for (int i = 0; i < inputJobVertices.size(); i++) {
					ExecutionEdge[] inputEdges = ev.getInputEdges(i);
					assertEquals(inputJobVertices.get(i).getParallelism(), inputEdges.length);

					int expectedPartitionNum = 0;
					for (ExecutionEdge inEdge : inputEdges) {
						assertEquals(i, inEdge.getInputNum());
						assertEquals(expectedPartitionNum, inEdge.getSource().getPartitionNumber());

						expectedPartitionNum++;
					}
				}
			}

			subtaskIndex++;
		}
	}
}
