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
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointRecoveryFactory;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.failover.FailoverRegion;
import org.apache.flink.runtime.executiongraph.restart.NoRestartStrategy;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.runtime.executiongraph.utils.SimpleSlotProvider;
import org.apache.flink.runtime.instance.BaseTestingActorGateway;
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.instance.SimpleSlot;
import org.apache.flink.runtime.instance.SlotProvider;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler;
import org.apache.flink.runtime.jobmanager.slots.AllocatedSlot;
import org.apache.flink.runtime.jobmanager.slots.SlotOwner;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.TaskMessages.CancelTask;
import org.apache.flink.runtime.messages.TaskMessages.FailIntermediateResultPartitions;
import org.apache.flink.runtime.messages.TaskMessages.SubmitTask;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.util.SerializedValue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.concurrent.ExecutionContext;
import scala.concurrent.ExecutionContext$;

import java.lang.reflect.Field;
import java.net.InetAddress;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

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
	 * Turns a newly scheduled execution graph into a state where all vertices run.
	 * This waits until all executions have reached state 'DEPLOYING' and then switches them to running.
	 */
	public static void waitUntilDeployedAndSwitchToRunning(ExecutionGraph eg, long timeout) throws TimeoutException {
		// wait until everything is running
		for (ExecutionVertex ev : eg.getAllExecutionVertices()) {
			final Execution exec = ev.getCurrentExecutionAttempt();
			waitUntilExecutionState(exec, ExecutionState.DEPLOYING, timeout);
		}

		// Note: As ugly as it is, we need this minor sleep, because between switching
		// to 'DEPLOYED' and when the 'switchToRunning()' may be called lies a race check
		// against concurrent modifications (cancel / fail). We can only switch this to running
		// once that check is passed. For the actual runtime, this switch is triggered by a callback
		// from the TaskManager, which comes strictly after that. For tests, we use mock TaskManagers
		// which cannot easily tell us when that condition has happened, unfortunately.
		try {
			Thread.sleep(2);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}

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
		try {
			Execution exec = vertex.getCurrentExecutionAttempt();
			
			Field f = Execution.class.getDeclaredField("assignedResource");
			f.setAccessible(true);
			f.set(exec, slot);
		}
		catch (Exception e) {
			throw new RuntimeException("Modifying the slot failed", e);
		}
	}

	// ------------------------------------------------------------------------
	//  Mocking Slots
	// ------------------------------------------------------------------------

	public static SimpleSlot createMockSimpleSlot(JobID jid, TaskManagerGateway gateway) {
		final TaskManagerLocation location = new TaskManagerLocation(
				ResourceID.generate(), InetAddress.getLoopbackAddress(), 6572);

		final AllocatedSlot allocatedSlot = new AllocatedSlot(
				new AllocationID(),
				jid,
				location,
				0,
				ResourceProfile.UNKNOWN,
				gateway);

		return new SimpleSlot(allocatedSlot, mock(SlotOwner.class), 0);
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

		return createSimpleTestGraph(new JobID(), restartStrategy, vertex);
	}

	/**
	 * Creates an execution graph containing the given vertices.
	 * 
	 * <p>The execution graph uses {@link NoRestartStrategy} as the restart strategy.
	 */
	public static ExecutionGraph createSimpleTestGraph(JobID jid, JobVertex... vertices) throws Exception {
		return createSimpleTestGraph(jid, new NoRestartStrategy(), vertices);
	}

	/**
	 * Creates an execution graph containing the given vertices and the given restart strategy.
	 */
	public static ExecutionGraph createSimpleTestGraph(
			JobID jid,
			RestartStrategy restartStrategy,
			JobVertex... vertices) throws Exception {

		int numSlotsNeeded = 0;
		for (JobVertex vertex : vertices) {
			numSlotsNeeded += vertex.getParallelism();
		}

		SlotProvider slotProvider = new SimpleSlotProvider(jid, numSlotsNeeded);

		return createSimpleTestGraph(jid, slotProvider, restartStrategy, vertices);
	}

	public static ExecutionGraph createSimpleTestGraph(
			JobID jid,
			SlotProvider slotProvider,
			RestartStrategy restartStrategy,
			JobVertex... vertices) throws Exception {

		return createExecutionGraph(jid, slotProvider, restartStrategy, TestingUtils.defaultExecutor(), vertices);
	}

	public static ExecutionGraph createExecutionGraph(
			JobID jid,
			SlotProvider slotProvider,
			RestartStrategy restartStrategy,
			ScheduledExecutorService executor,
			JobVertex... vertices) throws Exception {

		checkNotNull(jid);
		checkNotNull(restartStrategy);
		checkNotNull(vertices);

		return ExecutionGraphBuilder.buildGraph(
				null,
				new JobGraph(jid, "test job", vertices),
				new Configuration(),
				executor,
				executor,
				slotProvider,
				ExecutionGraphTestUtils.class.getClassLoader(),
				new StandaloneCheckpointRecoveryFactory(),
				Time.seconds(10),
				restartStrategy,
				new UnregisteredMetricsGroup(),
				1,
				TEST_LOGGER);
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

	@SuppressWarnings("serial")
	public static class SimpleActorGateway extends BaseTestingActorGateway {
		
		public TaskDeploymentDescriptor lastTDD;

		public SimpleActorGateway(ExecutionContext executionContext){
			super(executionContext);
		}

		@Override
		public Object handleMessage(Object message) {
			if (message instanceof SubmitTask) {
				SubmitTask submitTask = (SubmitTask) message;
				lastTDD = submitTask.tasks();
				return Acknowledge.get();
			} else if(message instanceof CancelTask) {
				return Acknowledge.get();
			} else if(message instanceof FailIntermediateResultPartitions) {
				return new Object();
			} else {
				return null;
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
			executor,
			executor,
			new JobID(), 
			"test job", 
			new Configuration(),
			new SerializedValue<>(new ExecutionConfig()),
			AkkaUtils.getDefaultTimeout(),
			new NoRestartStrategy(),
			new Scheduler(ExecutionContext$.MODULE$.fromExecutor(executor)));

		return spy(new ExecutionJobVertex(graph, ajv, 1, AkkaUtils.getDefaultTimeout()));
	}
	
	public static ExecutionJobVertex getExecutionVertex(JobVertexID id) throws Exception {
		return getExecutionVertex(id, TestingUtils.defaultExecutor());
	}
}
