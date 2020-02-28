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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.VoidBlobWriter;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.PendingCheckpoint;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointRecoveryFactory;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutorService;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.failover.flip1.RestartPipelinedRegionFailoverStrategy;
import org.apache.flink.runtime.executiongraph.failover.flip1.TestRestartBackoffTimeStrategy;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.executiongraph.utils.SimpleSlotProvider;
import org.apache.flink.runtime.io.network.partition.NoOpJobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.VoidBackPressureStatsTracker;
import org.apache.flink.runtime.scheduler.strategy.EagerSchedulingStrategy;
import org.apache.flink.runtime.shuffle.NettyShuffleMaster;
import org.apache.flink.runtime.taskexecutor.TaskExecutorOperatorEventGateway;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.util.SerializedValue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * A utility class to create {@link DefaultScheduler} instances for testing.
 */
public class SchedulerTestingUtils {

	private static final Logger LOG = LoggerFactory.getLogger(SchedulerTestingUtils.class);

	private static final long DEFAULT_CHECKPOINT_TIMEOUT_MS = 10 * 60 * 1000;

	public static DefaultScheduler createScheduler(
			JobGraph jobGraph,
			ManuallyTriggeredScheduledExecutorService asyncExecutor) throws Exception {

		return createScheduler(jobGraph, asyncExecutor, new SimpleAckingTaskManagerGateway());
	}

	public static DefaultScheduler createScheduler(
			JobGraph jobGraph,
			ManuallyTriggeredScheduledExecutorService asyncExecutor,
			TaskExecutorOperatorEventGateway operatorEventGateway) throws Exception {

		final TaskManagerGateway gateway = operatorEventGateway instanceof TaskManagerGateway
				? (TaskManagerGateway) operatorEventGateway
				: new TaskExecutorOperatorEventGatewayAdapter(operatorEventGateway);

		return createScheduler(jobGraph, asyncExecutor, gateway);
	}

	public static DefaultScheduler createScheduler(
			JobGraph jobGraph,
			ManuallyTriggeredScheduledExecutorService asyncExecutor,
			TaskManagerGateway taskManagerGateway) throws Exception {

		return new DefaultScheduler(
			LOG,
			jobGraph,
			VoidBackPressureStatsTracker.INSTANCE,
			Executors.directExecutor(),
			new Configuration(),
			new SimpleSlotProvider(jobGraph.getJobID(), 0), // this is not used any more in the new scheduler
			asyncExecutor,
			asyncExecutor,
			ClassLoader.getSystemClassLoader(),
			new StandaloneCheckpointRecoveryFactory(),
			Time.seconds(300),
			VoidBlobWriter.getInstance(),
			UnregisteredMetricGroups.createUnregisteredJobManagerJobMetricGroup(),
			Time.seconds(300),
			NettyShuffleMaster.INSTANCE,
			NoOpJobMasterPartitionTracker.INSTANCE,
			new EagerSchedulingStrategy.Factory(),
			new RestartPipelinedRegionFailoverStrategy.Factory(),
			new TestRestartBackoffTimeStrategy(true, 0),
			new DefaultExecutionVertexOperations(),
			new ExecutionVertexVersioner(),
			new TestExecutionSlotAllocatorFactory(taskManagerGateway));
	}

	public static void enableCheckpointing(final JobGraph jobGraph) {
		final List<JobVertexID> triggerVertices = new ArrayList<>();
		final List<JobVertexID> allVertices = new ArrayList<>();

		for (JobVertex vertex : jobGraph.getVertices()) {
			if (vertex.isInputVertex()) {
				triggerVertices.add(vertex.getID());
			}
			allVertices.add(vertex.getID());
		}

		final CheckpointCoordinatorConfiguration config = new CheckpointCoordinatorConfiguration(
			Long.MAX_VALUE, // disable periodical checkpointing
			DEFAULT_CHECKPOINT_TIMEOUT_MS,
			0,
			1,
			CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
			false,
			false,
			0);

		jobGraph.setSnapshotSettings(new JobCheckpointingSettings(
				triggerVertices, allVertices, allVertices,
				config, null));
	}

	public static Collection<ExecutionAttemptID> getAllCurrentExecutionAttempts(DefaultScheduler scheduler) {
		return StreamSupport.stream(scheduler.requestJob().getAllExecutionVertices().spliterator(), false)
			.map((vertex) -> vertex.getCurrentExecutionAttempt().getAttemptId())
			.collect(Collectors.toList());
	}

	public static void setAllExecutionsToRunning(final DefaultScheduler scheduler) {
		final JobID jid = scheduler.requestJob().getJobID();
		getAllCurrentExecutionAttempts(scheduler).forEach(
			(attemptId) -> scheduler.updateTaskExecutionState(new TaskExecutionState(jid, attemptId, ExecutionState.RUNNING))
		);
	}

	public static void acknowledgePendingCheckpoint(final DefaultScheduler scheduler, final long checkpointId) throws CheckpointException {
		final CheckpointCoordinator checkpointCoordinator = getCheckpointCoordinator(scheduler);
		final JobID jid = scheduler.requestJob().getJobID();

		for (ExecutionAttemptID attemptId : getAllCurrentExecutionAttempts(scheduler)) {
			final AcknowledgeCheckpoint acknowledgeCheckpoint = new AcknowledgeCheckpoint(jid, attemptId, checkpointId);
			checkpointCoordinator.receiveAcknowledgeMessage(acknowledgeCheckpoint, "Unknown location");
		}
	}

	public static CompletedCheckpoint takeCheckpoint(DefaultScheduler scheduler) throws Exception {
		final CheckpointCoordinator checkpointCoordinator = getCheckpointCoordinator(scheduler);
		checkpointCoordinator.triggerCheckpoint(System.currentTimeMillis(), false);

		assertEquals("test setup inconsistent", 1, checkpointCoordinator.getNumberOfPendingCheckpoints());
		final PendingCheckpoint checkpoint = checkpointCoordinator.getPendingCheckpoints().values().iterator().next();
		final CompletableFuture<CompletedCheckpoint> future = checkpoint.getCompletionFuture();

		acknowledgePendingCheckpoint(scheduler, checkpoint.getCheckpointId());

		CompletedCheckpoint completed = future.getNow(null);
		assertNotNull("checkpoint not complete", completed);
		return completed;
	}

	@SuppressWarnings("deprecation")
	public static CheckpointCoordinator getCheckpointCoordinator(SchedulerBase scheduler) {
		return scheduler.getExecutionGraph().getCheckpointCoordinator();
	}

	// ------------------------------------------------------------------------

	private static final class TaskExecutorOperatorEventGatewayAdapter extends SimpleAckingTaskManagerGateway {

		private final TaskExecutorOperatorEventGateway operatorGateway;

		TaskExecutorOperatorEventGatewayAdapter(TaskExecutorOperatorEventGateway operatorGateway) {
			this.operatorGateway = operatorGateway;
		}

		@Override
		public CompletableFuture<Acknowledge> sendOperatorEventToTask(
				ExecutionAttemptID task,
				OperatorID operator,
				SerializedValue<OperatorEvent> evt) {
			return operatorGateway.sendOperatorEventToTask(task, operator, evt);
		}
	}
}
