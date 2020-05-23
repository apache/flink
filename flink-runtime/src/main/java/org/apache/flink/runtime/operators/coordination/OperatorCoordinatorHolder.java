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

package org.apache.flink.runtime.operators.coordination;

import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.scheduler.SchedulerNG;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TemporaryClassLoaderContext;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A holder for an {@link OperatorCoordinator.Context} and all the necessary facility around it that
 * is needed to interaction between the Coordinator, the Scheduler, the Checkpoint Coordinator, etc.
 *
 * <p>The holder is itself a {@link OperatorCoordinator} and forwards all calls to the actual coordinator.
 * That way, we can make adjustments to assumptions about the threading model and message/call forwarding
 * without needing to adjust all the call sites that interact with the coordinator.
 *
 * <p>This is also needed, unfortunately, because we need a lazy two-step initialization:
 * When the execution graph is created, we need to create the coordinators (or the holders, to be specific)
 * because the CheckpointCoordinator is also created in the ExecutionGraph and needs access to them.
 * However, the real Coordinators can only be created after SchedulerNG was created, because they need
 * a reference to it for the failure calls.
 */
public class OperatorCoordinatorHolder implements OperatorCoordinator {

	private final OperatorCoordinator coordinator;
	private final OperatorID operatorId;
	private final LazyInitializedCoordinatorContext context;

	private OperatorCoordinatorHolder(
			final OperatorID operatorId,
			final OperatorCoordinator coordinator,
			final LazyInitializedCoordinatorContext context) {

		this.operatorId = checkNotNull(operatorId);
		this.coordinator = checkNotNull(coordinator);
		this.context = checkNotNull(context);
	}

	// ------------------------------------------------------------------------

	public OperatorID getOperatorId() {
		return operatorId;
	}

	public OperatorCoordinator getCoordinator() {
		return coordinator;
	}

	public void lazyInitialize(SchedulerNG scheduler, Executor schedulerExecutor) {
		context.lazyInitialize(scheduler, schedulerExecutor);
	}

	// ------------------------------------------------------------------------
	//  OperatorCoordinator Interface
	// ------------------------------------------------------------------------

	@Override
	public void start() throws Exception {
		checkState(context.isInitialized(), "Coordinator Context is not yet initialized");
		coordinator.start();
	}

	@Override
	public void close() throws Exception {
		coordinator.close();
		context.unInitialize();
	}

	@Override
	public void handleEventFromOperator(int subtask, OperatorEvent event) throws Exception {
		coordinator.handleEventFromOperator(subtask, event);
	}

	@Override
	public void subtaskFailed(int subtask, @Nullable Throwable reason) {
		coordinator.subtaskFailed(subtask, reason);
	}

	@Override
	public CompletableFuture<byte[]> checkpointCoordinator(long checkpointId) throws Exception {
		return coordinator.checkpointCoordinator(checkpointId);
	}

	@Override
	public void checkpointComplete(long checkpointId) {
		coordinator.checkpointComplete(checkpointId);
	}

	@Override
	public void resetToCheckpoint(byte[] checkpointData) throws Exception {
		coordinator.resetToCheckpoint(checkpointData);
	}

	// ------------------------------------------------------------------------
	//  Factories
	// ------------------------------------------------------------------------

	public static OperatorCoordinatorHolder create(
			SerializedValue<OperatorCoordinator.Provider> serializedProvider,
			ExecutionJobVertex jobVertex,
			ClassLoader classLoader) throws IOException, ClassNotFoundException {

		try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(classLoader)) {
			final OperatorCoordinator.Provider provider = serializedProvider.deserializeValue(classLoader);
			final OperatorID opId = provider.getOperatorId();
			final LazyInitializedCoordinatorContext context = new LazyInitializedCoordinatorContext(opId, jobVertex);
			final OperatorCoordinator coordinator = provider.create(context);
			return new OperatorCoordinatorHolder(opId, coordinator, context);
		}
	}

	// ------------------------------------------------------------------------
	//  Nested Classes
	// ------------------------------------------------------------------------

	/**
	 * An implementation of the {@link OperatorCoordinator.Context}.
	 *
	 * <p>All methods are safe to be called from other threads than the Scheduler's and the JobMaster's
	 * main threads.
	 *
	 * <p>Implementation note: Ideally, we would like to operate purely against the scheduler
	 * interface, but it is not exposing enough information at the moment.
	 */
	private static final class LazyInitializedCoordinatorContext implements OperatorCoordinator.Context {

		private final OperatorID operatorId;
		private final ExecutionJobVertex jobVertex;

		private SchedulerNG scheduler;
		private Executor schedulerExecutor;

		public LazyInitializedCoordinatorContext(OperatorID operatorId, ExecutionJobVertex jobVertex) {
			this.operatorId = checkNotNull(operatorId);
			this.jobVertex = checkNotNull(jobVertex);
		}

		void lazyInitialize(SchedulerNG scheduler, Executor schedulerExecutor) {
			this.scheduler = checkNotNull(scheduler);
			this.schedulerExecutor = checkNotNull(schedulerExecutor);
		}

		void unInitialize() {
			this.scheduler = null;
			this.schedulerExecutor = null;
		}

		boolean isInitialized() {
			return jobVertex != null;
		}

		private void checkInitialized() {
			checkState(isInitialized(), "Context was not yet initialized");
		}

		@Override
		public OperatorID getOperatorId() {
			return operatorId;
		}

		@Override
		public CompletableFuture<Acknowledge> sendEvent(final OperatorEvent evt, final int targetSubtask) {
			checkInitialized();

			if (targetSubtask < 0 || targetSubtask >= currentParallelism()) {
				throw new IllegalArgumentException(
					String.format("subtask index %d out of bounds [0, %d).", targetSubtask, currentParallelism()));
			}

			final SerializedValue<OperatorEvent> serializedEvent;
			try {
				serializedEvent = new SerializedValue<>(evt);
			}
			catch (IOException e) {
				// we do not expect that this exception is handled by the caller, so we make it
				// unchecked so that it can bubble up
				throw new FlinkRuntimeException("Cannot serialize operator event", e);
			}

			final Execution executionAttempt = jobVertex.getTaskVertices()[targetSubtask].getCurrentExecutionAttempt();
			return executionAttempt.sendOperatorEvent(operatorId, serializedEvent);
		}

		@Override
		public void failTask(final int subtask, final Throwable cause) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void failJob(final Throwable cause) {
			checkInitialized();

			final FlinkException e = new FlinkException("Global failure triggered by OperatorCoordinator for '" +
				jobVertex.getName() + "' (operator " + operatorId + ").", cause);

			schedulerExecutor.execute(() -> scheduler.handleGlobalFailure(e));
		}

		@Override
		public int currentParallelism() {
			checkInitialized();
			return jobVertex.getParallelism();
		}
	}
}
