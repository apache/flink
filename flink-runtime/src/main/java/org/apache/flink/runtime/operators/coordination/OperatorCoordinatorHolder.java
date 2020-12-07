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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.checkpoint.OperatorCoordinatorCheckpointContext;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.scheduler.SchedulerNG;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TemporaryClassLoaderContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The {@code OperatorCoordinatorHolder} holds the {@link OperatorCoordinator} and manages all its
 * interactions with the remaining components.
 * It provides the context and is responsible for checkpointing and exactly once semantics.
 *
 * <h3>Exactly-one Semantics</h3>
 *
 * <p>The semantics are described under {@link OperatorCoordinator#checkpointCoordinator(long, CompletableFuture)}.
 *
 * <h3>Exactly-one Mechanism</h3>
 *
 * <p>This implementation can handle one checkpoint being triggered at a time. If another checkpoint
 * is triggered while the triggering of the first one was not completed or aborted, this class will
 * throw an exception. That is in line with the capabilities of the Checkpoint Coordinator, which can
 * handle multiple concurrent checkpoints on the TaskManagers, but only one concurrent triggering phase.
 *
 * <p>The mechanism for exactly once semantics is as follows:
 *
 * <ul>
 *   <li>Events pass through a special channel, the {@link OperatorEventValve}. If we are not currently
 *       triggering a checkpoint, then events simply pass through.
 *   <li>Atomically, with the completion of the checkpoint future for the coordinator, this operator
 *       operator event valve is closed. Events coming after that are held back (buffered), because
 *       they belong to the epoch after the checkpoint.
 *   <li>Once all coordinators in the job have completed the checkpoint, the barriers to the sources
 *       are injected. After that (see {@link #afterSourceBarrierInjection(long)}) the valves are
 *       opened again and the events are sent.
 *   <li>If a task fails in the meantime, the events are dropped from the valve. From the coordinator's
 *       perspective, these events are lost, because they were sent to a failed subtask after it's latest
 *       complete checkpoint.
 * </ul>
 *
 * <p><b>IMPORTANT:</b> A critical assumption is that all events from the scheduler to the Tasks are
 * transported strictly in order. Events being sent from the coordinator after the checkpoint barrier
 * was injected must not overtake the checkpoint barrier. This is currently guaranteed by Flink's
 * RPC mechanism.
 *
 * <p>Consider this example:
 * <pre>
 * Coordinator one events: => a . . b . |trigger| . . |complete| . . c . . d . |barrier| . e . f
 * Coordinator two events: => . . x . . |trigger| . . . . . . . . . .|complete||barrier| . . y . . z
 * </pre>
 *
 * <p>Two coordinators trigger checkpoints at the same time. 'Coordinator Two' takes longer to complete,
 * and in the meantime 'Coordinator One' sends more events.
 *
 * <p>'Coordinator One' emits events 'c' and 'd' after it finished its checkpoint, meaning the events must
 * take place after the checkpoint. But they are before the barrier injection, meaning the runtime
 * task would see them before the checkpoint, if they were immediately transported.
 *
 * <p>'Coordinator One' closes its valve as soon as the checkpoint future completes. Events 'c' and 'd'
 * get held back in the valve. Once 'Coordinator Two' completes its checkpoint, the barriers are sent
 * to the sources. Then the valves are opened, and events 'c' and 'd' can flow to the tasks where they
 * are received after the barrier.
 *
 * <h3>Concurrency and Threading Model</h3>
 *
 * <p>This component runs mainly in a main-thread-executor, like RPC endpoints. However,
 * some actions need to be triggered synchronously by other threads. Most notably, when the
 * checkpoint future is completed by the {@code OperatorCoordinator} implementation, we need to
 * synchronously suspend event-sending.
 */
public class OperatorCoordinatorHolder implements OperatorCoordinator, OperatorCoordinatorCheckpointContext {

	private final OperatorCoordinator coordinator;
	private final OperatorID operatorId;
	private final LazyInitializedCoordinatorContext context;
	private final OperatorEventValve eventValve;

	private final int operatorParallelism;
	private final int operatorMaxParallelism;

	private Consumer<Throwable> globalFailureHandler;
	private ComponentMainThreadExecutor mainThreadExecutor;

	private OperatorCoordinatorHolder(
			final OperatorID operatorId,
			final OperatorCoordinator coordinator,
			final LazyInitializedCoordinatorContext context,
			final OperatorEventValve eventValve,
			final int operatorParallelism,
			final int operatorMaxParallelism) {

		this.operatorId = checkNotNull(operatorId);
		this.coordinator = checkNotNull(coordinator);
		this.context = checkNotNull(context);
		this.eventValve = checkNotNull(eventValve);
		this.operatorParallelism = operatorParallelism;
		this.operatorMaxParallelism = operatorMaxParallelism;
	}

	public void lazyInitialize(SchedulerNG scheduler, ComponentMainThreadExecutor mainThreadExecutor) {
		lazyInitialize(scheduler::handleGlobalFailure, mainThreadExecutor);
	}

	@VisibleForTesting
	void lazyInitialize(Consumer<Throwable> globalFailureHandler, ComponentMainThreadExecutor mainThreadExecutor) {
		this.globalFailureHandler = globalFailureHandler;
		this.mainThreadExecutor = mainThreadExecutor;
		context.lazyInitialize(globalFailureHandler, mainThreadExecutor);
	}

	// ------------------------------------------------------------------------
	//  Properties
	// ------------------------------------------------------------------------

	public OperatorCoordinator coordinator() {
		return coordinator;
	}

	@Override
	public OperatorID operatorId() {
		return operatorId;
	}

	@Override
	public int maxParallelism() {
		return operatorMaxParallelism;
	}

	@Override
	public int currentParallelism() {
		return operatorParallelism;
	}

	// ------------------------------------------------------------------------
	//  OperatorCoordinator Interface
	// ------------------------------------------------------------------------

	@Override
	public void start() throws Exception {
		mainThreadExecutor.assertRunningInMainThread();
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
		mainThreadExecutor.assertRunningInMainThread();
		coordinator.handleEventFromOperator(subtask, event);
	}

	@Override
	public void subtaskFailed(int subtask, @Nullable Throwable reason) {
		mainThreadExecutor.assertRunningInMainThread();
		coordinator.subtaskFailed(subtask, reason);
		eventValve.resetForTask(subtask);
	}

	@Override
	public void subtaskReset(int subtask, long checkpointId) {
		mainThreadExecutor.assertRunningInMainThread();
		coordinator.subtaskReset(subtask, checkpointId);
	}

	@Override
	public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> result) {
		// unfortunately, this method does not run in the scheduler executor, but in the
		// checkpoint coordinator time thread.
		// we can remove the delegation once the checkpoint coordinator runs fully in the scheduler's
		// main thread executor
		mainThreadExecutor.execute(() -> checkpointCoordinatorInternal(checkpointId, result));
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) {
		// unfortunately, this method does not run in the scheduler executor, but in the
		// checkpoint coordinator time thread.
		// we can remove the delegation once the checkpoint coordinator runs fully in the scheduler's
		// main thread executor
		mainThreadExecutor.execute(() -> coordinator.notifyCheckpointComplete(checkpointId));
	}

	@Override
	public void notifyCheckpointAborted(long checkpointId) {
		// unfortunately, this method does not run in the scheduler executor, but in the
		// checkpoint coordinator time thread.
		// we can remove the delegation once the checkpoint coordinator runs fully in the scheduler's
		// main thread executor
		mainThreadExecutor.execute(() -> coordinator.notifyCheckpointAborted(checkpointId));
	}

	@Override
	public void resetToCheckpoint(long checkpointId, @Nullable byte[] checkpointData) throws Exception {
		// ideally we would like to check this here, however this method is called early during
		// execution graph construction, before the main thread executor is set

		eventValve.reset();
		if (context != null) {
			context.resetFailed();
		}
		coordinator.resetToCheckpoint(checkpointId, checkpointData);
	}

	private void checkpointCoordinatorInternal(final long checkpointId, final CompletableFuture<byte[]> result) {
		mainThreadExecutor.assertRunningInMainThread();

		// synchronously!!!, with the completion, we need to shut the event valve
		result.whenComplete((success, failure) -> {
			if (failure != null) {
				result.completeExceptionally(failure);
			} else {
				try {
					eventValve.shutValve(checkpointId);
					result.complete(success);
				} catch (Exception e) {
					result.completeExceptionally(e);
				}
			}
		});

		try {
			eventValve.markForCheckpoint(checkpointId);
			coordinator.checkpointCoordinator(checkpointId, result);
		} catch (Throwable t) {
			ExceptionUtils.rethrowIfFatalErrorOrOOM(t);
			result.completeExceptionally(t);
			globalFailureHandler.accept(t);
		}
	}

	// ------------------------------------------------------------------------
	//  Checkpointing Callbacks
	// ------------------------------------------------------------------------

	@Override
	public void afterSourceBarrierInjection(long checkpointId) {
		// this method is commonly called by the CheckpointCoordinator's executor thread (timer thread).

		// we ideally want the scheduler main-thread to be the one that sends the blocked events
		// however, we need to react synchronously here, to maintain consistency and not allow
		// another checkpoint injection in-between (unlikely, but possible).
		// fortunately, the event-sending goes pretty much directly to the RPC gateways, which are
		// thread safe.

		// this will automatically be fixed once the checkpoint coordinator runs in the
		// scheduler's main thread executor
		eventValve.openValveAndUnmarkCheckpoint();
	}

	@Override
	public void abortCurrentTriggering() {
		// this method is commonly called by the CheckpointCoordinator's executor thread (timer thread).

		// we ideally want the scheduler main-thread to be the one that sends the blocked events
		// however, we need to react synchronously here, to maintain consistency and not allow
		// another checkpoint injection in-between (unlikely, but possible).
		// fortunately, the event-sending goes pretty much directly to the RPC gateways, which are
		// thread safe.

		// this will automatically be fixed once the checkpoint coordinator runs in the
		// scheduler's main thread executor
		eventValve.openValveAndUnmarkCheckpoint();
	}

	// ------------------------------------------------------------------------
	//  Factories
	// ------------------------------------------------------------------------

	public static OperatorCoordinatorHolder create(
			SerializedValue<OperatorCoordinator.Provider> serializedProvider,
			ExecutionJobVertex jobVertex,
			ClassLoader classLoader) throws Exception {

		try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(classLoader)) {
			final OperatorCoordinator.Provider provider = serializedProvider.deserializeValue(classLoader);
			final OperatorID opId = provider.getOperatorId();

			final BiFunction<SerializedValue<OperatorEvent>, Integer, CompletableFuture<Acknowledge>> eventSender =
				(serializedEvent, subtask) -> {
					final Execution executionAttempt = jobVertex.getTaskVertices()[subtask].getCurrentExecutionAttempt();
					return executionAttempt.sendOperatorEvent(opId, serializedEvent);
				};

			return create(
					opId,
					provider,
					eventSender,
					jobVertex.getName(),
					jobVertex.getGraph().getUserClassLoader(),
					jobVertex.getParallelism(),
					jobVertex.getMaxParallelism());
		}
	}

	@VisibleForTesting
	static OperatorCoordinatorHolder create(
			final OperatorID opId,
			final OperatorCoordinator.Provider coordinatorProvider,
			final BiFunction<SerializedValue<OperatorEvent>, Integer, CompletableFuture<Acknowledge>> eventSender,
			final String operatorName,
			final ClassLoader userCodeClassLoader,
			final int operatorParallelism,
			final int operatorMaxParallelism) throws Exception {

		final OperatorEventValve valve = new OperatorEventValve(eventSender);

		final LazyInitializedCoordinatorContext context = new LazyInitializedCoordinatorContext(
				opId, valve, operatorName, userCodeClassLoader, operatorParallelism);

		final OperatorCoordinator coordinator = coordinatorProvider.create(context);

		return new OperatorCoordinatorHolder(
				opId,
				coordinator,
				context,
				valve,
				operatorParallelism,
				operatorMaxParallelism);
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

		private static final Logger LOG = LoggerFactory.getLogger(LazyInitializedCoordinatorContext.class);

		private final OperatorID operatorId;
		private final OperatorEventValve eventValve;
		private final String operatorName;
		private final ClassLoader userCodeClassLoader;
		private final int operatorParallelism;

		private Consumer<Throwable> globalFailureHandler;
		private Executor schedulerExecutor;

		private volatile boolean failed;

		public LazyInitializedCoordinatorContext(
				final OperatorID operatorId,
				final OperatorEventValve eventValve,
				final String operatorName,
				final ClassLoader userCodeClassLoader,
				final int operatorParallelism) {
			this.operatorId = checkNotNull(operatorId);
			this.eventValve = checkNotNull(eventValve);
			this.operatorName = checkNotNull(operatorName);
			this.userCodeClassLoader = checkNotNull(userCodeClassLoader);
			this.operatorParallelism = operatorParallelism;
		}

		void lazyInitialize(Consumer<Throwable> globalFailureHandler, Executor schedulerExecutor) {
			this.globalFailureHandler = checkNotNull(globalFailureHandler);
			this.schedulerExecutor = checkNotNull(schedulerExecutor);
		}

		void unInitialize() {
			this.globalFailureHandler = null;
			this.schedulerExecutor = null;
		}

		boolean isInitialized() {
			return schedulerExecutor != null;
		}

		private void checkInitialized() {
			checkState(isInitialized(), "Context was not yet initialized");
		}

		void resetFailed() {
			failed = false;
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

			return eventValve.sendEvent(serializedEvent, targetSubtask);
		}

		@Override
		public void failJob(final Throwable cause) {
			checkInitialized();
			if (failed) {
				LOG.warn("Ignoring the request to fail job because the job is already failing. "
							+ "The ignored failure cause is", cause);
				return;
			}
			failed = true;

			final FlinkException e = new FlinkException("Global failure triggered by OperatorCoordinator for '" +
				operatorName + "' (operator " + operatorId + ").", cause);

			schedulerExecutor.execute(() -> globalFailureHandler.accept(e));
		}

		@Override
		public int currentParallelism() {
			return operatorParallelism;
		}

		@Override
		public ClassLoader getUserCodeClassloader() {
			return userCodeClassLoader;
		}
	}
}
