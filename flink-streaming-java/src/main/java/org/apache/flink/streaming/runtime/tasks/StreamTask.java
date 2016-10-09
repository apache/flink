/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.StatefulTask;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.ChainedStateHandle;
import org.apache.flink.runtime.state.CheckpointStateHandles;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.ClosableRegistry;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StateBackendFactory;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackendFactory;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.taskmanager.DispatcherThreadFactory;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamCheckpointedOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.io.RecordWriterOutput;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadFactory;

/**
 * Base class for all streaming tasks. A task is the unit of local processing that is deployed
 * and executed by the TaskManagers. Each task runs one or more {@link StreamOperator}s which form
 * the Task's operator chain. Operators that are chained together execute synchronously in the
 * same thread and hence on the same stream partition. A common case for these chains
 * are successive map/flatmap/filter tasks.
 *
 * <p>The task chain contains one "head" operator and multiple chained operators.
 * The StreamTask is specialized for the type of the head operator: one-input and two-input tasks,
 * as well as for sources, iteration heads and iteration tails.
 *
 * <p>The Task class deals with the setup of the streams read by the head operator, and the streams
 * produced by the operators at the ends of the operator chain. Note that the chain may fork and
 * thus have multiple ends.
 *
 * The life cycle of the task is set up as follows:
 * <pre>{@code
 *  -- getOperatorState() -> restores state of all operators in the chain
 *
 *  -- invoke()
 *        |
 *        +----> Create basic utils (config, etc) and load the chain of operators
 *        +----> operators.setup()
 *        +----> task specific init()
 *        +----> open-operators()
 *        +----> run()
 *        +----> close-operators()
 *        +----> dispose-operators()
 *        +----> common cleanup
 *        +----> task specific cleanup()
 * }</pre>
 *
 * <p> The {@code StreamTask} has a lock object called {@code lock}. All calls to methods on a
 * {@code StreamOperator} must be synchronized on this lock object to ensure that no methods
 * are called concurrently.
 *
 * @param <OUT>
 * @param <OP>
 */
@Internal
public abstract class StreamTask<OUT, OP extends StreamOperator<OUT>>
		extends AbstractInvokable
		implements StatefulTask, AsyncExceptionHandler {

	/** The thread group that holds all trigger timer threads */
	public static final ThreadGroup TRIGGER_THREAD_GROUP = new ThreadGroup("Triggers");

	/** The logger used by the StreamTask and its subclasses */
	private static final Logger LOG = LoggerFactory.getLogger(StreamTask.class);

	// ------------------------------------------------------------------------

	/**
	 * All interaction with the {@code StreamOperator} must be synchronized on this lock object to ensure that
	 * we don't have concurrent method calls that void consistent checkpoints.
	 */
	private final Object lock = new Object();

	/** the head operator that consumes the input streams of this task */
	protected OP headOperator;

	/** The chain of operators executed by this task */
	private OperatorChain<OUT, OP> operatorChain;

	/** The configuration of this streaming task */
	private StreamConfig configuration;

	/** Our state backend. We use this to create checkpoint streams and a keyed state backend. */
	private AbstractStateBackend stateBackend;

	/** Keyed state backend for the head operator, if it is keyed. There can only ever be one. */
	private AbstractKeyedStateBackend<?> keyedStateBackend;

	/**
	 * The internal {@link TimeServiceProvider} used to define the current
	 * processing time (default = {@code System.currentTimeMillis()}) and
	 * register timers for tasks to be executed in the future.
	 */
	private TimeServiceProvider timerService;

	/** The map of user-defined accumulators of this task */
	private Map<String, Accumulator<?, ?>> accumulatorMap;

	/** The chained operator state to be restored once the initialization is done */
	private ChainedStateHandle<StreamStateHandle> lazyRestoreChainedOperatorState;

	private List<KeyGroupsStateHandle> lazyRestoreKeyGroupStates;

	private List<Collection<OperatorStateHandle>> lazyRestoreOperatorState;


	/** The currently active background materialization threads */
	private final ClosableRegistry cancelables = new ClosableRegistry();

	/** Flag to mark the task "in operation", in which case check
	 * needs to be initialized to true, so that early cancel() before invoke() behaves correctly */
	private volatile boolean isRunning;

	/** Flag to mark this task as canceled */
	private volatile boolean canceled;

	private long lastCheckpointSize = 0;

	/** Thread pool for async snapshot workers */
	private ExecutorService asyncOperationsThreadPool;

	// ------------------------------------------------------------------------
	//  Life cycle methods for specific implementations
	// ------------------------------------------------------------------------

	protected abstract void init() throws Exception;

	protected abstract void run() throws Exception;

	protected abstract void cleanup() throws Exception;

	protected abstract void cancelTask() throws Exception;

	// ------------------------------------------------------------------------
	//  Core work methods of the Stream Task
	// ------------------------------------------------------------------------

	/**
	 * Allows the user to specify his own {@link TimeServiceProvider TimerServiceProvider}.
	 * By default a {@link DefaultTimeServiceProvider DefaultTimerService} is going to be provided.
	 * Changing it can be useful for testing processing time functionality, such as
	 * {@link org.apache.flink.streaming.api.windowing.assigners.WindowAssigner WindowAssigners}
	 * and {@link org.apache.flink.streaming.api.windowing.triggers.Trigger Triggers}.
	 * */
	public void setTimeService(TimeServiceProvider timeProvider) {
		if (timeProvider == null) {
			throw new RuntimeException("The timeProvider cannot be set to null.");
		}
		timerService = timeProvider;
	}

	@Override
	public final void invoke() throws Exception {

		boolean disposed = false;
		try {
			// -------- Initialize ---------
			LOG.debug("Initializing {}", getName());

			asyncOperationsThreadPool = Executors.newCachedThreadPool();

			configuration = new StreamConfig(getTaskConfiguration());

			stateBackend = createStateBackend();

			accumulatorMap = getEnvironment().getAccumulatorRegistry().getUserMap();

			// if the clock is not already set, then assign a default TimeServiceProvider
			if (timerService == null) {
				ThreadFactory timerThreadFactory =
					new DispatcherThreadFactory(TRIGGER_THREAD_GROUP, "Time Trigger for " + getName());

				timerService = new DefaultTimeServiceProvider(this, getCheckpointLock(), timerThreadFactory);
			}

			operatorChain = new OperatorChain<>(this, getEnvironment().getAccumulatorRegistry().getReadWriteReporter());
			headOperator = operatorChain.getHeadOperator();

			getEnvironment().getMetricGroup().gauge("lastCheckpointSize", new Gauge<Long>() {
				@Override
				public Long getValue() {
					return StreamTask.this.lastCheckpointSize;
				}
			});

			// task specific initialization
			init();

			// save the work of reloadig state, etc, if the task is already canceled
			if (canceled) {
				throw new CancelTaskException();
			}

			// -------- Invoke --------
			LOG.debug("Invoking {}", getName());

			// first order of business is to give operators back their state
			restoreState();
			lazyRestoreChainedOperatorState = null; // GC friendliness

			// we need to make sure that any triggers scheduled in open() cannot be
			// executed before all operators are opened
			synchronized (lock) {
				openAllOperators();
			}

			// final check to exit early before starting to run
			if (canceled) {
				throw new CancelTaskException();
			}

			// let the task do its work
			isRunning = true;
			run();

			// make sure all timers finish and no new timers can come
			timerService.quiesceAndAwaitPending();

			LOG.debug("Finished task {}", getName());

			// make sure no further checkpoint and notification actions happen.
			// we make sure that no other thread is currently in the locked scope before
			// we close the operators by trying to acquire the checkpoint scope lock
			// we also need to make sure that no triggers fire concurrently with the close logic
			// at the same time, this makes sure that during any "regular" exit where still
			synchronized (lock) {
				isRunning = false;

				// this is part of the main logic, so if this fails, the task is considered failed
				closeAllOperators();
			}

			LOG.debug("Closed operators for task {}", getName());

			// make sure all buffered data is flushed
			operatorChain.flushOutputs();

			// make an attempt to dispose the operators such that failures in the dispose call
			// still let the computation fail
			tryDisposeAllOperators();
			disposed = true;
		}
		finally {
			// clean up everything we initialized
			isRunning = false;

			// stop all timers and threads
			if (timerService != null) {
				try {
					timerService.shutdownService();
				}
				catch (Throwable t) {
					// catch and log the exception to not replace the original exception
					LOG.error("Could not shut down timer service", t);
				}
			}

			// stop all asynchronous checkpoint threads
			try {
				cancelables.close();
				shutdownAsyncThreads();
			}
			catch (Throwable t) {
				// catch and log the exception to not replace the original exception
				LOG.error("Could not shut down async checkpoint threads", t);
			}

			// release the output resources. this method should never fail.
			if (operatorChain != null) {
				operatorChain.releaseOutputs();
			}

			// we must! perform this cleanup
			try {
				cleanup();
			}
			catch (Throwable t) {
				// catch and log the exception to not replace the original exception
				LOG.error("Error during cleanup of stream task", t);
			}

			// if the operators were not disposed before, do a hard dispose
			if (!disposed) {
				disposeAllOperators();
			}
		}
	}

	@Override
	public final void cancel() throws Exception {
		isRunning = false;
		canceled = true;
		cancelTask();
		cancelables.close();
	}

	public final boolean isRunning() {
		return isRunning;
	}

	public final boolean isCanceled() {
		return canceled;
	}

	/**
	 * Execute the operator-specific {@link StreamOperator#open()} method in each
	 * of the operators in the chain of this {@link StreamTask}. </b> Opening happens
	 * from <b>tail to head</b> operator in the chain, contrary to
	 * {@link StreamOperator#close()} which happens <b>head to tail</b>
	 * operator (see {@link #closeAllOperators()}.
	 */
	private void openAllOperators() throws Exception {
		for (StreamOperator<?> operator : operatorChain.getAllOperators()) {
			if (operator != null) {
				operator.open();
			}
		}
	}

	/**
	 * Execute the operator-specific {@link StreamOperator#close()} method in each
	 * of the operators in the chain of this {@link StreamTask}. </b> Closing happens
	 * from <b>head to tail</b> operator in the chain, contrary to
	 * {@link StreamOperator#open()} which happens <b>tail to head</b> operator
	 * (see {@link #openAllOperators()}.
	 */
	private void closeAllOperators() throws Exception {
		// We need to close them first to last, since upstream operators in the chain might emit
		// elements in their close methods.
		StreamOperator<?>[] allOperators = operatorChain.getAllOperators();
		for (int i = allOperators.length - 1; i >= 0; i--) {
			StreamOperator<?> operator = allOperators[i];
			if (operator != null) {
				operator.close();
			}
		}
	}

	/**
	 * Execute the operator-specific {@link StreamOperator#dispose()} method in each
	 * of the operators in the chain of this {@link StreamTask}. </b> Disposing happens
	 * from <b>tail to head</b> operator in the chain.
	 */
	private void tryDisposeAllOperators() throws Exception {
		for (StreamOperator<?> operator : operatorChain.getAllOperators()) {
			if (operator != null) {
				operator.dispose();
			}
		}
	}

	private void shutdownAsyncThreads() throws Exception {
		if (!asyncOperationsThreadPool.isShutdown()) {
			asyncOperationsThreadPool.shutdownNow();
		}
	}

	/**
	 * Execute the operator-specific {@link StreamOperator#dispose()} method in each
	 * of the operators in the chain of this {@link StreamTask}. </b> Disposing happens
	 * from <b>tail to head</b> operator in the chain.
	 *
	 * The difference with the {@link #tryDisposeAllOperators()} is that in case of an
	 * exception, this method catches it and logs the message.
	 */
	private void disposeAllOperators() {
		if (operatorChain != null) {
			for (StreamOperator<?> operator : operatorChain.getAllOperators()) {
				try {
					if (operator != null) {
						operator.dispose();
					}
				}
				catch (Throwable t) {
					LOG.error("Error during disposal of stream operator.", t);
				}
			}
		}
	}

	/**
	 * The finalize method shuts down the timer. This is a fail-safe shutdown, in case the original
	 * shutdown method was never called.
	 *
	 * <p>
	 * This should not be relied upon! It will cause shutdown to happen much later than if manual
	 * shutdown is attempted, and cause threads to linger for longer than needed.
	 */
	@Override
	protected void finalize() throws Throwable {
		super.finalize();
		if (timerService != null) {
			if (!timerService.isTerminated()) {
				LOG.info("Timer service is shutting down.");
				timerService.shutdownService();
			}
		}

		cancelables.close();
	}

	boolean isSerializingTimestamps() {
		TimeCharacteristic tc = configuration.getTimeCharacteristic();
		return tc == TimeCharacteristic.EventTime | tc == TimeCharacteristic.IngestionTime;
	}

	// ------------------------------------------------------------------------
	//  Access to properties and utilities
	// ------------------------------------------------------------------------

	/**
	 * Gets the name of the task, in the form "taskname (2/5)".
	 * @return The name of the task.
	 */
	public String getName() {
		return getEnvironment().getTaskInfo().getTaskNameWithSubtasks();
	}

	/**
	 * Gets the lock object on which all operations that involve data and state mutation have to lock.
	 * @return The checkpoint lock object.
	 */
	public Object getCheckpointLock() {
		return lock;
	}

	public StreamConfig getConfiguration() {
		return configuration;
	}

	public Map<String, Accumulator<?, ?>> getAccumulatorMap() {
		return accumulatorMap;
	}

	Output<StreamRecord<OUT>> getHeadOutput() {
		return operatorChain.getChainEntryPoint();
	}

	RecordWriterOutput<?>[] getStreamOutputs() {
		return operatorChain.getStreamOutputs();
	}

	// ------------------------------------------------------------------------
	//  Checkpoint and Restore
	// ------------------------------------------------------------------------

	@Override
	public void setInitialState(
		ChainedStateHandle<StreamStateHandle> chainedState,
		List<KeyGroupsStateHandle> keyGroupsState,
		List<Collection<OperatorStateHandle>> partitionableOperatorState) {

		lazyRestoreChainedOperatorState = chainedState;
		lazyRestoreKeyGroupStates = keyGroupsState;
		lazyRestoreOperatorState = partitionableOperatorState;
	}

	private void restoreState() throws Exception {
		final StreamOperator<?>[] allOperators = operatorChain.getAllOperators();

		if (lazyRestoreChainedOperatorState != null) {
			Preconditions.checkState(lazyRestoreChainedOperatorState.getLength() == allOperators.length,
					"Invalid Invalid number of operator states. Found :" + lazyRestoreChainedOperatorState.getLength() +
							". Expected: " + allOperators.length);
		}

		if (lazyRestoreOperatorState != null) {
			Preconditions.checkArgument(lazyRestoreOperatorState.isEmpty()
							|| lazyRestoreOperatorState.size() == allOperators.length,
					"Invalid number of operator states. Found :" + lazyRestoreOperatorState.size() +
							". Expected: " + allOperators.length);
		}

		for (int i = 0; i < allOperators.length; i++) {
			StreamOperator<?> operator = allOperators[i];

			if (null != lazyRestoreOperatorState && !lazyRestoreOperatorState.isEmpty()) {
				operator.restoreState(lazyRestoreOperatorState.get(i));
			}

			// TODO deprecated code path
			if (operator instanceof StreamCheckpointedOperator) {

				if (lazyRestoreChainedOperatorState != null) {
					StreamStateHandle state = lazyRestoreChainedOperatorState.get(i);

					if (state != null) {
						LOG.debug("Restore state of task {} in chain ({}).", i, getName());

						FSDataInputStream is = state.openInputStream();
						try {
							cancelables.registerClosable(is);
							((StreamCheckpointedOperator) operator).restoreState(is);
						} finally {
							cancelables.unregisterClosable(is);
							is.close();
						}
					}
				}
			}
		}
	}

	@Override
	public boolean triggerCheckpoint(CheckpointMetaData checkpointMetaData) throws Exception {
		try {
			checkpointMetaData.
					setBytesBufferedInAlignment(0L).
					setAlignmentDurationNanos(0L);
			return performCheckpoint(checkpointMetaData);
		}
		catch (Exception e) {
			// propagate exceptions only if the task is still in "running" state
			if (isRunning) {
				throw e;
			} else {
				return false;
			}
		}
	}

	@Override
	public void triggerCheckpointOnBarrier(CheckpointMetaData checkpointMetaData) throws Exception {

		try {
			performCheckpoint(checkpointMetaData);
		}
		catch (CancelTaskException e) {
			throw e;
		}
		catch (Exception e) {
			throw new Exception("Error while performing a checkpoint", e);
		}
	}

	private boolean performCheckpoint(CheckpointMetaData checkpointMetaData) throws Exception {

		long checkpointId = checkpointMetaData.getCheckpointId();
		long timestamp = checkpointMetaData.getTimestamp();

		LOG.debug("Starting checkpoint {} on task {}", checkpointId, getName());

		synchronized (lock) {
			if (isRunning) {

				final long startOfSyncPart = System.nanoTime();

				// Since both state checkpointing and downstream barrier emission occurs in this
				// lock scope, they are an atomic operation regardless of the order in which they occur.
				// Given this, we immediately emit the checkpoint barriers, so the downstream operators
				// can start their checkpoint work as soon as possible
				operatorChain.broadcastCheckpointBarrier(checkpointId, timestamp);

				// now draw the state snapshot
				final StreamOperator<?>[] allOperators = operatorChain.getAllOperators();

				final List<StreamStateHandle> nonPartitionedStates =
						Arrays.asList(new StreamStateHandle[allOperators.length]);

				final List<OperatorStateHandle> operatorStates =
						Arrays.asList(new OperatorStateHandle[allOperators.length]);

				for (int i = 0; i < allOperators.length; i++) {
					StreamOperator<?> operator = allOperators[i];

					if (operator != null) {

						final String operatorId = createOperatorIdentifier(operator, configuration.getVertexID());

						CheckpointStreamFactory streamFactory =
								stateBackend.createStreamFactory(getEnvironment().getJobID(), operatorId);

						//TODO deprecated code path
						if (operator instanceof StreamCheckpointedOperator) {

							CheckpointStreamFactory.CheckpointStateOutputStream outStream =
									streamFactory.createCheckpointStateOutputStream(checkpointId, timestamp);


							cancelables.registerClosable(outStream);

							try {
								((StreamCheckpointedOperator) operator).
										snapshotState(outStream, checkpointId, timestamp);

								nonPartitionedStates.set(i, outStream.closeAndGetHandle());
							} finally {
								cancelables.unregisterClosable(outStream);
							}
						}

						RunnableFuture<OperatorStateHandle> handleFuture =
								operator.snapshotState(checkpointId, timestamp, streamFactory);

						if (null != handleFuture) {
							//TODO for now we assume there are only synchrous snapshots, no need to start the runnable.
							if (!handleFuture.isDone()) {
								throw new IllegalStateException("Currently only supports synchronous snapshots!");
							}

							operatorStates.set(i, handleFuture.get());
						}
					}

				}

				RunnableFuture<KeyGroupsStateHandle> keyGroupsStateHandleFuture = null;

				if (keyedStateBackend != null) {
					CheckpointStreamFactory streamFactory = stateBackend.createStreamFactory(
							getEnvironment().getJobID(),
							createOperatorIdentifier(headOperator, configuration.getVertexID()));

					keyGroupsStateHandleFuture = keyedStateBackend.snapshot(checkpointId, timestamp, streamFactory);
				}

				ChainedStateHandle<StreamStateHandle> chainedNonPartitionedStateHandles =
						new ChainedStateHandle<>(nonPartitionedStates);

				ChainedStateHandle<OperatorStateHandle> chainedPartitionedStateHandles =
						new ChainedStateHandle<>(operatorStates);

				LOG.debug("Finished synchronous checkpoints for checkpoint {} on task {}", checkpointId, getName());

				final long syncEndNanos = System.nanoTime();
				final long syncDurationMillis = (syncEndNanos - startOfSyncPart) / 1_000_000;

				checkpointMetaData.setSyncDurationMillis(syncDurationMillis);

				AsyncCheckpointRunnable asyncCheckpointRunnable = new AsyncCheckpointRunnable(
						"checkpoint-" + checkpointId + "-" + timestamp,
						this,
						cancelables,
						chainedNonPartitionedStateHandles,
						chainedPartitionedStateHandles,
						keyGroupsStateHandleFuture,
						checkpointMetaData,
						syncEndNanos);

				cancelables.registerClosable(asyncCheckpointRunnable);
				asyncOperationsThreadPool.submit(asyncCheckpointRunnable);

				if (LOG.isDebugEnabled()) {
					LOG.debug("{} - finished synchronous part of checkpoint {}." +
							"Alignment duration: {} ms, snapshot duration {} ms",
							getName(), checkpointId, checkpointMetaData.getAlignmentDurationNanos() / 1_000_000, syncDurationMillis);
				}

				return true;
			} else {
				return false;
			}
		}
	}

	public ExecutorService getAsyncOperationsThreadPool() {
		return asyncOperationsThreadPool;
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		synchronized (lock) {
			if (isRunning) {
				LOG.debug("Notification of complete checkpoint for task {}", getName());

				for (StreamOperator<?> operator : operatorChain.getAllOperators()) {
					if (operator != null) {
						operator.notifyOfCompletedCheckpoint(checkpointId);
					}
				}
			}
			else {
				LOG.debug("Ignoring notification of complete checkpoint for not-running task {}", getName());
			}
		}
	}

	// ------------------------------------------------------------------------
	//  State backend
	// ------------------------------------------------------------------------

	private AbstractStateBackend createStateBackend() throws Exception {
		AbstractStateBackend stateBackend = configuration.getStateBackend(getUserCodeClassLoader());

		if (stateBackend != null) {
			// backend has been configured on the environment
			LOG.info("Using user-defined state backend: " + stateBackend);
		} else {
			// see if we have a backend specified in the configuration
			Configuration flinkConfig = getEnvironment().getTaskManagerInfo().getConfiguration();
			String backendName = flinkConfig.getString(ConfigConstants.STATE_BACKEND, null);

			if (backendName == null) {
				LOG.warn("No state backend has been specified, using default state backend (Memory / JobManager)");
				backendName = "jobmanager";
			}

			backendName = backendName.toLowerCase();
			switch (backendName) {
				case "jobmanager":
					LOG.info("State backend is set to heap memory (checkpoint to jobmanager)");
					stateBackend = new MemoryStateBackend();
					break;

				case "filesystem":
					FsStateBackend backend = new FsStateBackendFactory().createFromConfig(flinkConfig);
					LOG.info("State backend is set to heap memory (checkpoints to filesystem \""
							+ backend.getBasePath() + "\")");
					stateBackend = backend;
					break;

				default:
					try {
						@SuppressWarnings("rawtypes")
						Class<? extends StateBackendFactory> clazz =
								Class.forName(backendName, false, getUserCodeClassLoader()).asSubclass(StateBackendFactory.class);

						stateBackend = clazz.newInstance().createFromConfig(flinkConfig);
					} catch (ClassNotFoundException e) {
						throw new IllegalConfigurationException("Cannot find configured state backend: " + backendName);
					} catch (ClassCastException e) {
						throw new IllegalConfigurationException("The class configured under '" +
								ConfigConstants.STATE_BACKEND + "' is not a valid state backend factory (" +
								backendName + ')');
					} catch (Throwable t) {
						throw new IllegalConfigurationException("Cannot create configured state backend", t);
					}
			}
		}

		return stateBackend;
	}

	public OperatorStateBackend createOperatorStateBackend(
			StreamOperator<?> op, Collection<OperatorStateHandle> restoreStateHandles) throws Exception {

		Environment env = getEnvironment();
		String opId = createOperatorIdentifier(op, configuration.getVertexID());

		OperatorStateBackend newBackend = restoreStateHandles == null ?
				stateBackend.createOperatorStateBackend(env, opId)
				: stateBackend.restoreOperatorStateBackend(env, opId, restoreStateHandles);

		cancelables.registerClosable(newBackend);

		return newBackend;
	}

	public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
			TypeSerializer<K> keySerializer,
			int numberOfKeyGroups,
			KeyGroupRange keyGroupRange) throws Exception {

		if (keyedStateBackend != null) {
			throw new RuntimeException("The keyed state backend can only be created once.");
		}

		String operatorIdentifier = createOperatorIdentifier(
				headOperator,
				configuration.getVertexID());

		if (lazyRestoreKeyGroupStates != null) {
			keyedStateBackend = stateBackend.restoreKeyedStateBackend(
					getEnvironment(),
					getEnvironment().getJobID(),
					operatorIdentifier,
					keySerializer,
					numberOfKeyGroups,
					keyGroupRange,
					lazyRestoreKeyGroupStates,
					getEnvironment().getTaskKvStateRegistry());

			lazyRestoreKeyGroupStates = null; // GC friendliness
		} else {
			keyedStateBackend = stateBackend.createKeyedStateBackend(
					getEnvironment(),
					getEnvironment().getJobID(),
					operatorIdentifier,
					keySerializer,
					numberOfKeyGroups,
					keyGroupRange,
					getEnvironment().getTaskKvStateRegistry());
		}

		cancelables.registerClosable(keyedStateBackend);

		@SuppressWarnings("unchecked")
		AbstractKeyedStateBackend<K> typedBackend = (AbstractKeyedStateBackend<K>) keyedStateBackend;
		return typedBackend;
	}

	/**
	 * This is only visible because
	 * {@link org.apache.flink.streaming.runtime.operators.GenericWriteAheadSink} uses the
	 * checkpoint stream factory to write write-ahead logs. <b>This should not be used for
	 * anything else.</b>
	 */
	public CheckpointStreamFactory createCheckpointStreamFactory(StreamOperator<?> operator) throws IOException {
		return stateBackend.createStreamFactory(
				getEnvironment().getJobID(),
				createOperatorIdentifier(operator, configuration.getVertexID()));

	}

	private String createOperatorIdentifier(StreamOperator<?> operator, int vertexId) {
		return operator.getClass().getSimpleName() +
				"_" + vertexId +
				"_" + getEnvironment().getTaskInfo().getIndexOfThisSubtask();
	}

	/**
	 * Returns the {@link TimeServiceProvider} responsible for telling the current
	 * processing time and registering timers.
	 */
	public TimeServiceProvider getTimerService() {
		if (timerService == null) {
			throw new IllegalStateException("The timer service has not been initialized.");
		}
		return timerService;
	}

	/**
	 * Handles an exception thrown by another thread (e.g. a TriggerTask),
	 * other than the one executing the main task by failing the task entirely.
	 *
	 * In more detail, it marks task execution failed for an external reason
	 * (a reason other than the task code itself throwing an exception). If the task
	 * is already in a terminal state (such as FINISHED, CANCELED, FAILED), or if the
	 * task is already canceling this does nothing. Otherwise it sets the state to
	 * FAILED, and, if the invokable code is running, starts an asynchronous thread
	 * that aborts that code.
	 *
	 * <p>This method never blocks.</p>
	 */
	@Override
	public void handleAsyncException(String message, Throwable exception) {
		getEnvironment().failExternally(exception);
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------


	@Override
	public String toString() {
		return getName();
	}

	// ------------------------------------------------------------------------

	private static class AsyncCheckpointRunnable implements Runnable, Closeable {

		private final StreamTask<?, ?> owner;

		private final ClosableRegistry cancelables;

		private final ChainedStateHandle<StreamStateHandle> nonPartitionedStateHandles;

		private final ChainedStateHandle<OperatorStateHandle> partitioneableStateHandles;

		private final RunnableFuture<KeyGroupsStateHandle> keyGroupsStateHandleFuture;

		private final String name;

		private final CheckpointMetaData checkpointMetaData;

		private final long asyncStartNanos;

		AsyncCheckpointRunnable(
				String name,
				StreamTask<?, ?> owner,
				ClosableRegistry cancelables,
				ChainedStateHandle<StreamStateHandle> nonPartitionedStateHandles,
				ChainedStateHandle<OperatorStateHandle> partitioneableStateHandles,
				RunnableFuture<KeyGroupsStateHandle> keyGroupsStateHandleFuture,
				CheckpointMetaData checkpointMetaData,
				long asyncStartNanos
		) {

			this.name = name;
			this.owner = owner;
			this.cancelables = cancelables;
			this.nonPartitionedStateHandles = nonPartitionedStateHandles;
			this.partitioneableStateHandles = partitioneableStateHandles;
			this.keyGroupsStateHandleFuture = keyGroupsStateHandleFuture;
			this.checkpointMetaData = checkpointMetaData;
			this.asyncStartNanos = asyncStartNanos;
		}

		@Override
		public void run() {
			try {

				List<KeyGroupsStateHandle> keyedStates = Collections.emptyList();

				if (keyGroupsStateHandleFuture != null) {

					if (!keyGroupsStateHandleFuture.isDone()) {
						//TODO this currently works because we only have one RunnableFuture
						keyGroupsStateHandleFuture.run();
					}

					KeyGroupsStateHandle keyGroupsStateHandle = this.keyGroupsStateHandleFuture.get();
					if (keyGroupsStateHandle != null) {
						keyedStates = Collections.singletonList(keyGroupsStateHandle);
					}
				}

				final long asyncEndNanos = System.nanoTime();
				final long asyncDurationMillis = (asyncEndNanos - asyncStartNanos) / 1_000_000;

				checkpointMetaData.setAsyncDurationMillis(asyncDurationMillis);

				if (nonPartitionedStateHandles.isEmpty() && partitioneableStateHandles.isEmpty() && keyedStates.isEmpty()) {
					owner.getEnvironment().acknowledgeCheckpoint(checkpointMetaData);
				} else {
					CheckpointStateHandles allStateHandles = new CheckpointStateHandles(
							nonPartitionedStateHandles,
							partitioneableStateHandles,
							keyedStates);

					owner.getEnvironment().acknowledgeCheckpoint(checkpointMetaData, allStateHandles);
				}

				if (LOG.isDebugEnabled()) {
					LOG.debug("{} - finished asynchronous part of checkpoint {}. Asynchronous duration: {} ms", 
							owner.getName(), checkpointMetaData.getCheckpointId(), asyncDurationMillis);
				}
			}
			catch (Exception e) {
				// registers the exception and tries to fail the whole task
				AsynchronousException asyncException = new AsynchronousException(e);
				owner.handleAsyncException("Failure in asynchronous checkpoint materialization", asyncException);
			}
			finally {
				cancelables.unregisterClosable(this);
			}
		}

		@Override
		public void close() {
			if (keyGroupsStateHandleFuture != null) {
				keyGroupsStateHandleFuture.cancel(true);
			}
		}
	}
}
