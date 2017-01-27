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
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.SubtaskState;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.StatefulTask;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.ChainedStateHandle;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StateBackendFactory;
import org.apache.flink.runtime.state.StateUtil;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.TaskStateHandles;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackendFactory;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.taskmanager.DispatcherThreadFactory;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.OperatorSnapshotResult;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamCheckpointedOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.io.RecordWriterOutput;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FutureUtil;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;

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
 *  -- setInitialState -> provides state of all operators in the chain
 *
 *  -- invoke()
 *        |
 *        +----> Create basic utils (config, etc) and load the chain of operators
 *        +----> operators.setup()
 *        +----> task specific init()
 *        +----> initialize-operator-states()
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
	 * The internal {@link ProcessingTimeService} used to define the current
	 * processing time (default = {@code System.currentTimeMillis()}) and
	 * register timers for tasks to be executed in the future.
	 */
	private ProcessingTimeService timerService;

	/** The map of user-defined accumulators of this task */
	private Map<String, Accumulator<?, ?>> accumulatorMap;

	private TaskStateHandles restoreStateHandles;


	/** The currently active background materialization threads */
	private final CloseableRegistry cancelables = new CloseableRegistry();

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
	 * Allows the user to specify his own {@link ProcessingTimeService TimerServiceProvider}.
	 * By default a {@link SystemProcessingTimeService DefaultTimerService} is going to be provided.
	 * Changing it can be useful for testing processing time functionality, such as
	 * {@link org.apache.flink.streaming.api.windowing.assigners.WindowAssigner WindowAssigners}
	 * and {@link org.apache.flink.streaming.api.windowing.triggers.Trigger Triggers}.
	 * */
	public void setProcessingTimeService(ProcessingTimeService timeProvider) {
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
			LOG.debug("Initializing {}.", getName());

			asyncOperationsThreadPool = Executors.newCachedThreadPool();

			configuration = new StreamConfig(getTaskConfiguration());

			stateBackend = createStateBackend();

			accumulatorMap = getEnvironment().getAccumulatorRegistry().getUserMap();

			// if the clock is not already set, then assign a default TimeServiceProvider
			if (timerService == null) {
				ThreadFactory timerThreadFactory =
					new DispatcherThreadFactory(TRIGGER_THREAD_GROUP, "Time Trigger for " + getName());

				timerService = new SystemProcessingTimeService(this, getCheckpointLock(), timerThreadFactory);
			}

			operatorChain = new OperatorChain<>(this);
			headOperator = operatorChain.getHeadOperator();

			getEnvironment().getMetricGroup().gauge("lastCheckpointSize", new Gauge<Long>() {
				@Override
				public Long getValue() {
					return StreamTask.this.lastCheckpointSize;
				}
			});

			// task specific initialization
			init();

			// save the work of reloading state, etc, if the task is already canceled
			if (canceled) {
				throw new CancelTaskException();
			}

			// -------- Invoke --------
			LOG.debug("Invoking {}", getName());

			// first order of business is to give operators their state
			initializeState();

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

			// if this left the run() method cleanly despite the fact that this was canceled,
			// make sure the "clean shutdown" is not attempted
			if (canceled) {
				throw new CancelTaskException();
			}

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

			// release the output resources. this method should never fail.
			if (operatorChain != null) {
				operatorChain.releaseOutputs();
			}
		}
	}

	@Override
	public final void cancel() throws Exception {
		isRunning = false;
		canceled = true;

		// the "cancel task" call must come first, but the cancelables must be
		// closed no matter what
		try {
			cancelTask();
		}
		finally {
			cancelables.close();
		}
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
	public void setInitialState(TaskStateHandles taskStateHandles) {
		this.restoreStateHandles = taskStateHandles;
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
				throw new Exception("Could not perform checkpoint " + checkpointMetaData.getCheckpointId() +
					" for operator " + getName() + '.', e);
			} else {
				LOG.debug("Could not perform checkpoint {} for operator {} while the " +
					"invokable was not in state running.", checkpointMetaData.getCheckpointId(), getName(), e);
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
			throw new Exception("Operator " + getName() + " was cancelled while performing checkpoint " +
				checkpointMetaData.getCheckpointId() + '.');
		}
		catch (Exception e) {
			throw new Exception("Could not perform checkpoint " + checkpointMetaData.getCheckpointId() + " for operator " +
				getName() + '.', e);
		}
	}

	@Override
	public void abortCheckpointOnBarrier(long checkpointId, Throwable cause) throws Exception {
		LOG.debug("Aborting checkpoint via cancel-barrier {} for task {}", checkpointId, getName());

		// notify the coordinator that we decline this checkpoint
		getEnvironment().declineCheckpoint(checkpointId, cause);

		// notify all downstream operators that they should not wait for a barrier from us
		synchronized (lock) {
			operatorChain.broadcastCheckpointCancelMarker(checkpointId);
		}
	}

	private boolean performCheckpoint(CheckpointMetaData checkpointMetaData) throws Exception {
		LOG.debug("Starting checkpoint {} on task {}", checkpointMetaData.getCheckpointId(), getName());

		synchronized (lock) {
			if (isRunning) {
				// we can do a checkpoint

				// Since both state checkpointing and downstream barrier emission occurs in this
				// lock scope, they are an atomic operation regardless of the order in which they occur.
				// Given this, we immediately emit the checkpoint barriers, so the downstream operators
				// can start their checkpoint work as soon as possible
				operatorChain.broadcastCheckpointBarrier(
						checkpointMetaData.getCheckpointId(), checkpointMetaData.getTimestamp());

				checkpointState(checkpointMetaData);
				return true;
			}
			else {
				// we cannot perform our checkpoint - let the downstream operators know that they
				// should not wait for any input from this operator

				// we cannot broadcast the cancellation markers on the 'operator chain', because it may not
				// yet be created
				final CancelCheckpointMarker message = new CancelCheckpointMarker(checkpointMetaData.getCheckpointId());
				Exception exception = null;

				for (ResultPartitionWriter output : getEnvironment().getAllWriters()) {
					try {
						output.writeEventToAllChannels(message);
					} catch (Exception e) {
						exception = ExceptionUtils.firstOrSuppressed(
							new Exception("Could not send cancel checkpoint marker to downstream tasks.", e),
							exception);
					}
				}

				if (exception != null) {
					throw exception;
				}

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

	private void checkpointState(CheckpointMetaData checkpointMetaData) throws Exception {
		CheckpointingOperation checkpointingOperation = new CheckpointingOperation(this, checkpointMetaData);
		checkpointingOperation.executeCheckpointing();
	}

	private void initializeState() throws Exception {

		boolean restored = null != restoreStateHandles;

		if (restored) {

			checkRestorePreconditions(operatorChain.getChainLength());
			initializeOperators(true);
			restoreStateHandles = null; // free for GC
		} else {
			initializeOperators(false);
		}
	}

	private void initializeOperators(boolean restored) throws Exception {
		StreamOperator<?>[] allOperators = operatorChain.getAllOperators();
		for (int chainIdx = 0; chainIdx < allOperators.length; ++chainIdx) {
			StreamOperator<?> operator = allOperators[chainIdx];
			if (null != operator) {
				if (restored && restoreStateHandles != null) {
					operator.initializeState(new OperatorStateHandles(restoreStateHandles, chainIdx));
				} else {
					operator.initializeState(null);
				}
			}
		}
	}

	private void checkRestorePreconditions(int operatorChainLength) {

		ChainedStateHandle<StreamStateHandle> nonPartitionableOperatorStates =
				restoreStateHandles.getLegacyOperatorState();
		List<Collection<OperatorStateHandle>> operatorStates =
				restoreStateHandles.getManagedOperatorState();

		if (nonPartitionableOperatorStates != null) {
			Preconditions.checkState(nonPartitionableOperatorStates.getLength() == operatorChainLength,
					"Invalid Invalid number of operator states. Found :" + nonPartitionableOperatorStates.getLength()
							+ ". Expected: " + operatorChainLength);
		}

		if (!CollectionUtil.isNullOrEmpty(operatorStates)) {
			Preconditions.checkArgument(operatorStates.size() == operatorChainLength,
					"Invalid number of operator states. Found :" + operatorStates.size() +
							". Expected: " + operatorChainLength);
		}
	}

	// ------------------------------------------------------------------------
	//  State backend
	// ------------------------------------------------------------------------

	private AbstractStateBackend createStateBackend() throws Exception {
		AbstractStateBackend stateBackend = configuration.getStateBackend(getUserCodeClassLoader());

		if (stateBackend != null) {
			// backend has been configured on the environment
			LOG.info("Using user-defined state backend: {}.", stateBackend);
		} else {
			// see if we have a backend specified in the configuration
			Configuration flinkConfig = getEnvironment().getTaskManagerInfo().getConfiguration();
			String backendName = flinkConfig.getString(ConfigConstants.STATE_BACKEND, null);

			if (backendName == null) {
				LOG.warn("No state backend has been specified, using default state backend (Memory / JobManager)");
				backendName = "jobmanager";
			}

			switch (backendName.toLowerCase()) {
				case "jobmanager":
					LOG.info("State backend is set to heap memory (checkpoint to jobmanager)");
					stateBackend = new MemoryStateBackend();
					break;

				case "filesystem":
					FsStateBackend backend = new FsStateBackendFactory().createFromConfig(flinkConfig);
					LOG.info("State backend is set to heap memory (checkpoints to filesystem \"{}\")",
						backend.getBasePath());
					stateBackend = backend;
					break;

				case "rocksdb":
					backendName = "org.apache.flink.contrib.streaming.state.RocksDBStateBackendFactory";
					// fall through to the 'default' case that uses reflection to load the backend
					// that way we can keep RocksDB in a separate module

				default:
					try {
						@SuppressWarnings("rawtypes")
						Class<? extends StateBackendFactory> clazz =
								Class.forName(backendName, false, getUserCodeClassLoader()).
										asSubclass(StateBackendFactory.class);

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
		String opId = createOperatorIdentifier(op, getConfiguration().getVertexID());

		OperatorStateBackend operatorStateBackend = stateBackend.createOperatorStateBackend(env, opId);

		// let operator state backend participate in the operator lifecycle, i.e. make it responsive to cancelation
		cancelables.registerClosable(operatorStateBackend);

		// restore if we have some old state
		if (null != restoreStateHandles) {
			operatorStateBackend.restore(restoreStateHandles);
		}

		return operatorStateBackend;
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

		keyedStateBackend = stateBackend.createKeyedStateBackend(
				getEnvironment(),
				getEnvironment().getJobID(),
				operatorIdentifier,
				keySerializer,
				numberOfKeyGroups,
				keyGroupRange,
				getEnvironment().getTaskKvStateRegistry());

		// let keyed state backend participate in the operator lifecycle, i.e. make it responsive to cancelation
		cancelables.registerClosable(keyedStateBackend);

		// restore if we have some old state
		if (null != restoreStateHandles && null != restoreStateHandles.getManagedKeyedState()) {
			keyedStateBackend.restore(restoreStateHandles.getManagedKeyedState());
		}

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
	 * Returns the {@link ProcessingTimeService} responsible for telling the current
	 * processing time and registering timers.
	 */
	public ProcessingTimeService getProcessingTimeService() {
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

	private static final class AsyncCheckpointRunnable implements Runnable, Closeable {

		private final StreamTask<?, ?> owner;

		private final List<OperatorSnapshotResult> snapshotInProgressList;

		private RunnableFuture<KeyGroupsStateHandle> futureKeyedBackendStateHandles;
		private RunnableFuture<KeyGroupsStateHandle> futureKeyedStreamStateHandles;

		private List<StreamStateHandle> nonPartitionedStateHandles;

		private final CheckpointMetaData checkpointMetaData;

		private final long asyncStartNanos;

		private final AtomicReference<CheckpointingOperation.AsynCheckpointState> asyncCheckpointState = new AtomicReference<>(
			CheckpointingOperation.AsynCheckpointState.RUNNING);

		AsyncCheckpointRunnable(
				StreamTask<?, ?> owner,
				List<StreamStateHandle> nonPartitionedStateHandles,
				List<OperatorSnapshotResult> snapshotInProgressList,
				CheckpointMetaData checkpointMetaData,
				long asyncStartNanos) {

			this.owner = Preconditions.checkNotNull(owner);
			this.snapshotInProgressList = Preconditions.checkNotNull(snapshotInProgressList);
			this.checkpointMetaData = Preconditions.checkNotNull(checkpointMetaData);
			this.nonPartitionedStateHandles = nonPartitionedStateHandles;
			this.asyncStartNanos = asyncStartNanos;

			if (!snapshotInProgressList.isEmpty()) {
				// TODO Currently only the head operator of a chain can have keyed state, so simply access it directly.
				int headIndex = snapshotInProgressList.size() - 1;
				OperatorSnapshotResult snapshotInProgress = snapshotInProgressList.get(headIndex);
				if (null != snapshotInProgress) {
					this.futureKeyedBackendStateHandles = snapshotInProgress.getKeyedStateManagedFuture();
					this.futureKeyedStreamStateHandles = snapshotInProgress.getKeyedStateRawFuture();
				}
			}
		}

		@Override
		public void run() {

			try {

				// Keyed state handle future, currently only one (the head) operator can have this
				KeyGroupsStateHandle keyedStateHandleBackend = FutureUtil.runIfNotDoneAndGet(futureKeyedBackendStateHandles);
				KeyGroupsStateHandle keyedStateHandleStream = FutureUtil.runIfNotDoneAndGet(futureKeyedStreamStateHandles);

				List<OperatorStateHandle> operatorStatesBackend = new ArrayList<>(snapshotInProgressList.size());
				List<OperatorStateHandle> operatorStatesStream = new ArrayList<>(snapshotInProgressList.size());

				for (OperatorSnapshotResult snapshotInProgress : snapshotInProgressList) {
					if (null != snapshotInProgress) {
						operatorStatesBackend.add(
								FutureUtil.runIfNotDoneAndGet(snapshotInProgress.getOperatorStateManagedFuture()));
						operatorStatesStream.add(
								FutureUtil.runIfNotDoneAndGet(snapshotInProgress.getOperatorStateRawFuture()));
					} else {
						operatorStatesBackend.add(null);
						operatorStatesStream.add(null);
					}
				}

				final long asyncEndNanos = System.nanoTime();
				final long asyncDurationMillis = (asyncEndNanos - asyncStartNanos) / 1_000_000;

				checkpointMetaData.setAsyncDurationMillis(asyncDurationMillis);

				ChainedStateHandle<StreamStateHandle> chainedNonPartitionedOperatorsState =
						new ChainedStateHandle<>(nonPartitionedStateHandles);

				ChainedStateHandle<OperatorStateHandle> chainedOperatorStateBackend =
						new ChainedStateHandle<>(operatorStatesBackend);

				ChainedStateHandle<OperatorStateHandle> chainedOperatorStateStream =
						new ChainedStateHandle<>(operatorStatesStream);

				SubtaskState subtaskState = new SubtaskState(
						chainedNonPartitionedOperatorsState,
						chainedOperatorStateBackend,
						chainedOperatorStateStream,
						keyedStateHandleBackend,
						keyedStateHandleStream);

				if (asyncCheckpointState.compareAndSet(CheckpointingOperation.AsynCheckpointState.RUNNING, CheckpointingOperation.AsynCheckpointState.COMPLETED)) {
					owner.getEnvironment().acknowledgeCheckpoint(checkpointMetaData, subtaskState);

					if (LOG.isDebugEnabled()) {
						LOG.debug("{} - finished asynchronous part of checkpoint {}. Asynchronous duration: {} ms",
							owner.getName(), checkpointMetaData.getCheckpointId(), asyncDurationMillis);
					}
				} else {
					LOG.debug("{} - asynchronous part of checkpoint {} could not be completed because it was closed before.",
						owner.getName(),
						checkpointMetaData.getCheckpointId());
				}
			} catch (Exception e) {
				// the state is completed if an exception occurred in the acknowledgeCheckpoint call
				// in order to clean up, we have to set it to RUNNING again.
				asyncCheckpointState.compareAndSet(
					CheckpointingOperation.AsynCheckpointState.COMPLETED,
					CheckpointingOperation.AsynCheckpointState.RUNNING);
				
				try {
					cleanup();
				} catch (Exception cleanupException) {
					e.addSuppressed(cleanupException);
				}

				// registers the exception and tries to fail the whole task
				AsynchronousException asyncException = new AsynchronousException(
					new Exception(
						"Could not materialize checkpoint " + checkpointMetaData.getCheckpointId() +
							" for operator " + owner.getName() + '.',
						e));

				owner.handleAsyncException("Failure in asynchronous checkpoint materialization", asyncException);
			} finally {
				owner.cancelables.unregisterClosable(this);
			}
		}

		@Override
		public void close() {
			try {
				cleanup();
			} catch (Exception cleanupException) {
				LOG.warn("Could not properly clean up the async checkpoint runnable.", cleanupException);
			}
		}

		private void cleanup() throws Exception {
			if (asyncCheckpointState.compareAndSet(CheckpointingOperation.AsynCheckpointState.RUNNING, CheckpointingOperation.AsynCheckpointState.DISCARDED)) {
				LOG.debug("Cleanup AsyncCheckpointRunnable for checkpoint {} of {}.", checkpointMetaData.getCheckpointId(), owner.getName());
				Exception exception = null;

				// clean up ongoing operator snapshot results and non partitioned state handles
				for (OperatorSnapshotResult operatorSnapshotResult : snapshotInProgressList) {
					if (operatorSnapshotResult != null) {
						try {
							operatorSnapshotResult.cancel();
						} catch (Exception cancelException) {
							exception = ExceptionUtils.firstOrSuppressed(cancelException, exception);
						}
					}
				}

				// discard non partitioned state handles
				try {
					StateUtil.bestEffortDiscardAllStateObjects(nonPartitionedStateHandles);
				} catch (Exception discardException) {
					exception = ExceptionUtils.firstOrSuppressed(discardException, exception);
				}

				if (null != exception) {
					throw exception;
				}
			} else {
				LOG.debug("{} - asynchronous checkpointing operation for checkpoint {} has " +
						"already been completed. Thus, the state handles are not cleaned up.",
					owner.getName(),
					checkpointMetaData.getCheckpointId());
			}
		}
	}

	public CloseableRegistry getCancelables() {
		return cancelables;
	}

	// ------------------------------------------------------------------------

	private static final class CheckpointingOperation {

		private final StreamTask<?, ?> owner;

		private final CheckpointMetaData checkpointMetaData;

		private final StreamOperator<?>[] allOperators;

		private long startSyncPartNano;
		private long startAsyncPartNano;

		// ------------------------

		private CheckpointStreamFactory streamFactory;

		private final List<StreamStateHandle> nonPartitionedStates;
		private final List<OperatorSnapshotResult> snapshotInProgressList;

		public CheckpointingOperation(StreamTask<?, ?> owner, CheckpointMetaData checkpointMetaData) {
			this.owner = Preconditions.checkNotNull(owner);
			this.checkpointMetaData = Preconditions.checkNotNull(checkpointMetaData);
			this.allOperators = owner.operatorChain.getAllOperators();
			this.nonPartitionedStates = new ArrayList<>(allOperators.length);
			this.snapshotInProgressList = new ArrayList<>(allOperators.length);
		}

		public void executeCheckpointing() throws Exception {

			startSyncPartNano = System.nanoTime();

			boolean failed = true;
			try {

				for (StreamOperator<?> op : allOperators) {
					checkpointStreamOperator(op);
				}

				if (LOG.isDebugEnabled()) {
					LOG.debug("Finished synchronous checkpoints for checkpoint {} on task {}",
							checkpointMetaData.getCheckpointId(), owner.getName());
				}

				startAsyncPartNano = System.nanoTime();

				checkpointMetaData.setSyncDurationMillis((startAsyncPartNano - startSyncPartNano) / 1_000_000);

				// at this point we are transferring ownership over snapshotInProgressList for cleanup to the thread
				runAsyncCheckpointingAndAcknowledge();
				failed = false;

				if (LOG.isDebugEnabled()) {
					LOG.debug("{} - finished synchronous part of checkpoint {}." +
							"Alignment duration: {} ms, snapshot duration {} ms",
						owner.getName(), checkpointMetaData.getCheckpointId(),
						checkpointMetaData.getAlignmentDurationNanos() / 1_000_000,
						checkpointMetaData.getSyncDurationMillis());
				}
			} finally {
				if (failed) {
					// Cleanup to release resources
					for (OperatorSnapshotResult operatorSnapshotResult : snapshotInProgressList) {
						if (null != operatorSnapshotResult) {
							try {
								operatorSnapshotResult.cancel();
							} catch (Exception e) {
								LOG.warn("Could not properly cancel an operator snapshot result.", e);
							}
						}
					}

					// Cleanup non partitioned state handles
					for (StreamStateHandle nonPartitionedState : nonPartitionedStates) {
						if (nonPartitionedState != null) {
							try {
								nonPartitionedState.discardState();
							} catch (Exception e) {
								LOG.warn("Could not properly discard a non partitioned " +
									"state. This might leave some orphaned files behind.", e);
							}
						}
					}

					if (LOG.isDebugEnabled()) {
						LOG.debug("{} - did NOT finish synchronous part of checkpoint {}." +
								"Alignment duration: {} ms, snapshot duration {} ms",
							owner.getName(), checkpointMetaData.getCheckpointId(),
							checkpointMetaData.getAlignmentDurationNanos() / 1_000_000,
							checkpointMetaData.getSyncDurationMillis());
					}
				}
			}
		}

		private void checkpointStreamOperator(StreamOperator<?> op) throws Exception {
			if (null != op) {
				createStreamFactory(op);
				snapshotNonPartitionableState(op);

				OperatorSnapshotResult snapshotInProgress = op.snapshotState(
						checkpointMetaData.getCheckpointId(),
						checkpointMetaData.getTimestamp(),
						streamFactory);

				snapshotInProgressList.add(snapshotInProgress);
			} else {
				nonPartitionedStates.add(null);
				OperatorSnapshotResult emptySnapshotInProgress = new OperatorSnapshotResult();
				snapshotInProgressList.add(emptySnapshotInProgress);
			}
		}

		private void createStreamFactory(StreamOperator<?> operator) throws IOException {
			String operatorId = owner.createOperatorIdentifier(operator, owner.configuration.getVertexID());
			this.streamFactory = owner.stateBackend.createStreamFactory(owner.getEnvironment().getJobID(), operatorId);
		}

		//TODO deprecated code path
		private void snapshotNonPartitionableState(StreamOperator<?> operator) throws Exception {

			StreamStateHandle stateHandle = null;

			if (operator instanceof StreamCheckpointedOperator) {

				CheckpointStreamFactory.CheckpointStateOutputStream outStream =
						streamFactory.createCheckpointStateOutputStream(
								checkpointMetaData.getCheckpointId(), checkpointMetaData.getTimestamp());

				owner.cancelables.registerClosable(outStream);

				try {
					((StreamCheckpointedOperator) operator).
							snapshotState(
									outStream,
									checkpointMetaData.getCheckpointId(),
									checkpointMetaData.getTimestamp());

					stateHandle = outStream.closeAndGetHandle();
				} finally {
					owner.cancelables.unregisterClosable(outStream);
					outStream.close();
				}
			}

			nonPartitionedStates.add(stateHandle);
		}

		public void runAsyncCheckpointingAndAcknowledge() throws IOException {

			AsyncCheckpointRunnable asyncCheckpointRunnable = new AsyncCheckpointRunnable(
					owner,
					nonPartitionedStates,
					snapshotInProgressList,
					checkpointMetaData,
					startAsyncPartNano);

			owner.cancelables.registerClosable(asyncCheckpointRunnable);
			owner.asyncOperationsThreadPool.submit(asyncCheckpointRunnable);
		}

		private enum AsynCheckpointState {
			RUNNING,
			DISCARDED,
			COMPLETED
		}
	}
}
