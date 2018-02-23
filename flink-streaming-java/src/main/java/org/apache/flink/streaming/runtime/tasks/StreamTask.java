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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FileSystemSafetyNet;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.StatefulTask;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.taskmanager.DispatcherThreadFactory;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.OperatorSnapshotResult;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.configuration.TimerServiceOptions;
import org.apache.flink.streaming.runtime.io.RecordWriterOutput;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FutureUtil;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
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
 * <p>The life cycle of the task is set up as follows:
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
 * <p>The {@code StreamTask} has a lock object called {@code lock}. All calls to methods on a
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

	/** The thread group that holds all trigger timer threads. */
	public static final ThreadGroup TRIGGER_THREAD_GROUP = new ThreadGroup("Triggers");

	/** The logger used by the StreamTask and its subclasses. */
	private static final Logger LOG = LoggerFactory.getLogger(StreamTask.class);

	// ------------------------------------------------------------------------

	/**
	 * All interaction with the {@code StreamOperator} must be synchronized on this lock object to
	 * ensure that we don't have concurrent method calls that void consistent checkpoints.
	 */
	private final Object lock = new Object();

	/** the head operator that consumes the input streams of this task. */
	protected OP headOperator;

	/** The chain of operators executed by this task. */
	protected OperatorChain<OUT, OP> operatorChain;

	/** The configuration of this streaming task. */
	private StreamConfig configuration;

	/** Our state backend. We use this to create checkpoint streams and a keyed state backend. */
	private StateBackend stateBackend;

	/** Keyed state backend for the head operator, if it is keyed. There can only ever be one. */
	private AbstractKeyedStateBackend<?> keyedStateBackend;

	/**
	 * The internal {@link ProcessingTimeService} used to define the current
	 * processing time (default = {@code System.currentTimeMillis()}) and
	 * register timers for tasks to be executed in the future.
	 */
	private ProcessingTimeService timerService;

	/** The map of user-defined accumulators of this task. */
	private Map<String, Accumulator<?, ?>> accumulatorMap;

	private TaskStateSnapshot taskStateSnapshot;

	/** The currently active background materialization threads. */
	private final CloseableRegistry cancelables = new CloseableRegistry();

	/**
	 * Flag to mark the task "in operation", in which case check needs to be initialized to true,
	 * so that early cancel() before invoke() behaves correctly.
	 */
	private volatile boolean isRunning;

	/** Flag to mark this task as canceled. */
	private volatile boolean canceled;

	/** Thread pool for async snapshot workers. */
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
	@VisibleForTesting
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

			// task specific initialization
			init();

			// save the work of reloading state, etc, if the task is already canceled
			if (canceled) {
				throw new CancelTaskException();
			}

			// -------- Invoke --------
			LOG.debug("Invoking {}", getName());

			// we need to make sure that any triggers scheduled in open() cannot be
			// executed before all operators are opened
			synchronized (lock) {

				// both the following operations are protected by the lock
				// so that we avoid race conditions in the case that initializeState()
				// registers a timer, that fires before the open() is called.

				initializeState();
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

			LOG.debug("Finished task {}", getName());

			// make sure no further checkpoint and notification actions happen.
			// we make sure that no other thread is currently in the locked scope before
			// we close the operators by trying to acquire the checkpoint scope lock
			// we also need to make sure that no triggers fire concurrently with the close logic
			// at the same time, this makes sure that during any "regular" exit where still
			synchronized (lock) {
				// this is part of the main logic, so if this fails, the task is considered failed
				closeAllOperators();

				// make sure no new timers can come
				timerService.quiesce();

				// only set the StreamTask to not running after all operators have been closed!
				// See FLINK-7430
				isRunning = false;
			}

			// make sure all timers finish
			timerService.awaitPendingAfterQuiesce();

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

			// clear the interrupted status so that we can wait for the following resource shutdowns to complete
			Thread.interrupted();

			// stop all timers and threads
			if (timerService != null && !timerService.isTerminated()) {
				try {

					final long timeoutMs = getEnvironment().getTaskManagerInfo().getConfiguration().
						getLong(TimerServiceOptions.TIMER_SERVICE_TERMINATION_AWAIT_MS);

					// wait for a reasonable time for all pending timer threads to finish
					boolean timerShutdownComplete =
						timerService.shutdownAndAwaitPending(timeoutMs, TimeUnit.MILLISECONDS);

					if (!timerShutdownComplete) {
						LOG.warn("Timer service shutdown exceeded time limit of {} ms while waiting for pending " +
							"timers. Will continue with shutdown procedure.", timeoutMs);
					}
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
	 * Execute {@link StreamOperator#open()} of each operator in the chain of this
	 * {@link StreamTask}. Opening happens from <b>tail to head</b> operator in the chain, contrary
	 * to {@link StreamOperator#close()} which happens <b>head to tail</b>
	 * (see {@link #closeAllOperators()}.
	 */
	private void openAllOperators() throws Exception {
		for (StreamOperator<?> operator : operatorChain.getAllOperators()) {
			if (operator != null) {
				operator.open();
			}
		}
	}

	/**
	 * Execute {@link StreamOperator#close()} of each operator in the chain of this
	 * {@link StreamTask}. Closing happens from <b>head to tail</b> operator in the chain,
	 * contrary to {@link StreamOperator#open()} which happens <b>tail to head</b>
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
	 * Execute {@link StreamOperator#dispose()} of each operator in the chain of this
	 * {@link StreamTask}. Disposing happens from <b>tail to head</b> operator in the chain.
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
	 * Execute @link StreamOperator#dispose()} of each operator in the chain of this
	 * {@link StreamTask}. Disposing happens from <b>tail to head</b> operator in the chain.
	 *
	 * <p>The difference with the {@link #tryDisposeAllOperators()} is that in case of an
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
	 * <p>This should not be relied upon! It will cause shutdown to happen much later than if manual
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

	public StreamStatusMaintainer getStreamStatusMaintainer() {
		return operatorChain;
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
	public void setInitialState(TaskStateSnapshot taskStateHandles) {
		this.taskStateSnapshot = taskStateHandles;
	}

	@Override
	public boolean triggerCheckpoint(CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions) throws Exception {
		try {
			// No alignment if we inject a checkpoint
			CheckpointMetrics checkpointMetrics = new CheckpointMetrics()
					.setBytesBufferedInAlignment(0L)
					.setAlignmentDurationNanos(0L);

			return performCheckpoint(checkpointMetaData, checkpointOptions, checkpointMetrics);
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
	public void triggerCheckpointOnBarrier(
			CheckpointMetaData checkpointMetaData,
			CheckpointOptions checkpointOptions,
			CheckpointMetrics checkpointMetrics) throws Exception {

		try {
			performCheckpoint(checkpointMetaData, checkpointOptions, checkpointMetrics);
		}
		catch (CancelTaskException e) {
			LOG.info("Operator {} was cancelled while performing checkpoint {}.",
					getName(), checkpointMetaData.getCheckpointId());
			throw e;
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

	private boolean performCheckpoint(
			CheckpointMetaData checkpointMetaData,
			CheckpointOptions checkpointOptions,
			CheckpointMetrics checkpointMetrics) throws Exception {

		LOG.debug("Starting checkpoint ({}) {} on task {}",
			checkpointMetaData.getCheckpointId(), checkpointOptions.getCheckpointType(), getName());

		synchronized (lock) {
			if (isRunning) {
				// we can do a checkpoint

				// Since both state checkpointing and downstream barrier emission occurs in this
				// lock scope, they are an atomic operation regardless of the order in which they occur.
				// Given this, we immediately emit the checkpoint barriers, so the downstream operators
				// can start their checkpoint work as soon as possible
				operatorChain.broadcastCheckpointBarrier(
						checkpointMetaData.getCheckpointId(),
						checkpointMetaData.getTimestamp(),
						checkpointOptions);

				checkpointState(checkpointMetaData, checkpointOptions, checkpointMetrics);
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
						output.writeBufferToAllChannels(EventSerializer.toBuffer(message));
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

	private void checkpointState(
			CheckpointMetaData checkpointMetaData,
			CheckpointOptions checkpointOptions,
			CheckpointMetrics checkpointMetrics) throws Exception {

		CheckpointingOperation checkpointingOperation = new CheckpointingOperation(
			this,
			checkpointMetaData,
			checkpointOptions,
			checkpointMetrics);

		checkpointingOperation.executeCheckpointing();
	}

	private void initializeState() throws Exception {

		boolean restored = null != taskStateSnapshot;

		if (restored) {
			initializeOperators(true);
			taskStateSnapshot = null; // free for GC
		} else {
			initializeOperators(false);
		}
	}

	private void initializeOperators(boolean restored) throws Exception {
		StreamOperator<?>[] allOperators = operatorChain.getAllOperators();
		for (int chainIdx = 0; chainIdx < allOperators.length; ++chainIdx) {
			StreamOperator<?> operator = allOperators[chainIdx];
			if (null != operator) {
				if (restored && taskStateSnapshot != null) {
					operator.initializeState(taskStateSnapshot.getSubtaskStateByOperatorID(operator.getOperatorID()));
				} else {
					operator.initializeState(null);
				}
			}
		}
	}

	// ------------------------------------------------------------------------
	//  State backend
	// ------------------------------------------------------------------------

	private StateBackend createStateBackend() throws Exception {
		final StateBackend fromJob = configuration.getStateBackend(getUserCodeClassLoader());

		if (fromJob != null) {
			// backend has been configured on the environment
			LOG.info("Using user-defined state backend: {}.", fromJob);
			return fromJob;
		}
		else {
			return AbstractStateBackend.loadStateBackendFromConfigOrCreateDefault(
					getEnvironment().getTaskManagerInfo().getConfiguration(),
					getUserCodeClassLoader(),
					LOG);
		}
	}

	public OperatorStateBackend createOperatorStateBackend(
			StreamOperator<?> op, Collection<OperatorStateHandle> restoreStateHandles) throws Exception {

		Environment env = getEnvironment();
		String opId = createOperatorIdentifier(op, getConfiguration().getVertexID());

		OperatorStateBackend operatorStateBackend = stateBackend.createOperatorStateBackend(env, opId);

		// let operator state backend participate in the operator lifecycle, i.e. make it responsive to cancelation
		cancelables.registerCloseable(operatorStateBackend);

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
		cancelables.registerCloseable(keyedStateBackend);

		// restore if we have some old state
		Collection<KeyedStateHandle> restoreKeyedStateHandles = null;

		if (taskStateSnapshot != null) {
			OperatorSubtaskState stateByOperatorID =
				taskStateSnapshot.getSubtaskStateByOperatorID(headOperator.getOperatorID());
			restoreKeyedStateHandles = stateByOperatorID != null ? stateByOperatorID.getManagedKeyedState() : null;
		}

		keyedStateBackend.restore(restoreKeyedStateHandles);

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

	public CheckpointStreamFactory createSavepointStreamFactory(StreamOperator<?> operator, String targetLocation) throws IOException {
		return stateBackend.createSavepointStreamFactory(
			getEnvironment().getJobID(),
			createOperatorIdentifier(operator, configuration.getVertexID()),
			targetLocation);
	}

	private String createOperatorIdentifier(StreamOperator<?> operator, int vertexId) {

		TaskInfo taskInfo = getEnvironment().getTaskInfo();
		return operator.getClass().getSimpleName() +
			"_" + operator.getOperatorID() +
			"_(" + taskInfo.getIndexOfThisSubtask() + "/" + taskInfo.getNumberOfParallelSubtasks() + ")";
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
	 * <p>In more detail, it marks task execution failed for an external reason
	 * (a reason other than the task code itself throwing an exception). If the task
	 * is already in a terminal state (such as FINISHED, CANCELED, FAILED), or if the
	 * task is already canceling this does nothing. Otherwise it sets the state to
	 * FAILED, and, if the invokable code is running, starts an asynchronous thread
	 * that aborts that code.
	 *
	 * <p>This method never blocks.
	 */
	@Override
	public void handleAsyncException(String message, Throwable exception) {
		if (isRunning) {
			// only fail if the task is still running
			getEnvironment().failExternally(exception);
		}
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

		private final Map<OperatorID, OperatorSnapshotResult> operatorSnapshotsInProgress;

		private final CheckpointMetaData checkpointMetaData;
		private final CheckpointMetrics checkpointMetrics;

		private final long asyncStartNanos;

		private final AtomicReference<CheckpointingOperation.AsynCheckpointState> asyncCheckpointState = new AtomicReference<>(
			CheckpointingOperation.AsynCheckpointState.RUNNING);

		AsyncCheckpointRunnable(
				StreamTask<?, ?> owner,
				Map<OperatorID, OperatorSnapshotResult> operatorSnapshotsInProgress,
				CheckpointMetaData checkpointMetaData,
				CheckpointMetrics checkpointMetrics,
				long asyncStartNanos) {

			this.owner = Preconditions.checkNotNull(owner);
			this.operatorSnapshotsInProgress = Preconditions.checkNotNull(operatorSnapshotsInProgress);
			this.checkpointMetaData = Preconditions.checkNotNull(checkpointMetaData);
			this.checkpointMetrics = Preconditions.checkNotNull(checkpointMetrics);
			this.asyncStartNanos = asyncStartNanos;
		}

		@Override
		public void run() {
			FileSystemSafetyNet.initializeSafetyNetForThread();
			try {
				boolean hasState = false;
				final TaskStateSnapshot taskOperatorSubtaskStates =
					new TaskStateSnapshot(operatorSnapshotsInProgress.size());

				for (Map.Entry<OperatorID, OperatorSnapshotResult> entry : operatorSnapshotsInProgress.entrySet()) {

					OperatorID operatorID = entry.getKey();
					OperatorSnapshotResult snapshotInProgress = entry.getValue();

					OperatorSubtaskState operatorSubtaskState = new OperatorSubtaskState(
						FutureUtil.runIfNotDoneAndGet(snapshotInProgress.getOperatorStateManagedFuture()),
						FutureUtil.runIfNotDoneAndGet(snapshotInProgress.getOperatorStateRawFuture()),
						FutureUtil.runIfNotDoneAndGet(snapshotInProgress.getKeyedStateManagedFuture()),
						FutureUtil.runIfNotDoneAndGet(snapshotInProgress.getKeyedStateRawFuture())
					);

					hasState |= operatorSubtaskState.hasState();
					taskOperatorSubtaskStates.putSubtaskStateByOperatorID(operatorID, operatorSubtaskState);
				}

				final long asyncEndNanos = System.nanoTime();
				final long asyncDurationMillis = (asyncEndNanos - asyncStartNanos) / 1_000_000;

				checkpointMetrics.setAsyncDurationMillis(asyncDurationMillis);

				if (asyncCheckpointState.compareAndSet(CheckpointingOperation.AsynCheckpointState.RUNNING,
						CheckpointingOperation.AsynCheckpointState.COMPLETED)) {

					TaskStateSnapshot acknowledgedState = hasState ? taskOperatorSubtaskStates : null;

					// we signal stateless tasks by reporting null, so that there are no attempts to assign empty state
					// to stateless tasks on restore. This enables simple job modifications that only concern
					// stateless without the need to assign them uids to match their (always empty) states.
					owner.getEnvironment().acknowledgeCheckpoint(
						checkpointMetaData.getCheckpointId(),
						checkpointMetrics,
						acknowledgedState);

					LOG.debug("{} - finished asynchronous part of checkpoint {}. Asynchronous duration: {} ms",
						owner.getName(), checkpointMetaData.getCheckpointId(), asyncDurationMillis);

					LOG.trace("{} - reported the following states in snapshot for checkpoint {}: {}.",
						owner.getName(), checkpointMetaData.getCheckpointId(), acknowledgedState);

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
				owner.cancelables.unregisterCloseable(this);
				FileSystemSafetyNet.closeSafetyNetAndGuardedResourcesForThread();
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
				for (OperatorSnapshotResult operatorSnapshotResult : operatorSnapshotsInProgress.values()) {
					if (operatorSnapshotResult != null) {
						try {
							operatorSnapshotResult.cancel();
						} catch (Exception cancelException) {
							exception = ExceptionUtils.firstOrSuppressed(cancelException, exception);
						}
					}
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
		private final CheckpointOptions checkpointOptions;
		private final CheckpointMetrics checkpointMetrics;

		private final StreamOperator<?>[] allOperators;

		private long startSyncPartNano;
		private long startAsyncPartNano;

		// ------------------------

		private final Map<OperatorID, OperatorSnapshotResult> operatorSnapshotsInProgress;

		public CheckpointingOperation(
				StreamTask<?, ?> owner,
				CheckpointMetaData checkpointMetaData,
				CheckpointOptions checkpointOptions,
				CheckpointMetrics checkpointMetrics) {

			this.owner = Preconditions.checkNotNull(owner);
			this.checkpointMetaData = Preconditions.checkNotNull(checkpointMetaData);
			this.checkpointOptions = Preconditions.checkNotNull(checkpointOptions);
			this.checkpointMetrics = Preconditions.checkNotNull(checkpointMetrics);
			this.allOperators = owner.operatorChain.getAllOperators();
			this.operatorSnapshotsInProgress = new HashMap<>(allOperators.length);
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

				checkpointMetrics.setSyncDurationMillis((startAsyncPartNano - startSyncPartNano) / 1_000_000);

				// at this point we are transferring ownership over snapshotInProgressList for cleanup to the thread
				runAsyncCheckpointingAndAcknowledge();
				failed = false;

				if (LOG.isDebugEnabled()) {
					LOG.debug("{} - finished synchronous part of checkpoint {}." +
							"Alignment duration: {} ms, snapshot duration {} ms",
						owner.getName(), checkpointMetaData.getCheckpointId(),
						checkpointMetrics.getAlignmentDurationNanos() / 1_000_000,
						checkpointMetrics.getSyncDurationMillis());
				}
			} finally {
				if (failed) {
					// Cleanup to release resources
					for (OperatorSnapshotResult operatorSnapshotResult : operatorSnapshotsInProgress.values()) {
						if (null != operatorSnapshotResult) {
							try {
								operatorSnapshotResult.cancel();
							} catch (Exception e) {
								LOG.warn("Could not properly cancel an operator snapshot result.", e);
							}
						}
					}

					if (LOG.isDebugEnabled()) {
						LOG.debug("{} - did NOT finish synchronous part of checkpoint {}." +
								"Alignment duration: {} ms, snapshot duration {} ms",
							owner.getName(), checkpointMetaData.getCheckpointId(),
							checkpointMetrics.getAlignmentDurationNanos() / 1_000_000,
							checkpointMetrics.getSyncDurationMillis());
					}
				}
			}
		}

		@SuppressWarnings("deprecation")
		private void checkpointStreamOperator(StreamOperator<?> op) throws Exception {
			if (null != op) {

				OperatorSnapshotResult snapshotInProgress = op.snapshotState(
						checkpointMetaData.getCheckpointId(),
						checkpointMetaData.getTimestamp(),
						checkpointOptions);
				operatorSnapshotsInProgress.put(op.getOperatorID(), snapshotInProgress);
			}
		}

		public void runAsyncCheckpointingAndAcknowledge() throws IOException {

			AsyncCheckpointRunnable asyncCheckpointRunnable = new AsyncCheckpointRunnable(
					owner,
					operatorSnapshotsInProgress,
					checkpointMetaData,
					checkpointMetrics,
					startAsyncPartNano);

			owner.cancelables.registerCloseable(asyncCheckpointRunnable);
			owner.asyncOperationsThreadPool.submit(asyncCheckpointRunnable);
		}

		private enum AsynCheckpointState {
			RUNNING,
			DISCARDED,
			COMPLETED
		}
	}
}
