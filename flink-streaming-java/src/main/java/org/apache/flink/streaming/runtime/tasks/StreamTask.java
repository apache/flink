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
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.StatefulTask;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.ChainedStateHandle;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.StateBackendFactory;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackendFactory;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.taskmanager.DispatcherThreadFactory;
import org.apache.flink.runtime.util.event.EventListener;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.io.RecordWriterOutput;
import org.apache.flink.streaming.runtime.operators.Triggerable;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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
 *  -- restoreState() -> restores state of all operators in the chain
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
 * @param <Operator>
 */
@Internal
public abstract class StreamTask<OUT, Operator extends StreamOperator<OUT>>
		extends AbstractInvokable
		implements StatefulTask {

	/** The thread group that holds all trigger timer threads */
	public static final ThreadGroup TRIGGER_THREAD_GROUP = new ThreadGroup("Triggers");
	
	/** The logger used by the StreamTask and its subclasses */
	protected static final Logger LOG = LoggerFactory.getLogger(StreamTask.class);
	
	// ------------------------------------------------------------------------
	
	/**
	 * All interaction with the {@code StreamOperator} must be synchronized on this lock object to ensure that
	 * we don't have concurrent method calls that void consistent checkpoints.
	 */
	private final Object lock = new Object();
	
	/** the head operator that consumes the input streams of this task */
	protected Operator headOperator;

	/** The chain of operators executed by this task */
	private OperatorChain<OUT> operatorChain;
	
	/** The configuration of this streaming task */
	private StreamConfig configuration;

	/** The class loader used to load dynamic classes of a job */
	private ClassLoader userClassLoader;

	/** Our state backend. We use this to create checkpoint streams and a keyed state backend. */
	private AbstractStateBackend stateBackend;

	/** Keyed state backend for the head operator, if it is keyed. There can only ever be one. */
	private KeyedStateBackend<?> keyedStateBackend;

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

	/**
	 * This field is used to forward an exception that is caught in the timer thread or other
	 * asynchronous Threads. Subclasses must ensure that exceptions stored here get thrown on the
	 * actual execution Thread. */
	private volatile AsynchronousException asyncException;

	/** The currently active background materialization threads */
	private final Set<Closeable> cancelables = new HashSet<>();
	
	/** Flag to mark the task "in operation", in which case check
	 * needs to be initialized to true, so that early cancel() before invoke() behaves correctly */
	private volatile boolean isRunning;
	
	/** Flag to mark this task as canceled */
	private volatile boolean canceled;

	private long lastCheckpointSize = 0;

	/** Thread pool for async snapshot workers */
	private ExecutorService asyncOperationsThreadPool;

	/** Timeout to await the termination of the thread pool in milliseconds */
	private long threadPoolTerminationTimeout = 0L;

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

	public long getCurrentProcessingTime() {
		return timerService.getCurrentProcessingTime();
	}

	@Override
	public final void invoke() throws Exception {

		boolean disposed = false;
		try {
			// -------- Initialize ---------
			LOG.debug("Initializing {}", getName());

			asyncOperationsThreadPool = Executors.newCachedThreadPool();

			userClassLoader = getUserCodeClassLoader();

			configuration = new StreamConfig(getTaskConfiguration());

			stateBackend = createStateBackend();

			accumulatorMap = getEnvironment().getAccumulatorRegistry().getUserMap();

			// if the clock is not already set, then assign a default TimeServiceProvider
			if (timerService == null) {

				ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1,
					new DispatcherThreadFactory(TRIGGER_THREAD_GROUP, "Time Trigger for " + getName()));

				// allow trigger tasks to be removed if all timers for
				// that timestamp are removed by user
				executor.setRemoveOnCancelPolicy(true);

				timerService = DefaultTimeServiceProvider.create(executor);
			}

			headOperator = configuration.getStreamOperator(userClassLoader);
			operatorChain = new OperatorChain<>(this, headOperator, 
						getEnvironment().getAccumulatorRegistry().getReadWriteReporter());

			if (headOperator != null) {
				headOperator.setup(this, configuration, operatorChain.getChainEntryPoint());
			}

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

			// Don't forget to check and throw exceptions that happened in async thread one last time
			checkTimerException();
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
				closeAllClosables();
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

	/**
	 * Marks task execution failed for an external reason (a reason other than the task code itself
	 * throwing an exception). If the task is already in a terminal state
	 * (such as FINISHED, CANCELED, FAILED), or if the task is already canceling this does nothing.
	 * Otherwise it sets the state to FAILED, and, if the invokable code is running,
	 * starts an asynchronous thread that aborts that code.
	 *
	 * <p>This method never blocks.</p>
	 */
	public void failExternally(Throwable cause) {
		getEnvironment().failExternally(cause);
	}

	@Override
	public final void cancel() throws Exception {
		isRunning = false;
		canceled = true;
		cancelTask();
		closeAllClosables();
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

		if(threadPoolTerminationTimeout > 0L) {
			asyncOperationsThreadPool.awaitTermination(threadPoolTerminationTimeout, TimeUnit.MILLISECONDS);
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
			timerService.shutdownService();
		}

		closeAllClosables();
	}

	private void closeAllClosables() {
		// first, create a copy of the cancelables to prevent concurrent modifications
		// and to not hold the lock for too long. the copy can be a cheap list
		List<Closeable> localCancelables = null;
		synchronized (cancelables) {
			if (cancelables.size() > 0) {
				localCancelables = new ArrayList<>(cancelables);
				cancelables.clear();
			}
		}

		if (localCancelables != null) {
			for (Closeable cancelable : localCancelables) {
				try {
					cancelable.close();
				} catch (Throwable t) {
					LOG.error("Error on canceling operation", t);
				}
			}
		}
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
	public void setInitialState(ChainedStateHandle<StreamStateHandle> chainedState, List<KeyGroupsStateHandle> keyGroupsState) {
		lazyRestoreChainedOperatorState = chainedState;
		lazyRestoreKeyGroupStates = keyGroupsState;
	}

	private void restoreState() throws Exception {
		final StreamOperator<?>[] allOperators = operatorChain.getAllOperators();

		try {
			if (lazyRestoreChainedOperatorState != null) {

				synchronized (cancelables) {
					cancelables.add(lazyRestoreChainedOperatorState);
				}

				for (int i = 0; i < lazyRestoreChainedOperatorState.getLength(); i++) {
					StreamStateHandle state = lazyRestoreChainedOperatorState.get(i);
					if (state == null) {
						continue;
					}
					if (state != null) {
						StreamOperator<?> operator = allOperators[i];

						if (operator != null) {
							LOG.debug("Restore state of task {} in chain ({}).", i, getName());
							operator.restoreState(state.openInputStream());
						}
					}
				}
			}
		} finally {
			synchronized (cancelables) {
				cancelables.remove(lazyRestoreChainedOperatorState);
			}
		}
	}

	@Override
	public boolean triggerCheckpoint(long checkpointId, long timestamp) throws Exception {
		try {
			return performCheckpoint(checkpointId, timestamp);
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

	private boolean performCheckpoint(final long checkpointId, final long timestamp) throws Exception {
		LOG.debug("Starting checkpoint {} on task {}", checkpointId, getName());
		synchronized (lock) {
			if (isRunning) {

				// Since both state checkpointing and downstream barrier emission occurs in this
				// lock scope, they are an atomic operation regardless of the order in which they occur.
				// Given this, we immediately emit the checkpoint barriers, so the downstream operators
				// can start their checkpoint work as soon as possible
				operatorChain.broadcastCheckpointBarrier(checkpointId, timestamp);
				
				// now draw the state snapshot
				final StreamOperator<?>[] allOperators = operatorChain.getAllOperators();
				final List<StreamStateHandle> nonPartitionedStates = Arrays.asList(new StreamStateHandle[allOperators.length]);

				for (int i = 0; i < allOperators.length; i++) {
					StreamOperator<?> operator = allOperators[i];

					if (operator != null) {
						CheckpointStreamFactory streamFactory =
								stateBackend.createStreamFactory(
										getEnvironment().getJobID(),
										createOperatorIdentifier(
												operator,
												configuration.getVertexID()));

						CheckpointStreamFactory.CheckpointStateOutputStream outStream =
								streamFactory.createCheckpointStateOutputStream(checkpointId, timestamp);

						operator.snapshotState(outStream, checkpointId, timestamp);

						nonPartitionedStates.set(i, outStream.closeAndGetHandle());
					}
				}

				RunnableFuture<KeyGroupsStateHandle> keyGroupsStateHandleFuture = null;

				if (keyedStateBackend != null) {
					CheckpointStreamFactory streamFactory = stateBackend.createStreamFactory(
							getEnvironment().getJobID(),
							createOperatorIdentifier(
									headOperator,
									configuration.getVertexID()));
					keyGroupsStateHandleFuture = keyedStateBackend.snapshot(
							checkpointId,
							timestamp,
							streamFactory);
				}

				ChainedStateHandle<StreamStateHandle> chainedStateHandles = new ChainedStateHandle<>(nonPartitionedStates);

				LOG.debug("Finished synchronous checkpoints for checkpoint {} on task {}", checkpointId, getName());

				AsyncCheckpointRunnable asyncCheckpointRunnable = new AsyncCheckpointRunnable(
						"checkpoint-" + checkpointId + "-" + timestamp,
						this,
						cancelables,
						chainedStateHandles,
						keyGroupsStateHandleFuture,
						checkpointId);

				synchronized (cancelables) {
					cancelables.add(asyncCheckpointRunnable);
				}
				asyncOperationsThreadPool.submit(asyncCheckpointRunnable);
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

						stateBackend = ((StateBackendFactory<?>) clazz.newInstance()).createFromConfig(flinkConfig);
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

	public <K> KeyedStateBackend<K> createKeyedStateBackend(
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

		return (KeyedStateBackend<K>) keyedStateBackend;
	}

	/**
	 * This is only visible because
	 * {@link org.apache.flink.streaming.runtime.operators.GenericWriteAheadSink} uses the
	 * checkpoint stream factory to write write-ahead logs. <b>This should not be used for
	 * anything else.</b>
	 */
	public CheckpointStreamFactory createCheckpointStreamFactory(StreamOperator operator) throws IOException {
		return stateBackend.createStreamFactory(
				getEnvironment().getJobID(),
				createOperatorIdentifier(
						operator,
						configuration.getVertexID()));

	}

	private String createOperatorIdentifier(StreamOperator operator, int vertexId) {
		return operator.getClass().getSimpleName() +
				"_" + vertexId +
				"_" + getEnvironment().getTaskInfo().getIndexOfThisSubtask();
	}

	/**
	 * Registers a timer.
	 */
	public ScheduledFuture<?> registerTimer(final long timestamp, final Triggerable target) {
		if (timerService == null) {
			throw new IllegalStateException("The timer service has not been initialized.");
		}
		return timerService.registerTimer(timestamp, new TriggerTask(this, lock, target, timestamp));
	}

	/**
	 * Check whether an exception was thrown in a Thread other than the main Thread. (For example
	 * in the processing-time trigger Thread). This will rethrow that exception in case on
	 * occurred.
	 *
	 * <p>This must be called in the main loop of {@code StreamTask} subclasses to ensure
	 * that we propagate failures.
	 */
	public void checkTimerException() throws AsynchronousException {
		if (asyncException != null) {
			throw asyncException;
		}
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------
	
	@Override
	public String toString() {
		return getName();
	}

	final EventListener<CheckpointBarrier> getCheckpointBarrierListener() {
		return new EventListener<CheckpointBarrier>() {
			@Override
			public void onEvent(CheckpointBarrier barrier) {
				try {
					performCheckpoint(barrier.getId(), barrier.getTimestamp());
				}
				catch (CancelTaskException e) {
					throw e;
				}
				catch (Exception e) {
					throw new RuntimeException("Error triggering a checkpoint as the result of receiving checkpoint barrier", e);
				}
			}
		};
	}

	/**
	 * Sets a timeout for the async thread pool. Default should always be 0 to avoid blocking restarts of task.
	 *
	 * @param threadPoolTerminationTimeout timeout for the async thread pool in milliseconds
	 */
	public void setThreadPoolTerminationTimeout(long threadPoolTerminationTimeout) {
		this.threadPoolTerminationTimeout = threadPoolTerminationTimeout;
	}

	// ------------------------------------------------------------------------

	/**
	 * Internal task that is invoked by the timer service and triggers the target.
	 */
	private static final class TriggerTask implements Runnable {

		private final Object lock;
		private final Triggerable target;
		private final long timestamp;
		private final StreamTask<?, ?> task;

		TriggerTask(StreamTask<?, ?> task, final Object lock, Triggerable target, long timestamp) {
			this.task = task;
			this.lock = lock;
			this.target = target;
			this.timestamp = timestamp;
		}

		@Override
		public void run() {
			synchronized (lock) {
				try {
					target.trigger(timestamp);
				} catch (Throwable t) {
					if (task.isRunning) {
						LOG.error("Caught exception while processing timer.", t);
					}
					if (task.asyncException == null) {
						task.asyncException = new TimerException(t);
					}
				}
			}
		}
	}

	// ------------------------------------------------------------------------
	
	private static class AsyncCheckpointRunnable implements Runnable, Closeable {

		private final StreamTask<?, ?> owner;

		private final Set<Closeable> cancelables;

		private final ChainedStateHandle<StreamStateHandle> chainedStateHandles;

		private final RunnableFuture<KeyGroupsStateHandle> keyGroupsStateHandleFuture;

		private final long checkpointId;

		private final String name;

		AsyncCheckpointRunnable(
				String name,
				StreamTask<?, ?> owner,
				Set<Closeable> cancelables,
				ChainedStateHandle<StreamStateHandle> chainedStateHandles,
				RunnableFuture<KeyGroupsStateHandle> keyGroupsStateHandleFuture,
				long checkpointId) {

			this.name = name;
			this.owner = owner;
			this.cancelables = cancelables;
			this.chainedStateHandles = chainedStateHandles;
			this.keyGroupsStateHandleFuture = keyGroupsStateHandleFuture;
			this.checkpointId = checkpointId;
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
						keyedStates = Arrays.asList(keyGroupsStateHandle);
					}
				}

				if (chainedStateHandles.isEmpty() && keyedStates.isEmpty()) {
					owner.getEnvironment().acknowledgeCheckpoint(checkpointId);
				} else  {
					owner.getEnvironment().acknowledgeCheckpoint(checkpointId, chainedStateHandles, keyedStates);
				}

				if(LOG.isDebugEnabled()) {
					LOG.debug("Finished asynchronous checkpoints for checkpoint {} on task {}. Returning handles on " +
							"keyed states {}.", checkpointId, name, keyedStates);
				}
			}
			catch (Exception e) {
				if (owner.isRunning()) {
					LOG.error("Caught exception while materializing asynchronous checkpoints.", e);
				}
				if (owner.asyncException == null) {
					owner.asyncException = new AsynchronousException(e);
				}
			}
			finally {
				synchronized (cancelables) {
					cancelables.remove(this);
				}
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
