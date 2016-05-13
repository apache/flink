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

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.StatefulTask;
import org.apache.flink.runtime.state.AsynchronousKvStateSnapshot;
import org.apache.flink.runtime.state.AsynchronousStateHandle;
import org.apache.flink.runtime.state.KvStateSnapshot;
import org.apache.flink.runtime.state.PartitionedStateSnapshot;
import org.apache.flink.runtime.state.StateHandle;
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

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Base class for all streaming tasks. A task is the unit of local processing that is deployed
 * and executed by the TaskManagers. Each task runs one or more {@link StreamOperator}s which form
 * the Task's operator chain. Operators that are chained together execute synchronously in the
 * same thread and hence on the same stream partition. A common case for these chaines
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
		implements StatefulTask<StreamTaskState, ChainedKeyGroupState> {

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
	Operator headOperator;

	/** The chain of operators executed by this task */
	private OperatorChain<OUT> operatorChain;
	
	/** The configuration of this streaming task */
	private StreamConfig configuration;

	/** The executor service that schedules and calls the triggers of this task*/
	private ScheduledThreadPoolExecutor timerService;
	
	/** The map of user-defined accumulators of this task */
	private Map<String, Accumulator<?, ?>> accumulatorMap;
	
	/** The state to be restored once the initialization is done */
	private StreamTaskState lazyRestoreState;

	private Map<Integer, ChainedKeyGroupState> lazyRestoreKeyGroupStates;

	/**
	 * This field is used to forward an exception that is caught in the timer thread or other
	 * asynchronous Threads. Subclasses must ensure that exceptions stored here get thrown on the
	 * actual execution Thread. */
	private volatile AsynchronousException asyncException;

	/** The currently active background materialization threads */
	private final Set<Thread> asyncCheckpointThreads = Collections.synchronizedSet(new HashSet<Thread>());
	
	/** Flag to mark the task "in operation", in which case check
	 * needs to be initialized to true, so that early cancel() before invoke() behaves correctly */
	private volatile boolean isRunning;
	
	/** Flag to mark this task as canceled */
	private volatile boolean canceled;

	private long recoveryTimestamp;

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
	
	@Override
	public final void invoke() throws Exception {

		boolean disposed = false;
		try {
			// -------- Initialize ---------
			LOG.debug("Initializing {}", getName());

			configuration = new StreamConfig(getTaskConfiguration());
			accumulatorMap = getEnvironment().getAccumulatorRegistry().getUserMap();

			headOperator = configuration.getStreamOperator(getUserCodeClassLoader());
			operatorChain = new OperatorChain<>(this, headOperator, 
						getEnvironment().getAccumulatorRegistry().getReadWriteReporter());

			if (headOperator != null) {
				headOperator.setup(this, configuration, operatorChain.getChainEntryPoint());
			}

			timerService =new ScheduledThreadPoolExecutor(1, new DispatcherThreadFactory(TRIGGER_THREAD_GROUP, "Time Trigger for " + getName()));
			// allow trigger tasks to be removed if all timers for that timestamp are removed by user
			timerService.setRemoveOnCancelPolicy(true);

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
			lazyRestoreState = null; // GC friendliness
			lazyRestoreKeyGroupStates = null; // GC friendliness
			
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
		}
		finally {
			// clean up everything we initialized
			isRunning = false;

			// stop all timers and threads
			if (timerService != null) {
				try {
					timerService.shutdownNow();
				}
				catch (Throwable t) {
					// catch and log the exception to not replace the original exception
					LOG.error("Could not shut down timer service", t);
				}
			}
			
			// stop all asynchronous checkpoint threads
			try {
				for (Thread checkpointThread : asyncCheckpointThreads) {
					checkpointThread.interrupt();
				}
				asyncCheckpointThreads.clear();
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
	}

	public final boolean isRunning() {
		return isRunning;
	}
	
	public final boolean isCanceled() {
		return canceled;
	}
	
	private void openAllOperators() throws Exception {
		for (StreamOperator<?> operator : operatorChain.getAllOperators()) {
			if (operator != null) {
				operator.open();
			}
		}
	}

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

	private void tryDisposeAllOperators() throws Exception {
		for (StreamOperator<?> operator : operatorChain.getAllOperators()) {
			if (operator != null) {
				operator.dispose();
			}
		}
	}
	
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
				LOG.warn("Timer service was not shut down. Shutting down in finalize().");
			}
			timerService.shutdownNow();
		}

		for (Thread checkpointThread : asyncCheckpointThreads) {
			checkpointThread.interrupt();
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
	public void setInitialState(StreamTaskState initialState, Map<Integer, ChainedKeyGroupState> initialKeyGroupStates, long recoveryTimestamp) {
		lazyRestoreState = initialState;
		lazyRestoreKeyGroupStates = initialKeyGroupStates;
		this.recoveryTimestamp = recoveryTimestamp;
	}
	
	private void restoreState() throws Exception {

		if (lazyRestoreState != null || lazyRestoreKeyGroupStates != null) {

			LOG.info("Restoring checkpointed state to task {}", getName());

			final StreamOperator<?>[] allOperators = operatorChain.getAllOperators();
			StreamOperatorNonPartitionedState[] nonPartitionedStates;

			final List<Map<Integer, PartitionedStateSnapshot>> keyGroupStates = new ArrayList<Map<Integer, PartitionedStateSnapshot>>(allOperators.length);

			for (int i = 0; i < allOperators.length; i++) {
				keyGroupStates.add(new HashMap<Integer, PartitionedStateSnapshot>());
			}

			if (lazyRestoreState != null) {
				try {
					nonPartitionedStates = lazyRestoreState.getState(getUserCodeClassLoader());

					// be GC friendly
					lazyRestoreState = null;
				} catch (Exception e) {
					throw new Exception("Could not restore checkpointed non-partitioned state.", e);
				}
			} else {
				nonPartitionedStates = new StreamOperatorNonPartitionedState[allOperators.length];
			}

			if (lazyRestoreKeyGroupStates != null) {
				try {
					// construct key groups state for operators
					for (Map.Entry<Integer, ChainedKeyGroupState> lazyRestoreKeyGroupState : lazyRestoreKeyGroupStates.entrySet()) {
						int keyGroupId = lazyRestoreKeyGroupState.getKey();

						Map<Integer, PartitionedStateSnapshot> chainedKeyGroupStates = lazyRestoreKeyGroupState.getValue().getState(getUserCodeClassLoader());

						for (Map.Entry<Integer, PartitionedStateSnapshot> chainedKeyGroupState : chainedKeyGroupStates.entrySet()) {
							int chainedIndex = chainedKeyGroupState.getKey();

							Map<Integer, PartitionedStateSnapshot> keyGroupState;

							keyGroupState = keyGroupStates.get(chainedIndex);
							keyGroupState.put(keyGroupId, chainedKeyGroupState.getValue());
						}
					}

					lazyRestoreKeyGroupStates = null;

				} catch (Exception e) {
					throw new Exception("Could not restore checkpointed partitioned state.", e);
				}
			}

			for (int i = 0; i < nonPartitionedStates.length; i++) {
				StreamOperatorNonPartitionedState nonPartitionedState = nonPartitionedStates[i];
				StreamOperator<?> operator = allOperators[i];
				StreamOperatorPartitionedState partitionedState = new StreamOperatorPartitionedState(keyGroupStates.get(i));
				StreamOperatorState operatorState = new StreamOperatorState(partitionedState, nonPartitionedState);

				if (operator != null) {
					LOG.debug("Restore state of task {} in chain ({}).", i, getName());
					operator.restoreState(operatorState, recoveryTimestamp);
				}
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

				// since both state checkpointing and downstream barrier emission occurs in this
				// lock scope, they are an atomic operation regardless of the order in which they occur
				// we immediately emit the checkpoint barriers, so the downstream operators can start
				// their checkpoint work as soon as possible
				operatorChain.broadcastCheckpointBarrier(checkpointId, timestamp);
				
				// now draw the state snapshot
				final StreamOperator<?>[] allOperators = operatorChain.getAllOperators();
				final StreamOperatorNonPartitionedState[] nonPartitionedStates = new StreamOperatorNonPartitionedState[allOperators.length];
				final HashMap<Integer, StateHandle<?>> chainedPartitionedStates = new HashMap<>();

				boolean hasAsyncStates = false;

				for (int i = 0; i < allOperators.length; i++) {
					StreamOperator<?> operator = allOperators[i];
					if (operator != null) {
						StreamOperatorState state = operator.snapshotOperatorState(checkpointId, timestamp);
						StreamOperatorNonPartitionedState nonPartitionedState = state.getNonPartitionedState();
						StreamOperatorPartitionedState partitionedState = state.getPartitionedState();

						if (nonPartitionedState.getOperatorState() instanceof AsynchronousStateHandle) {
							hasAsyncStates = true;
						}
						if (nonPartitionedState.getFunctionState() instanceof AsynchronousStateHandle) {
							hasAsyncStates = true;
						}

						nonPartitionedStates[i] = nonPartitionedState.isEmpty() ? null : nonPartitionedState;

						if (partitionedState != null) {
							for (Map.Entry<Integer, PartitionedStateSnapshot> keyGroupState : partitionedState.entrySet()) {
								PartitionedStateSnapshot partitionedStateSnapshot = keyGroupState.getValue();

								if (partitionedStateSnapshot != null) {
									// check if any of the KvStateSnapshots asynchronous is
									for (KvStateSnapshot<?, ?, ?> kvSnapshot : partitionedStateSnapshot.values()) {
										if (kvSnapshot instanceof AsynchronousKvStateSnapshot) {
											hasAsyncStates = true;
										}
									}
								}

								// Group all key group states with the same key group index but
								// coming from different streaming operators together.
								ChainedKeyGroupState chainedKeyGroupState;

								if (!chainedPartitionedStates.containsKey(keyGroupState.getKey())) {
									chainedKeyGroupState = new ChainedKeyGroupState(allOperators.length);
									chainedPartitionedStates.put(keyGroupState.getKey(), chainedKeyGroupState);
								} else {
									chainedKeyGroupState = (ChainedKeyGroupState) chainedPartitionedStates.get(keyGroupState.getKey());
								}

								// Add this key group state belonging to the i-th operator to the
								// key group chain identified by the key group index
								chainedKeyGroupState.put(i, keyGroupState.getValue());
							}
						}
					}
				}

				if (!isRunning) {
					// Rethrow the cancel exception because some state backends could swallow
					// exceptions and seem to exit cleanly.
					throw new CancelTaskException();
				}

				StreamTaskState streamTaskState = new StreamTaskState(nonPartitionedStates);

				if (streamTaskState.isEmpty() && chainedPartitionedStates.isEmpty()) {
					getEnvironment().acknowledgeCheckpoint(checkpointId);
				} else if (!hasAsyncStates) {
					getEnvironment().acknowledgeCheckpoint(checkpointId, streamTaskState, chainedPartitionedStates);
				} else {
					// start a Thread that does the asynchronous materialization and
					// then sends the checkpoint acknowledge

					String threadName = "Materialize checkpoint state " + checkpointId + " - " + getName();
					Thread checkpointThread = new Thread(threadName) {
						@Override
						public void run() {
							try {
								for (StreamOperatorNonPartitionedState state : nonPartitionedStates) {
									if (state != null) {
										if (state.getFunctionState() instanceof AsynchronousStateHandle) {
											AsynchronousStateHandle<Serializable> asyncState = (AsynchronousStateHandle<Serializable>) state.getFunctionState();
											state.setFunctionState(asyncState.materialize());
										}
										if (state.getOperatorState() instanceof AsynchronousStateHandle) {
											AsynchronousStateHandle<?> asyncState = (AsynchronousStateHandle<?>) state.getOperatorState();
											state.setOperatorState(asyncState.materialize());
										}
									}
								}

								for (StateHandle<?> stateHandle: chainedPartitionedStates.values()) {
									ChainedKeyGroupState chainedKeyGroupState = (ChainedKeyGroupState) stateHandle;
									for (PartitionedStateSnapshot keyGroupState: chainedKeyGroupState.getState(getUserCodeClassLoader()).values()) {
										for (Map.Entry<String, KvStateSnapshot<?, ?, ?>> entry: keyGroupState.entrySet()) {
											KvStateSnapshot<?, ?, ?> kvStateSnapshot = entry.getValue();
											if (kvStateSnapshot instanceof AsynchronousKvStateSnapshot) {
												AsynchronousKvStateSnapshot<?, ?, ?> asyncHandle = (AsynchronousKvStateSnapshot<?, ?, ?>) kvStateSnapshot;
												entry.setValue(asyncHandle.materialize());
											}
										}
									}
								}

								StreamTaskState streamTaskState = new StreamTaskState(nonPartitionedStates);
								getEnvironment().acknowledgeCheckpoint(checkpointId, streamTaskState, chainedPartitionedStates);
								LOG.debug("Finished asynchronous checkpoints for checkpoint {} on task {}", checkpointId, getName());
							}
							catch (Exception e) {
								if (isRunning()) {
									LOG.error("Caught exception while materializing asynchronous checkpoints.", e);
								}
								if (asyncException == null) {
									asyncException = new AsynchronousException(e);
								}
							}
							asyncCheckpointThreads.remove(this);
						}
					};

					asyncCheckpointThreads.add(checkpointThread);
					checkpointThread.setDaemon(true);
					checkpointThread.start();
				}
				return true;
			} else {
				return false;
			}
		}
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

	/**
	 * Registers a timer.
	 */
	public ScheduledFuture<?> registerTimer(final long timestamp, final Triggerable target) {
		long delay = Math.max(timestamp - System.currentTimeMillis(), 0);

		return timerService.schedule(
			new TriggerTask(this, lock, target, timestamp),
			delay,
			TimeUnit.MILLISECONDS);
	}

	/**
	 * Check whether an exception was thrown in a Thread other than the main Thread. (For example
	 * in the processing-time trigger Thread). This will rethrow that exception in case on
	 * occured.
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
}
