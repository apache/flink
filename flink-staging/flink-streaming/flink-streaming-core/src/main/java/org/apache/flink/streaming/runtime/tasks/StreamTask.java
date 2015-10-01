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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.functors.NotNullPredicate;

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.accumulators.AccumulatorRegistry;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.StatefulTask;
import org.apache.flink.runtime.state.FileStateHandle;
import org.apache.flink.runtime.state.LocalStateHandle;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.runtime.state.StateHandleProvider;
import org.apache.flink.runtime.taskmanager.DispatcherThreadFactory;
import org.apache.flink.runtime.util.event.EventListener;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.StatefulStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.state.OperatorStateHandle;
import org.apache.flink.streaming.api.state.WrapperStateHandle;

import org.apache.flink.streaming.runtime.operators.Triggerable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Base Invokable for all {@code StreamTasks}. A {@code StreamTask} processes input and forwards
 * elements and watermarks to a {@link StreamOperator}.
 *
 * <pre>
 *     
 *  -- registerInputOutput()
 *         |
 *         +----> Create basic utils (config, etc) and load operators
 *         +----> operator specific init()
 *  
 *  -- restoreState()
 *  
 *  -- invoke()
 *        |
 *        +----> open operators()
 *        +----> run()
 *        +----> close operators()
 *        +----> common cleanup
 *        +----> operator specific cleanup()
 * </pre>
 *
 * <p>
 * {@code StreamTask} has a lock object called {@code lock}. All calls to methods on a
 * {@code StreamOperator} must be synchronized on this lock object to ensure that no methods
 * are called concurrently.
 * 
 * @param <OUT>
 * @param <O>
 */
public abstract class StreamTask<OUT, O extends StreamOperator<OUT>> extends AbstractInvokable implements StatefulTask<StateHandle<Serializable>> {

	/** The thread group that holds all trigger timer threads */
	public static final ThreadGroup TRIGGER_THREAD_GROUP = new ThreadGroup("Triggers");
	
	private static final Logger LOG = LoggerFactory.getLogger(StreamTask.class);

	/**
	 * All interaction with the {@code StreamOperator} must be synchronized on this lock object to ensure that
	 * we don't have concurrent method calls.
	 */
	protected final Object lock = new Object();

	private final EventListener<CheckpointBarrier> checkpointBarrierListener;
	
	protected final List<StreamingRuntimeContext> contexts;

	protected StreamingRuntimeContext headContext;
	
	protected StreamConfig configuration;

	protected ClassLoader userClassLoader;

	/** The executor service that */
	private ScheduledExecutorService timerService;

	/**
	 * This field is used to forward an exception that is caught in the timer thread. Subclasses
	 * must ensure that exceptions stored here get thrown on the actual execution Thread.
	 */
	protected volatile TimerException timerException = null;

	protected OutputHandler<OUT> outputHandler;

	protected O streamOperator;

	protected boolean hasChainedOperators;

	/** Flag to mark the task "in operation", in which case check
	 * needs to be initialized to true, so that early cancel() before invoke() behaves correctly */
	private volatile boolean isRunning;
	
	// ------------------------------------------------------------------------
	
	public StreamTask() {
		checkpointBarrierListener = new CheckpointBarrierListener();
		contexts = new ArrayList<>();
	}

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
	public final void registerInputOutput() throws Exception {
		LOG.debug("Begin initialization for {}", getName());
		
		userClassLoader = getUserCodeClassLoader();
		configuration = new StreamConfig(getTaskConfiguration());

		streamOperator = configuration.getStreamOperator(userClassLoader);

		// Create and register Accumulators
		AccumulatorRegistry accumulatorRegistry = getEnvironment().getAccumulatorRegistry();
		Map<String, Accumulator<?, ?>> accumulatorMap = accumulatorRegistry.getUserMap();
		AccumulatorRegistry.Reporter reporter = accumulatorRegistry.getReadWriteReporter();

		outputHandler = new OutputHandler<>(this, accumulatorMap, reporter);

		if (streamOperator != null) {
			// IterationHead and IterationTail don't have an Operator...

			//Create context of the head operator
			headContext = createRuntimeContext(configuration, accumulatorMap);
			this.contexts.add(headContext);
			streamOperator.setup(outputHandler.getOutput(), headContext);
		}

		hasChainedOperators = outputHandler.getChainedOperators().size() != 1;

		this.timerService = Executors.newSingleThreadScheduledExecutor(
				new DispatcherThreadFactory(TRIGGER_THREAD_GROUP, "Time Trigger for " + getName()));

		// operator specific initialization
		init();
		
		LOG.debug("Finish initialization for {}", getName());
	}
	
	@Override
	public final void invoke() throws Exception {
		LOG.debug("Invoking {}", getName());
		
		boolean disposed = false;
		try {
			openAllOperators();

			// let the task do its work
			isRunning = true;
			run();
			isRunning = false;
			
			if (LOG.isDebugEnabled()) {
				LOG.debug("Finished task {}", getName());
			}
			
			// make sure no further checkpoint and notification actions happen.
			// we make sure that no other thread is currently in the locked scope before
			// we close the operators by trying to acquire the checkpoint scope lock
			synchronized (lock) {}
			
			// this is part of the main logic, so if this fails, the task is considered failed
			closeAllOperators();
			
			// make sure all data is flushed
			outputHandler.flushOutputs();

			// make an attempt to dispose the operators such that failures in the dispose call
			// still let the computation fail
			tryDisposeAllOperators();
			disposed = true;
		}
		finally {
			isRunning = false;

			timerService.shutdown();
			
			// release the output resources. this method should never fail.
			if (outputHandler != null) {
				outputHandler.releaseOutputs();
			}

			// we must! perform this cleanup

			try {
				cleanup();
			}
			catch (Throwable t) {
				// catch and log the exception to not replace the original exception
				LOG.error("Error during cleanup of stream task.");
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
		cancelTask();
	}
	
	private void openAllOperators() throws Exception {
		for (StreamOperator<?> operator : outputHandler.getChainedOperators()) {
			if (operator != null) {
				operator.open(getTaskConfiguration());
			}
		}
	}

	private void closeAllOperators() throws Exception {
		// We need to close them first to last, since upstream operators in the chain might emit
		// elements in their close methods.
		for (int i = outputHandler.getChainedOperators().size() - 1; i >= 0; i--) {
			StreamOperator<?> operator = outputHandler.getChainedOperators().get(i);
			if (operator != null) {
				operator.close();
			}
		}
	}

	private void tryDisposeAllOperators() throws Exception {
		for (StreamOperator<?> operator : outputHandler.getChainedOperators()) {
			if (operator != null) {
				operator.dispose();
			}
		}
	}
	
	private void disposeAllOperators() {
		for (StreamOperator<?> operator : outputHandler.getChainedOperators()) {
			if (operator != null) {
				try {
					operator.dispose();
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
	@SuppressWarnings("FinalizeDoesntCallSuperFinalize")
	protected void finalize() {
		if (timerService != null) {
			if (!timerService.isTerminated()) {
				LOG.warn("Timer service was not shut down. Shutting down in finalize().");
			}
			timerService.shutdown();
		}
	}

	// ------------------------------------------------------------------------
	//  Access to properties and utilities
	// ------------------------------------------------------------------------

	/**
	 * Gets the name of the task, in the form "taskname (2/5)".
	 * @return The name of the task.
	 */
	public String getName() {
		return getEnvironment().getTaskNameWithSubtasks();
	}

	public Object getCheckpointLock() {
		return lock;
	}

	// ------------------------------------------------------------------------
	//  Checkpoint and Restore
	// ------------------------------------------------------------------------

	@SuppressWarnings("unchecked")
	@Override
	public void setInitialState(StateHandle<Serializable> stateHandle) throws Exception {

		// We retrieve end restore the states for the chained operators.
		List<Tuple2<StateHandle<Serializable>, Map<String, OperatorStateHandle>>> chainedStates = 
				(List<Tuple2<StateHandle<Serializable>, Map<String, OperatorStateHandle>>>) stateHandle.getState(this.userClassLoader);

		// We restore all stateful operators
		for (int i = 0; i < chainedStates.size(); i++) {
			Tuple2<StateHandle<Serializable>, Map<String, OperatorStateHandle>> state = chainedStates.get(i);
			// If state is not null we need to restore it
			if (state != null) {
				StreamOperator<?> chainedOperator = outputHandler.getChainedOperators().get(i);
				((StatefulStreamOperator<?>) chainedOperator).restoreInitialState(state);
			}
		}
	}

	@Override
	public void triggerCheckpoint(long checkpointId, long timestamp) throws Exception {

		LOG.debug("Starting checkpoint {} on task {}", checkpointId, getName());
		
		synchronized (lock) {
			if (isRunning) {
				try {
					// We wrap the states of the chained operators in a list, marking non-stateful operators with null
					List<Tuple2<StateHandle<Serializable>, Map<String, OperatorStateHandle>>> chainedStates = new ArrayList<>();

					// A wrapper handle is created for the List of statehandles
					WrapperStateHandle stateHandle;
					try {

						// We construct a list of states for chained tasks
						for (StreamOperator<?> chainedOperator : outputHandler.getChainedOperators()) {
							if (chainedOperator instanceof StatefulStreamOperator) {
								chainedStates.add(((StatefulStreamOperator<?>) chainedOperator)
										.getStateSnapshotFromFunction(checkpointId, timestamp));
							}else{
								chainedStates.add(null);
							}
						}

						stateHandle = CollectionUtils.exists(chainedStates,
								NotNullPredicate.INSTANCE) ? new WrapperStateHandle(chainedStates) : null;
					}
					catch (Exception e) {
						throw new Exception("Error while drawing snapshot of the user state.", e);
					}

					// now emit the checkpoint barriers
					outputHandler.broadcastBarrier(checkpointId, timestamp);

					// now confirm the checkpoint
					if (stateHandle == null) {
						getEnvironment().acknowledgeCheckpoint(checkpointId);
					} else {
						getEnvironment().acknowledgeCheckpoint(checkpointId, stateHandle);
					}
				}
				catch (Exception e) {
					if (isRunning) {
						throw e;
					}
				}
			}
		}
	}
	
	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		synchronized (lock) {
			if (isRunning) {
				for (StreamOperator<?> chainedOperator : outputHandler.getChainedOperators()) {
					if (chainedOperator instanceof StatefulStreamOperator) {
						((StatefulStreamOperator<?>) chainedOperator).notifyCheckpointComplete(checkpointId);
					}
				}
			}
		}
	}
	
	// ------------------------------------------------------------------------
	//  State backend
	// ------------------------------------------------------------------------
	
	private StateHandleProvider<Serializable> getStateHandleProvider() {
		StateHandleProvider<Serializable> provider = configuration.getStateHandleProvider(userClassLoader);

		// If the user did not specify a provider in the program we try to get it from the config
		if (provider == null) {
			Configuration flinkConfig = getEnvironment().getTaskManagerInfo().getConfiguration();
			String backendName = flinkConfig.getString(ConfigConstants.STATE_BACKEND,
					ConfigConstants.DEFAULT_STATE_BACKEND).toUpperCase();

			StateBackend backend;

			try {
				backend = StateBackend.valueOf(backendName);
			} catch (Exception e) {
				throw new RuntimeException(backendName + " is not a valid state backend.\nSupported backends: jobmanager, filesystem.");
			}

			switch (backend) {
				case JOBMANAGER:
					LOG.info("State backend for state checkpoints is set to jobmanager.");
					return new LocalStateHandle.LocalStateHandleProvider<>();
				case FILESYSTEM:
					String checkpointDir = flinkConfig.getString(ConfigConstants.STATE_BACKEND_FS_DIR, null);
					if (checkpointDir != null) {
						LOG.info("State backend for state checkpoints is set to filesystem with directory: "
								+ checkpointDir);
						return FileStateHandle.createProvider(checkpointDir);
					} else {
						throw new RuntimeException(
								"For filesystem checkpointing, a checkpoint directory needs to be specified.\nFor example: \"state.backend.dir: hdfs://checkpoints\"");
					}
				default:
					throw new RuntimeException("Backend " + backend + " is not supported yet.");
			}

		} else {
			LOG.info("Using user defined state backend for streaming checkpoitns.");
			return provider;
		}
	}

	private enum StateBackend {
		JOBMANAGER, FILESYSTEM
	}

	/**
	 * Registers a timer.
	 */
	public void registerTimer(final long timestamp, final Triggerable target) {
		long delay = Math.max(timestamp - System.currentTimeMillis(), 0);

		timerService.schedule(
				new TriggerTask(this, lock, target, timestamp),
				delay,
				TimeUnit.MILLISECONDS);
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	public StreamingRuntimeContext createRuntimeContext(StreamConfig conf, Map<String, Accumulator<?,?>> accumulatorMap) {
		KeySelector<?,Serializable> statePartitioner = conf.getStatePartitioner(userClassLoader);

		return new StreamingRuntimeContext(getEnvironment(), getExecutionConfig(),
				statePartitioner, getStateHandleProvider(), accumulatorMap, this);
	}
	
	@Override
	public String toString() {
		return getName();
	}

	// ------------------------------------------------------------------------

	public EventListener<CheckpointBarrier> getCheckpointBarrierListener() {
		return this.checkpointBarrierListener;
	}
	
	private class CheckpointBarrierListener implements EventListener<CheckpointBarrier> {

		@Override
		public void onEvent(CheckpointBarrier barrier) {
			try {
				triggerCheckpoint(barrier.getId(), barrier.getTimestamp());
			}
			catch (Exception e) {
				throw new RuntimeException("Error triggering a checkpoint as the result of receiving checkpoint barrier", e);
			}
		}
	}

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
					LOG.error("Caught exception while processing timer.", t);
					if (task.timerException == null) {
						task.timerException = new TimerException(t);
					}
				}
			}
		}
	}
}
