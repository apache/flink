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

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.functors.NotNullPredicate;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.event.task.TaskEvent;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCommittingOperator;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointedOperator;
import org.apache.flink.runtime.jobgraph.tasks.OperatorStateCarrier;
import org.apache.flink.runtime.state.FileStateHandle;
import org.apache.flink.runtime.state.LocalStateHandle;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.runtime.state.StateHandleProvider;
import org.apache.flink.runtime.util.event.EventListener;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StatefulStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class StreamTask<OUT, O extends StreamOperator<OUT>> extends AbstractInvokable implements
		OperatorStateCarrier<StateHandle<Serializable>>, CheckpointedOperator, CheckpointCommittingOperator {

	private static final Logger LOG = LoggerFactory.getLogger(StreamTask.class);

	private final Object checkpointLock = new Object();

	protected StreamConfig configuration;

	protected OutputHandler<OUT> outputHandler;

	protected O streamOperator;

	protected boolean hasChainedOperators;

	protected volatile boolean isRunning = false;

	protected List<StreamingRuntimeContext> contexts;

	protected ClassLoader userClassLoader;
	
	private StateHandleProvider<Serializable> stateHandleProvider;

	private EventListener<TaskEvent> superstepListener;

	public StreamTask() {
		streamOperator = null;
		superstepListener = new SuperstepEventListener();
		contexts = new ArrayList<StreamingRuntimeContext>();
	}

	@Override
	public void registerInputOutput() {
		this.userClassLoader = getUserCodeClassLoader();
		this.configuration = new StreamConfig(getTaskConfiguration());
		this.stateHandleProvider = getStateHandleProvider();

		outputHandler = new OutputHandler<OUT>(this);

		streamOperator = configuration.getStreamOperator(userClassLoader);
		
		if (streamOperator != null) {
			//Create context of the head operator
			StreamingRuntimeContext headContext = createRuntimeContext(configuration);
			this.contexts.add(headContext);

			// IterationHead and IterationTail don't have an Operator...
			streamOperator.setup(outputHandler.getOutput(), headContext);
		}

		hasChainedOperators = !outputHandler.getChainedOperators().isEmpty();
	}

	public String getName() {
		return getEnvironment().getTaskName();
	}

	public StreamingRuntimeContext createRuntimeContext(StreamConfig conf) {
		Environment env = getEnvironment();
		return new StreamingRuntimeContext(conf.getStreamOperator(userClassLoader).getClass()
				.getSimpleName(), env, getUserCodeClassLoader(), getExecutionConfig());
	}
	
	private StateHandleProvider<Serializable> getStateHandleProvider() {

		StateHandleProvider<Serializable> provider = configuration
				.getStateHandleProvider(userClassLoader);

		// If the user did not specify a provider in the program we try to get it from the config
		if (provider == null) {
			String backendName = GlobalConfiguration.getString(ConfigConstants.STATE_BACKEND,
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
					return LocalStateHandle.createProvider();
				case FILESYSTEM:
					String checkpointDir = GlobalConfiguration.getString(ConfigConstants.STATE_BACKEND_FS_DIR, null);
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

	protected void openOperator() throws Exception {
		streamOperator.open(getTaskConfiguration());

		for (OneInputStreamOperator<?, ?> operator : outputHandler.chainedOperators) {
			operator.open(getTaskConfiguration());
		}
	}

	protected void closeOperator() throws Exception {
		streamOperator.close();

		// We need to close them first to last, since upstream operators in the chain might emit
		// elements in their close methods.
		for (int i = outputHandler.chainedOperators.size()-1; i >= 0; i--) {
			outputHandler.chainedOperators.get(i).close();
		}
	}

	protected void clearBuffers() throws IOException {
		if (outputHandler != null) {
			outputHandler.clearWriters();
		}
	}

	@Override
	public void cancel() {
		this.isRunning = false;
	}

	public EventListener<TaskEvent> getSuperstepListener() {
		return this.superstepListener;
	}

	// ------------------------------------------------------------------------
	//  Checkpoint and Restore
	// ------------------------------------------------------------------------

	/**
	 * Re-injects the user states into the map. Also set the state on the functions.
	 */
	@Override
	public void setInitialState(StateHandle<Serializable> stateHandle) throws Exception {
		// here, we later resolve the state handle into the actual state by
		// loading the state described by the handle from the backup store
		Serializable state = stateHandle.getState();

		if (hasChainedOperators) {
			@SuppressWarnings("unchecked")
			List<Serializable> chainedStates = (List<Serializable>) state;

			Serializable headState = chainedStates.get(0);
			if (headState != null) {
				if (streamOperator instanceof StatefulStreamOperator) {
					((StatefulStreamOperator) streamOperator).restoreInitialState(headState);
				}
			}

			for (int i = 1; i < chainedStates.size(); i++) {
				Serializable chainedState = chainedStates.get(i);
				if (chainedState != null) {
					StreamOperator chainedOperator = outputHandler.getChainedOperators().get(i - 1);
					if (chainedOperator instanceof StatefulStreamOperator) {
						((StatefulStreamOperator) chainedOperator).restoreInitialState(chainedState);
					}

				}
			}

		} else {
			if (streamOperator instanceof StatefulStreamOperator) {
				((StatefulStreamOperator) streamOperator).restoreInitialState(state);
			}

		}
	}

	/**
	 * This method is either called directly by the checkpoint coordinator, or called
	 * when all incoming channels have reported a barrier
	 */
	@Override
	public void triggerCheckpoint(long checkpointId, long timestamp) throws Exception {
		
		synchronized (checkpointLock) {
			if (isRunning) {
				try {
					LOG.info("Starting checkpoint {} on task {}", checkpointId, getName());
					
					// first draw the state that should go into checkpoint
					StateHandle<Serializable> state;
					try {

						Serializable userState = null;

						if (streamOperator instanceof StatefulStreamOperator) {
							userState = ((StatefulStreamOperator) streamOperator).getStateSnapshotFromFunction(checkpointId, timestamp);
						}


						if (hasChainedOperators) {
							// We construct a list of states for chained tasks
							List<Serializable> chainedStates = new ArrayList<Serializable>();

							chainedStates.add(userState);

							for (OneInputStreamOperator<?, ?> chainedOperator : outputHandler.getChainedOperators()) {
								if (chainedOperator instanceof StatefulStreamOperator) {
									chainedStates.add(((StatefulStreamOperator) chainedOperator).getStateSnapshotFromFunction(checkpointId, timestamp));
								}
							}

							userState = CollectionUtils.exists(chainedStates,
									NotNullPredicate.INSTANCE) ? (Serializable) chainedStates
									: null;
						}
						
						state = userState == null ? null : stateHandleProvider.createStateHandle(userState);
					}
					catch (Exception e) {
						throw new Exception("Error while drawing snapshot of the user state.", e);
					}
			
					// now emit the checkpoint barriers
					outputHandler.broadcastBarrier(checkpointId, timestamp);
					
					// now confirm the checkpoint
					if (state == null) {
						getEnvironment().acknowledgeCheckpoint(checkpointId);
					} else {
						getEnvironment().acknowledgeCheckpoint(checkpointId, state);
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
	public void confirmCheckpoint(long checkpointId, long timestamp) throws Exception {
		// we do nothing here so far. this should call commit on the source function, for example
		synchronized (checkpointLock) {
			if (streamOperator instanceof StatefulStreamOperator) {
				((StatefulStreamOperator) streamOperator).confirmCheckpointCompleted(checkpointId, timestamp);
			}

			if (hasChainedOperators) {
				for (OneInputStreamOperator<?, ?> chainedOperator : outputHandler.getChainedOperators()) {
					if (chainedOperator instanceof StatefulStreamOperator) {
						((StatefulStreamOperator) chainedOperator).confirmCheckpointCompleted(checkpointId, timestamp);
					}
				}
			}
		}
	}
	
	
	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		return getEnvironment().getTaskNameWithSubtasks();
	}

	// ------------------------------------------------------------------------

	private class SuperstepEventListener implements EventListener<TaskEvent> {

		@Override
		public void onEvent(TaskEvent event) {
			try {
				StreamingSuperstep sStep = (StreamingSuperstep) event;
				triggerCheckpoint(sStep.getId(), sStep.getTimestamp());
			}
			catch (Exception e) {
				throw new RuntimeException("Error triggering a checkpoint as the result of receiving checkpoint barrier", e);
			}
		}
	}
}
