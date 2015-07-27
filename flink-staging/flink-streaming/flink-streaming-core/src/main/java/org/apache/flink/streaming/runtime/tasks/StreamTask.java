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
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.functors.NotNullPredicate;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.accumulators.AccumulatorRegistry;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointNotificationOperator;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointedOperator;
import org.apache.flink.runtime.jobgraph.tasks.OperatorStateCarrier;
import org.apache.flink.runtime.state.FileStateHandle;
import org.apache.flink.runtime.state.LocalStateHandle;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.runtime.state.StateHandleProvider;
import org.apache.flink.runtime.util.event.EventListener;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.StatefulStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.state.OperatorStateHandle;
import org.apache.flink.streaming.api.state.WrapperStateHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class StreamTask<OUT, O extends StreamOperator<OUT>> extends AbstractInvokable implements
		OperatorStateCarrier<StateHandle<Serializable>>, CheckpointedOperator, CheckpointNotificationOperator {

	private static final Logger LOG = LoggerFactory.getLogger(StreamTask.class);

	protected final Object checkpointLock = new Object();

	protected StreamConfig configuration;

	protected OutputHandler<OUT> outputHandler;

	protected O streamOperator;

	protected boolean hasChainedOperators;

	protected volatile boolean isRunning = false;

	protected List<StreamingRuntimeContext> contexts;

	protected StreamingRuntimeContext headContext;

	protected ClassLoader userClassLoader;
	
	private EventListener<CheckpointBarrier> checkpointBarrierListener;

	public StreamTask() {
		streamOperator = null;
		checkpointBarrierListener = new CheckpointBarrierListener();
		contexts = new ArrayList<StreamingRuntimeContext>();
	}

	@Override
	public void registerInputOutput() {
		this.userClassLoader = getUserCodeClassLoader();
		this.configuration = new StreamConfig(getTaskConfiguration());

		streamOperator = configuration.getStreamOperator(userClassLoader);

		// Create and register Accumulators
		Environment env = getEnvironment();
		AccumulatorRegistry accumulatorRegistry = env.getAccumulatorRegistry();
		Map<String, Accumulator<?, ?>> accumulatorMap = accumulatorRegistry.getUserMap();
		AccumulatorRegistry.Reporter reporter = accumulatorRegistry.getReadWriteReporter();

		outputHandler = new OutputHandler<OUT>(this, accumulatorMap, reporter);

		if (streamOperator != null) {
			// IterationHead and IterationTail don't have an Operator...

			//Create context of the head operator
			headContext = createRuntimeContext(configuration, accumulatorMap);
			this.contexts.add(headContext);
			streamOperator.setup(outputHandler.getOutput(), headContext);
		}

		hasChainedOperators = outputHandler.getChainedOperators().size() != 1;
	}

	public String getName() {
		return getEnvironment().getTaskName();
	}

	public StreamingRuntimeContext createRuntimeContext(StreamConfig conf, Map<String, Accumulator<?,?>> accumulatorMap) {
		Environment env = getEnvironment();
		String operatorName = conf.getStreamOperator(userClassLoader).getClass().getSimpleName();

		KeySelector<?,Serializable> statePartitioner = conf.getStatePartitioner(userClassLoader);

		return new StreamingRuntimeContext(operatorName, env, getUserCodeClassLoader(),
				getExecutionConfig(), statePartitioner, getStateHandleProvider(), accumulatorMap);
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
					return new LocalStateHandle.LocalStateHandleProvider<Serializable>();
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
		for (StreamOperator<?> operator : outputHandler.getChainedOperators()) {
			if (operator != null) {
				operator.open(getTaskConfiguration());
			}
		}
	}

	protected void closeOperator() throws Exception {
		// We need to close them first to last, since upstream operators in the chain might emit
		// elements in their close methods.
		for (int i = outputHandler.getChainedOperators().size()-1; i >= 0; i--) {
			StreamOperator<?> operator = outputHandler.getChainedOperators().get(i);
			if (operator != null) {
				operator.close();
			}
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

	public EventListener<CheckpointBarrier> getCheckpointBarrierListener() {
		return this.checkpointBarrierListener;
	}

	// ------------------------------------------------------------------------
	//  Checkpoint and Restore
	// ------------------------------------------------------------------------

	@SuppressWarnings("unchecked")
	@Override
	public void setInitialState(StateHandle<Serializable> stateHandle) throws Exception {

		// We retrieve end restore the states for the chained oeprators.
		List<Tuple2<StateHandle<Serializable>, Map<String, OperatorStateHandle>>> chainedStates = (List<Tuple2<StateHandle<Serializable>, Map<String, OperatorStateHandle>>>) stateHandle.getState();

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

		synchronized (checkpointLock) {
			if (isRunning) {
				try {
					LOG.debug("Starting checkpoint {} on task {}", checkpointId, getName());

					// We wrap the states of the chained operators in a list, marking non-stateful oeprators with null
					List<Tuple2<StateHandle<Serializable>, Map<String, OperatorStateHandle>>> chainedStates = new ArrayList<Tuple2<StateHandle<Serializable>, Map<String, OperatorStateHandle>>>();

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

	@SuppressWarnings("rawtypes")
	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		synchronized (checkpointLock) {

			for (StreamOperator<?> chainedOperator : outputHandler.getChainedOperators()) {
				if (chainedOperator instanceof StatefulStreamOperator) {
					((StatefulStreamOperator) chainedOperator).notifyCheckpointComplete(checkpointId);
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
}
