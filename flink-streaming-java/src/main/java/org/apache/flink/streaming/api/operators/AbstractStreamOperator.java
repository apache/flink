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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.KeyGroupAssigner;
import org.apache.flink.api.common.state.PartitionedState;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.state.KeyGroupStateBackend;
import org.apache.flink.runtime.state.PartitionedStateSnapshot;
import org.apache.flink.runtime.state.StateBackendFactory;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackendFactory;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.taskmanager.TaskManagerRuntimeInfo;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.streaming.runtime.operators.Triggerable;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamOperatorState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledFuture;

/**
 * Base class for all stream operators. Operators that contain a user function should extend the class 
 * {@link AbstractUdfStreamOperator} instead (which is a specialized subclass of this class). 
 * 
 * <p>For concrete implementations, one of the following two interfaces must also be implemented, to
 * mark the operator as unary or binary:
 * {@link org.apache.flink.streaming.api.operators.OneInputStreamOperator} or
 * {@link org.apache.flink.streaming.api.operators.TwoInputStreamOperator}.
 *
 * <p>Methods of {@code StreamOperator} are guaranteed not to be called concurrently. Also, if using
 * the timer service, timer callbacks are also guaranteed not to be called concurrently with
 * methods on {@code StreamOperator}.
 *
 * @param <OUT> The output type of the operator
 */
@PublicEvolving
public abstract class AbstractStreamOperator<OUT>
		implements StreamOperator<OUT>, java.io.Serializable {

	private static final long serialVersionUID = 1L;
	
	/** The logger used by the operator class and its subclasses */
	protected static final Logger LOG = LoggerFactory.getLogger(AbstractStreamOperator.class);

	// ----------- configuration properties -------------

	// A sane default for most operators
	protected ChainingStrategy chainingStrategy = ChainingStrategy.HEAD;
	
	private boolean inputCopyDisabled = false;
	
	// ---------------- runtime fields ------------------

	/** The task that contains this operator (and other operators in the same chain) */
	private transient StreamTask<?, ?> container;
	
	private transient StreamConfig stream;

	protected transient Output<StreamRecord<OUT>> output;

	/** The runtime context for UDFs */
	private transient StreamingRuntimeContext runtimeContext;


	// ---------------- key/value state ------------------

	/** key selector used to get the key for the state. Non-null only is the operator uses key/value state */
	private transient KeySelector<?, ?> stateKeySelector1;
	private transient KeySelector<?, ?> stateKeySelector2;

	/** The state backend that stores the state and checkpoints for this task */
	private transient AbstractStateBackend stateBackend;

	/** The state backend that stores the partitioned state */
	private transient KeyGroupStateBackend<?> keyGroupStateBackend;

	// ------------------------------------------------------------------------
	//  Life Cycle
	// ------------------------------------------------------------------------

	@Override
	public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<OUT>> output) {
		this.container = containingTask;
		this.stream = config;
		this.output = output;
		this.runtimeContext = new StreamingRuntimeContext(this, container.getEnvironment(), container.getAccumulatorMap());

		stateKeySelector1 = config.getStatePartitioner(0, getUserCodeClassloader());
		stateKeySelector2 = config.getStatePartitioner(1, getUserCodeClassloader());

		try {
			// create state backend for non-partitioned data
			String operatorIdentifier = getClass().getSimpleName() + "_" + config.getVertexID() + "_" + runtimeContext.getIndexOfThisSubtask();
			stateBackend = createStateBackend(
				config,
				operatorIdentifier,
				getUserCodeClassloader(),
				container.getEnvironment());
		} catch (Exception e) {
			throw new RuntimeException("Could not initialize state backend. ", e);
		}

		// create the KeyGroupedStateBackend if we are a keyed stream operator
		if (stateKeySelector1 != null) {
			try {
				TypeSerializer<Object> keySerializer = config.getStateKeySerializer(getUserCodeClassloader());
				KeyGroupAssigner<Object> keyGroupAssigner = config.getKeyGroupAssigner(getUserCodeClassloader());
				keyGroupStateBackend = stateBackend.createKeyGroupStateBackend(keySerializer, keyGroupAssigner);
			} catch (Exception e) {
				throw new RuntimeException("Could not initialize key grouped state backend.", e);
			}
		}

	}

	/**
	 * This method is called immediately before any elements are processed, it should contain the
	 * operator's initialization logic.
	 *
	 * <p>The default implementation does nothing.
	 * 
	 * @throws Exception An exception in this method causes the operator to fail.
	 */
	@Override
	public void open() throws Exception {}

	/**
	 * This method is called after all records have been added to the operators via the methods
	 * {@link org.apache.flink.streaming.api.operators.OneInputStreamOperator#processElement(StreamRecord)}, or
	 * {@link org.apache.flink.streaming.api.operators.TwoInputStreamOperator#processElement1(StreamRecord)} and
	 * {@link org.apache.flink.streaming.api.operators.TwoInputStreamOperator#processElement2(StreamRecord)}.

	 * <p>The method is expected to flush all remaining buffered data. Exceptions during this flushing
	 * of buffered should be propagated, in order to cause the operation to be recognized asa failed,
	 * because the last data items are not processed properly.
	 *
	 * @throws Exception An exception in this method causes the operator to fail.
	 */
	@Override
	public void close() throws Exception {}
	
	/**
	 * This method is called at the very end of the operator's life, both in the case of a successful
	 * completion of the operation, and in the case of a failure and canceling.
	 *
	 * This method is expected to make a thorough effort to release all resources
	 * that the operator has acquired.
	 */
	@Override
	public void dispose() {
		if (stateBackend != null) {
			try {
				stateBackend.close();
			} catch (Exception e) {
				throw new RuntimeException("Error while closing/disposing state backend.", e);
			}
		}

		if (keyGroupStateBackend != null) {
			try {
				keyGroupStateBackend.close();
			} catch (Exception e) {
				throw new RuntimeException("Error while closing/disposing key group state backend.", e);
			}
		}
	}
	
	// ------------------------------------------------------------------------
	//  Checkpointing
	// ------------------------------------------------------------------------

	@Override
	public StreamOperatorState snapshotOperatorState(long checkpointId, long timestamp) throws Exception {
		return new StreamOperatorState();
	}

	@Override
	public Map<Integer, PartitionedStateSnapshot> snapshotKvState(long checkpointId, long timestamp) throws Exception {
		// here, we deal with key/value state snapshots
		if (keyGroupStateBackend != null) {
			return keyGroupStateBackend.snapshotPartitionedState(checkpointId, timestamp);
		} else {
			return null;
		}
	}
	
	@Override
	public void restoreState(StreamOperatorState state, long recoveryTimestamp) throws Exception {
	}

	@Override
	public void restoreKvState(Map<Integer, PartitionedStateSnapshot> keyGroupStates, long recoveryTimestamp) throws Exception {
		// restore the key/value state. the actual restore happens lazily, when the function requests
		// the state again, because the restore method needs information provided by the user function
		if (keyGroupStateBackend != null) {
			keyGroupStateBackend.restorePartitionedState(keyGroupStates, recoveryTimestamp);
		}
	}
	
	@Override
	public void notifyOfCompletedCheckpoint(long checkpointId) throws Exception {
		if (keyGroupStateBackend != null) {
			keyGroupStateBackend.notifyCompletedCheckpoint(checkpointId);
		}
	}

	// ------------------------------------------------------------------------
	//  Properties and Services
	// ------------------------------------------------------------------------

	/**
	 * Gets the execution config defined on the execution environment of the job to which this
	 * operator belongs.
	 * 
	 * @return The job's execution config.
	 */
	public ExecutionConfig getExecutionConfig() {
		return container.getExecutionConfig();
	}
	
	public StreamConfig getOperatorConfig() {
		return stream;
	}
	
	public StreamTask<?, ?> getContainingTask() {
		return container;
	}
	
	public ClassLoader getUserCodeClassloader() {
		return container.getUserCodeClassLoader();
	}
	
	/**
	 * Returns a context that allows the operator to query information about the execution and also
	 * to interact with systems such as broadcast variables and managed state. This also allows
	 * to register timers.
	 */
	public StreamingRuntimeContext getRuntimeContext() {
		return runtimeContext;
	}

	public AbstractStateBackend getStateBackend() {
		return stateBackend;
	}

	public KeyGroupStateBackend<?> getKeyGroupStateBackend() {
		if (keyGroupStateBackend != null) {
			return keyGroupStateBackend;
		} else {
			throw new RuntimeException("KeyGroupStateBackend has not been set. This indicates that" +
				"the AbstractStreamOperator is non-partitioned.");
		}
	}

	/**
	 * Register a timer callback. At the specified time the {@link Triggerable} will be invoked.
	 * This call is guaranteed to not happen concurrently with method calls on the operator.
	 *
	 * @param time The absolute time in milliseconds.
	 * @param target The target to be triggered.
	 */
	protected ScheduledFuture<?> registerTimer(long time, Triggerable target) {
		return container.registerTimer(time, target);
	}

	/**
	 * Creates a partitioned state handle, using the state backend configured for this task.
	 * 
	 * @throws IllegalStateException Thrown, if the key/value state was already initialized.
	 * @throws Exception Thrown, if the state backend cannot create the key/value state.
	 */
	protected <S extends PartitionedState> S getPartitionedState(StateDescriptor<S, ?> stateDescriptor) throws Exception {
		return this.getPartitionedState(null, VoidSerializer.INSTANCE, stateDescriptor);
	}

	/**
	 * Creates a partitioned state handle, using the state backend configured for this task.
	 *
	 * @throws IllegalStateException Thrown, if the key/value state was already initialized.
	 * @throws Exception Thrown, if the state backend cannot create the key/value state.
	 */
	@SuppressWarnings("unchecked")
	protected <S extends PartitionedState, N> S getPartitionedState(N namespace, TypeSerializer<N> namespaceSerializer, StateDescriptor<S, ?> stateDescriptor) throws Exception {
		if (getKeyGroupStateBackend() != null) {
			return getKeyGroupStateBackend().getPartitionedState(
				namespace,
				namespaceSerializer,
				stateDescriptor);
		} else {
			throw new RuntimeException("Cannot create partitioned state. The key grouped state " +
				"backend has not been set. This indicates that the operator is not " +
				"partitioned/keyed.");
		}
	}

	@Override
	@SuppressWarnings({"unchecked", "rawtypes"})
	public void setKeyContextElement1(StreamRecord record) throws Exception {
		if (stateKeySelector1 != null) {
			Object key = ((KeySelector) stateKeySelector1).getKey(record.getValue());

			setKeyContext(key);
		}
	}

	@Override
	@SuppressWarnings({"unchecked", "rawtypes"})
	public void setKeyContextElement2(StreamRecord record) throws Exception {
		if (stateKeySelector2 != null) {
			Object key = ((KeySelector) stateKeySelector2).getKey(record.getValue());

			setKeyContext(key);
		}
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	public void setKeyContext(Object key) {
		if (keyGroupStateBackend != null) {
			try {
				((KeyGroupStateBackend<Object>)keyGroupStateBackend).setCurrentKey(key);
			} catch (Exception e) {
				throw new RuntimeException("Exception occurred while setting the current key context.", e);
			}
		} else {
			throw new RuntimeException("Could not set the current key context, because the " +
				"KeyGroupStateBackend has not been initialized.");
		}
	}

	// ------------------------------------------------------------------------
	//  State backend
	// ------------------------------------------------------------------------

	protected AbstractStateBackend createStateBackend(
			StreamConfig configuration,
			String operatorIdentifier,
			ClassLoader classLoader,
			Environment environment) throws Exception {
		AbstractStateBackend stateBackend = configuration.getStateBackend(classLoader);

		if (stateBackend != null) {
			// backend has been configured on the environment
			LOG.info("Using user-defined state backend: " + stateBackend);
		} else {
			// see if we have a backend specified in the configuration
			TaskManagerRuntimeInfo taskManagerRuntimeInfo = environment.getTaskManagerInfo();

			if (taskManagerRuntimeInfo != null) {
				Configuration flinkConfig = taskManagerRuntimeInfo.getConfiguration();
				String backendName = flinkConfig.getString(ConfigConstants.STATE_BACKEND, null);

				if (backendName == null) {
					LOG.warn("No state backend has been specified, using default state backend (Memory / JobManager)");
					backendName = "jobmanager";
				}

				backendName = backendName.toLowerCase();
				switch (backendName) {
					case "jobmanager":
						LOG.info("State backend is set to heap memory (checkpoint to jobmanager)");
						stateBackend = MemoryStateBackend.create();
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
								Class.forName(backendName, false, classLoader).asSubclass(StateBackendFactory.class);

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
			} else {
				throw new IllegalConfigurationException("Cannot create a state backend because the TaskManagerRuntimeInfo has not been set.");
			}
		}
		stateBackend.initializeForJob(environment, operatorIdentifier);
		return stateBackend;

	}
	
	// ------------------------------------------------------------------------
	//  Context and chaining properties
	// ------------------------------------------------------------------------
	
	@Override
	public final void setChainingStrategy(ChainingStrategy strategy) {
		this.chainingStrategy = strategy;
	}
	
	@Override
	public final ChainingStrategy getChainingStrategy() {
		return chainingStrategy;
	}
	
	@Override
	public boolean isInputCopyingDisabled() {
		return inputCopyDisabled;
	}

	/**
	 * Enable object-reuse for this operator instance. This overrides the setting in
	 * the {@link org.apache.flink.api.common.ExecutionConfig}
	 */
	public void disableInputCopy() {
		this.inputCopyDisabled = true;
	}
}
