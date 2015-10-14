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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.OperatorState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.state.KvState;
import org.apache.flink.streaming.api.state.KvStateSnapshot;
import org.apache.flink.streaming.api.state.StateBackend;
import org.apache.flink.streaming.runtime.operators.Triggerable;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamTaskState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	
	private transient StreamConfig config;

	protected transient Output<StreamRecord<OUT>> output;

	/** The runtime context for UDFs */
	private transient StreamingRuntimeContext runtimeContext;

	// ---------------- key/value state ------------------
	
	/** key selector used to get the key for the state. Non-null only is the operator uses key/value state */
	private transient KeySelector<?, ?> stateKeySelector;
	
	private transient KvState<?, ?, ?> keyValueState;
	
	private transient KvStateSnapshot<?, ?, ?> keyValueStateSnapshot;
	
	// ------------------------------------------------------------------------
	//  Life Cycle
	// ------------------------------------------------------------------------

	@Override
	public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<OUT>> output) {
		this.container = containingTask;
		this.config = config;
		this.output = output;
		this.runtimeContext = new StreamingRuntimeContext(this, container.getEnvironment(), container.getAccumulatorMap());
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
		if (keyValueState != null) {
			keyValueState.dispose();
		}
	}
	
	// ------------------------------------------------------------------------
	//  Checkpointing
	// ------------------------------------------------------------------------

	@Override
	public StreamTaskState snapshotOperatorState(long checkpointId, long timestamp) throws Exception {
		// here, we deal with key/value state snapshots
		
		StreamTaskState state = new StreamTaskState();
		if (keyValueState != null) {
			KvStateSnapshot<?, ?, ?> snapshot = keyValueState.shapshot(checkpointId, timestamp);
			state.setKvState(snapshot);
		}
		
		return state;
	}
	
	@Override
	public void restoreState(StreamTaskState state) throws Exception {
		// restore the key/value state. the actual restore happens lazily, when the function requests
		// the state again, because the restore method needs information provided by the user function
		keyValueStateSnapshot = state.getKvState();
	}
	
	@Override
	public void notifyOfCompletedCheckpoint(long checkpointId) throws Exception {
		// by default, nothing needs a notification of checkpoint completion
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
		return config;
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

	public StateBackend<?> getStateBackend() {
		return container.getStateBackend();
	}

	/**
	 * Register a timer callback. At the specified time the {@link Triggerable} will be invoked.
	 * This call is guaranteed to not happen concurrently with method calls on the operator.
	 *
	 * @param time The absolute time in milliseconds.
	 * @param target The target to be triggered.
	 */
	protected void registerTimer(long time, Triggerable target) {
		container.registerTimer(time, target);
	}

	/**
	 * Creates a key/value state handle, using the state backend configured for this task.
	 *
	 * @param stateType The type information for the state type, used for managed memory and state snapshots.
	 * @param defaultValue The default value that the state should return for keys that currently have
	 *                     no value associated with them 
	 *
	 * @param <V> The type of the state value.
	 *
	 * @return The key/value state for this operator.
	 *
	 * @throws IllegalStateException Thrown, if the key/value state was already initialized.
	 * @throws Exception Thrown, if the state backend cannot create the key/value state.
	 */
	protected <V> OperatorState<V> createKeyValueState(
			TypeInformation<V> stateType, V defaultValue) throws Exception
	{
		return createKeyValueState(stateType.createSerializer(getExecutionConfig()), defaultValue);
	}
	
	/**
	 * Creates a key/value state handle, using the state backend configured for this task.
	 * 
	 * @param valueSerializer The type serializer for the state type, used for managed memory and state snapshots.
	 * @param defaultValue The default value that the state should return for keys that currently have
	 *                     no value associated with them 
	 * 
	 * @param <K> The type of the state key.
	 * @param <V> The type of the state value.
	 * @param <Backend> The type of the state backend that creates the key/value state.
	 * 
	 * @return The key/value state for this operator.
	 * 
	 * @throws IllegalStateException Thrown, if the key/value state was already initialized.
	 * @throws Exception Thrown, if the state backend cannot create the key/value state.
	 */
	protected <K, V, Backend extends StateBackend<Backend>> OperatorState<V> createKeyValueState(
			TypeSerializer<V> valueSerializer, V defaultValue) throws Exception
	{
		if (keyValueState != null) {
			throw new IllegalStateException("The key/value state has already been created");
		}
		
		// first time state access, make sure we load the state partitioner
		if (stateKeySelector == null) {
			stateKeySelector = config.getStatePartitioner(getUserCodeClassloader());
			if (stateKeySelector == null) {
				throw new UnsupportedOperationException("The function or operator is not executed " +
						"on a KeyedStream and can hence not access the key/value state");
			}
		}
		
		// create the key and value serializers
		TypeSerializer<K> keySerializer = config.getStateKeySerializer(getUserCodeClassloader());
		if (keySerializer == null) {
			throw new Exception("State key serializer has not been configured in the config.");
		}
		
		@SuppressWarnings("unchecked")
		Backend stateBackend = (Backend) container.getStateBackend();
		
		// check whether we restore the key/value state from a snapshot, or create a new blank one
		if (keyValueStateSnapshot != null) {
			@SuppressWarnings("unchecked")
			KvStateSnapshot<K, V, Backend> snapshot = (KvStateSnapshot<K, V, Backend>) keyValueStateSnapshot;

			KvState<K, V, Backend> kvstate = snapshot.restoreState(
					stateBackend, keySerializer, valueSerializer, defaultValue, getUserCodeClassloader());
			keyValueState = kvstate;
			
			// make sure we have no redundant copies in memory, let the GC clean up
			keyValueStateSnapshot = null;
			
			return kvstate;
		}
		else {
			// create a new blank key/value state
			KvState<K, V, Backend> kvstate = stateBackend.createKvState(keySerializer, valueSerializer, defaultValue);
			keyValueState = kvstate;
			return kvstate;
		}
	}
	
	@Override
	@SuppressWarnings({"unchecked", "rawtypes"})
	public void setKeyContextElement(StreamRecord record) throws Exception {
		if (stateKeySelector != null && keyValueState != null) {
			KvState kv = keyValueState;
			KeySelector selector = stateKeySelector;
			kv.setCurrentKey(selector.getKey(record.getValue()));
		}
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
