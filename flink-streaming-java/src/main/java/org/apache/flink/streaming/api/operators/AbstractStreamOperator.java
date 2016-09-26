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

import org.apache.commons.io.IOUtils;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.RunnableFuture;

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
		implements StreamOperator<OUT>, java.io.Serializable, KeyContext, StreamCheckpointedOperator {

	private static final long serialVersionUID = 1L;
	
	/** The logger used by the operator class and its subclasses */
	protected static final Logger LOG = LoggerFactory.getLogger(AbstractStreamOperator.class);

	// ----------- configuration properties -------------

	// A sane default for most operators
	protected ChainingStrategy chainingStrategy = ChainingStrategy.HEAD;

	// ---------------- runtime fields ------------------

	/** The task that contains this operator (and other operators in the same chain) */
	private transient StreamTask<?, ?> container;
	
	protected transient StreamConfig config;

	protected transient Output<StreamRecord<OUT>> output;

	/** The runtime context for UDFs */
	private transient StreamingRuntimeContext runtimeContext;


	// ---------------- key/value state ------------------

	/** key selector used to get the key for the state. Non-null only is the operator uses key/value state */
	private transient KeySelector<?, ?> stateKeySelector1;
	private transient KeySelector<?, ?> stateKeySelector2;

	/** Backend for keyed state. This might be empty if we're not on a keyed stream. */
	private transient AbstractKeyedStateBackend<?> keyedStateBackend;

	/** Operator state backend */
	private transient OperatorStateBackend operatorStateBackend;

	private transient Collection<OperatorStateHandle> lazyRestoreStateHandles;

	protected transient MetricGroup metrics;

	// ---------------- timers ------------------

	private transient Map<String, HeapInternalTimerService<?, ?>> timerServices;
	private transient Map<String, HeapInternalTimerService.RestoredTimers<?, ?>> restoredServices;


	// ---------------- two-input operator watermarks ------------------

	// We keep track of watermarks from both inputs, the combined input is the minimum
	// Once the minimum advances we emit a new watermark for downstream operators
	private long combinedWatermark = Long.MIN_VALUE;
	private long input1Watermark = Long.MIN_VALUE;
	private long input2Watermark = Long.MIN_VALUE;

	// ------------------------------------------------------------------------
	//  Life Cycle
	// ------------------------------------------------------------------------

	@Override
	public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<OUT>> output) {
		this.container = containingTask;
		this.config = config;
		String operatorName = containingTask.getEnvironment().getTaskInfo().getTaskName().split("->")[config.getChainIndex()].trim();
		
		this.metrics = container.getEnvironment().getMetricGroup().addOperator(operatorName);
		this.output = new CountingOutput(output, this.metrics.counter("numRecordsOut"));
		this.runtimeContext = new StreamingRuntimeContext(this, container.getEnvironment(), container.getAccumulatorMap());

		stateKeySelector1 = config.getStatePartitioner(0, getUserCodeClassloader());
		stateKeySelector2 = config.getStatePartitioner(1, getUserCodeClassloader());
	}
	
	public MetricGroup getMetricGroup() {
		return metrics;
	}

	@Override
	public void restoreState(Collection<OperatorStateHandle> stateHandles) {
		this.lazyRestoreStateHandles = stateHandles;
	}

	/**
	 * This method is called immediately before any elements are processed, it should contain the
	 * operator's initialization logic, e.g. state initialization.
	 *
	 * <p>The default implementation does nothing.
	 * 
	 * @throws Exception An exception in this method causes the operator to fail.
	 */
	@Override
	public void open() throws Exception {
		initOperatorState();
		initKeyedState();

		if (timerServices == null) {
			timerServices = new HashMap<>();
		}
	}

	private void initKeyedState() {
		try {
			TypeSerializer<Object> keySerializer = config.getStateKeySerializer(getUserCodeClassloader());
			// create a keyed state backend if there is keyed state, as indicated by the presence of a key serializer
			if (null != keySerializer) {

				KeyGroupRange subTaskKeyGroupRange = KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(
						container.getEnvironment().getTaskInfo().getNumberOfKeyGroups(),
						container.getEnvironment().getTaskInfo().getNumberOfParallelSubtasks(),
						container.getIndexInSubtaskGroup());

				this.keyedStateBackend = container.createKeyedStateBackend(
						keySerializer,
						container.getConfiguration().getNumberOfKeyGroups(getUserCodeClassloader()),
						subTaskKeyGroupRange);

			}

		} catch (Exception e) {
			throw new IllegalStateException("Could not initialize keyed state backend.", e);
		}
	}

	private void initOperatorState() {
		try {
			// create an operator state backend
			this.operatorStateBackend = container.createOperatorStateBackend(this, lazyRestoreStateHandles);
		} catch (Exception e) {
			throw new IllegalStateException("Could not initialize operator state backend.", e);
		}
	}

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
	public void dispose() throws Exception {

		if (operatorStateBackend != null) {
			IOUtils.closeQuietly(operatorStateBackend);
			operatorStateBackend.dispose();
		}

		if (keyedStateBackend != null) {
			IOUtils.closeQuietly(keyedStateBackend);
			keyedStateBackend.dispose();
		}
	}

	@Override
	public RunnableFuture<OperatorStateHandle> snapshotState(
			long checkpointId, long timestamp, CheckpointStreamFactory streamFactory) throws Exception {

		return operatorStateBackend != null ?
				operatorStateBackend.snapshot(checkpointId, timestamp, streamFactory) : null;
	}

	@Override
	public void notifyOfCompletedCheckpoint(long checkpointId) throws Exception {}

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

	@SuppressWarnings("rawtypes, unchecked")
	public <K> KeyedStateBackend<K> getKeyedStateBackend() {

		if (null == keyedStateBackend) {
			initKeyedState();
		}

		return (KeyedStateBackend<K>) keyedStateBackend;
	}

	public OperatorStateBackend getOperatorStateBackend() {

		if (null == operatorStateBackend) {
			initOperatorState();
		}

		return operatorStateBackend;
	}

	/**
	 * Returns the {@link ProcessingTimeService} responsible for getting  the current
	 * processing time and registering timers.
	 */
	protected ProcessingTimeService getProcessingTimeService() {
		return container.getProcessingTimeService();
	}

	/**
	 * Creates a partitioned state handle, using the state backend configured for this task.
	 * 
	 * @throws IllegalStateException Thrown, if the key/value state was already initialized.
	 * @throws Exception Thrown, if the state backend cannot create the key/value state.
	 */
	protected <S extends State> S getPartitionedState(StateDescriptor<S, ?> stateDescriptor) throws Exception {
		return getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, stateDescriptor);
	}

	/**
	 * Creates a partitioned state handle, using the state backend configured for this task.
	 *
	 * @throws IllegalStateException Thrown, if the key/value state was already initialized.
	 * @throws Exception Thrown, if the state backend cannot create the key/value state.
	 */
	@SuppressWarnings("unchecked")
	protected <S extends State, N> S getPartitionedState(N namespace, TypeSerializer<N> namespaceSerializer, StateDescriptor<S, ?> stateDescriptor) throws Exception {
		if (keyedStateBackend != null) {
			return keyedStateBackend.getPartitionedState(
					namespace,
					namespaceSerializer,
					stateDescriptor);
		} else {
			throw new RuntimeException("Cannot create partitioned state. The keyed state " +
				"backend has not been set. This indicates that the operator is not " +
				"partitioned/keyed.");
		}
	}

	@Override
	@SuppressWarnings({"unchecked", "rawtypes"})
	public void setKeyContextElement1(StreamRecord record) throws Exception {
		setRawKeyContextElement(record, stateKeySelector1);
	}

	@Override
	@SuppressWarnings({"unchecked", "rawtypes"})
	public void setKeyContextElement2(StreamRecord record) throws Exception {
		setRawKeyContextElement(record, stateKeySelector2);
	}

	private void setRawKeyContextElement(StreamRecord record, KeySelector<?, ?> selector) throws Exception {
		if (selector != null) {
			Object key = ((KeySelector) selector).getKey(record.getValue());
			setCurrentKey(key);
		}
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	public void setCurrentKey(Object key) {
		if (keyedStateBackend != null) {
			try {
				// need to work around type restrictions
				@SuppressWarnings("unchecked,rawtypes")
				AbstractKeyedStateBackend rawBackend = (AbstractKeyedStateBackend) keyedStateBackend;

				rawBackend.setCurrentKey(key);
			} catch (Exception e) {
				throw new RuntimeException("Exception occurred while setting the current key context.", e);
			}
		}
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	public Object getCurrentKey() {
		if (keyedStateBackend != null) {
			return keyedStateBackend.getCurrentKey();
		} else {
			throw new UnsupportedOperationException("Key can only be retrieven on KeyedStream.");
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

	public class CountingOutput implements Output<StreamRecord<OUT>> {
		private final Output<StreamRecord<OUT>> output;
		private final Counter numRecordsOut;

		public CountingOutput(Output<StreamRecord<OUT>> output, Counter counter) {
			this.output = output;
			this.numRecordsOut = counter;
		}

		@Override
		public void emitWatermark(Watermark mark) {
			output.emitWatermark(mark);
		}

		@Override
		public void collect(StreamRecord<OUT> record) {
			numRecordsOut.inc();
			output.collect(record);
		}

		@Override
		public void close() {
			output.close();
		}
	}

	// ------------------------------------------------------------------------
	//  Watermark handling
	// ------------------------------------------------------------------------

	/**
	 * Returns a {@link InternalTimerService} that can be used to query current processing time
	 * and event time and to set timers. An operator can have several timer services, where
	 * each has its own namespace serializer. Timer services are differentiated by the string
	 * key that is given when requesting them, if you call this method with the same key
	 * multiple times you will get the same timer service instance in subsequent requests.
	 *
	 * <p>Timers are always scoped to a key, the currently active key of a keyed stream operation.
	 * When a timer fires, this key will also be set as the currently active key.
	 *
	 * <p>Each timer has attached metadata, the namespace. Different timer services
	 * can have a different namespace type. If you don't need namespace differentiation you
	 * can use {@link VoidNamespaceSerializer} as the namespace serializer.
	 *
	 * @param name The name of the requested timer service. If no service exists under the given
	 *             name a new one will be created and returned.
	 * @param keySerializer {@code TypeSerializer} for the keys of the timers.
	 * @param namespaceSerializer {@code TypeSerializer} for the timer namespace.
	 * @param triggerable The {@link Triggerable} that should be invoked when timers fire
	 *
	 * @param <K> The type of the timer keys.
	 * @param <N> The type of the timer namespace.
	 */
	public <K, N> InternalTimerService<N> getInternalTimerService(
			String name,
			TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer,
			Triggerable<K, N> triggerable) {

		@SuppressWarnings("unchecked")
		HeapInternalTimerService<K, N> service = (HeapInternalTimerService<K, N>) timerServices.get(name);

		if (service == null) {
			if (restoredServices != null && restoredServices.containsKey(name)) {
				@SuppressWarnings("unchecked")
				HeapInternalTimerService.RestoredTimers<K, N> restoredService =
						(HeapInternalTimerService.RestoredTimers<K, N>) restoredServices.remove(name);

				service = new HeapInternalTimerService<>(
						keySerializer,
						namespaceSerializer,
						triggerable,
						this,
						getRuntimeContext().getProcessingTimeService(),
						restoredService);

			} else {
				service = new HeapInternalTimerService<>(
						keySerializer,
						namespaceSerializer,
						triggerable,
						this,
						getRuntimeContext().getProcessingTimeService());
			}
			timerServices.put(name, service);
		}

		return service;
	}

	public void processWatermark(Watermark mark) throws Exception {
		for (HeapInternalTimerService<?, ?> service : timerServices.values()) {
			service.advanceWatermark(mark.getTimestamp());
		}
		output.emitWatermark(mark);
	}

	public void processWatermark1(Watermark mark) throws Exception {
		input1Watermark = mark.getTimestamp();
		long newMin = Math.min(input1Watermark, input2Watermark);
		if (newMin > combinedWatermark) {
			combinedWatermark = newMin;
			processWatermark(new Watermark(combinedWatermark));
		}
	}

	public void processWatermark2(Watermark mark) throws Exception {
		input2Watermark = mark.getTimestamp();
		long newMin = Math.min(input1Watermark, input2Watermark);
		if (newMin > combinedWatermark) {
			combinedWatermark = newMin;
			processWatermark(new Watermark(combinedWatermark));
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public void snapshotState(FSDataOutputStream out, long checkpointId, long timestamp) throws Exception {
		DataOutputViewStreamWrapper dataOutputView = new DataOutputViewStreamWrapper(out);

		dataOutputView.writeInt(timerServices.size());

		for (Map.Entry<String, HeapInternalTimerService<?, ?>> service : timerServices.entrySet()) {
			dataOutputView.writeUTF(service.getKey());
			service.getValue().snapshotTimers(dataOutputView);
		}

	}

	@Override
	public void restoreState(FSDataInputStream in) throws Exception {
		DataInputViewStreamWrapper dataInputView = new DataInputViewStreamWrapper(in);

		restoredServices = new HashMap<>();

		int numServices = dataInputView.readInt();

		for (int i = 0; i < numServices; i++) {
			String name = dataInputView.readUTF();
			HeapInternalTimerService.RestoredTimers restoredService =
					new HeapInternalTimerService.RestoredTimers(in, getUserCodeClassloader());
			restoredServices.put(name, restoredService);
		}
	}

	@VisibleForTesting
	public int numProcessingTimeTimers() {
		int count = 0;
		for (HeapInternalTimerService<?, ?> timerService : timerServices.values()) {
			count += timerService.numProcessingTimeTimers();
		}
		return count;
	}

	@VisibleForTesting
	public int numEventTimeTimers() {
		int count = 0;
		for (HeapInternalTimerService<?, ?> timerService : timerServices.values()) {
			count += timerService.numEventTimeTimers();
		}
		return count;
	}
}
