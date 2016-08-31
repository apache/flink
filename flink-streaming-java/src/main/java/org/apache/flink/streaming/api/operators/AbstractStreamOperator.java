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
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.metrics.Counter;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.runtime.state.AsynchronousKvStateSnapshot;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.KvStateSnapshot;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.operators.Triggerable;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.InstantiationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.HashMap;
import java.util.Set;
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

	// ---------------- runtime fields ------------------

	/** The task that contains this operator (and other operators in the same chain) */
	private transient StreamTask<?, ?> container;
	
	private transient StreamConfig config;

	protected transient Output<StreamRecord<OUT>> output;

	/** The runtime context for UDFs */
	private transient StreamingRuntimeContext runtimeContext;


	// ---------------- key/value state ------------------

	/** key selector used to get the key for the state. Non-null only is the operator uses key/value state */
	private transient KeySelector<?, ?> stateKeySelector1;
	private transient KeySelector<?, ?> stateKeySelector2;

	/** The state backend that stores the state and checkpoints for this task */
	private transient AbstractStateBackend stateBackend;
	protected MetricGroup metrics;

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

		try {
			TypeSerializer<Object> keySerializer = config.getStateKeySerializer(getUserCodeClassloader());
			// if the keySerializer is null we still need to create the state backend
			// for the non-partitioned state features it provides, such as the state output streams
			String operatorIdentifier = getClass().getSimpleName() + "_" + config.getVertexID() + "_" + runtimeContext.getIndexOfThisSubtask();
			stateBackend = container.createStateBackend(operatorIdentifier, keySerializer);
		} catch (Exception e) {
			throw new RuntimeException("Could not initialize state backend. ", e);
		}
	}
	
	public MetricGroup getMetricGroup() {
		return metrics;
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
				stateBackend.discardState();
			} catch (Exception e) {
				throw new RuntimeException("Error while closing/disposing state backend.", e);
			}
		}
	}
	
	// ------------------------------------------------------------------------
	//  Checkpointing
	// ------------------------------------------------------------------------

	@Override
	public void snapshotState(FSDataOutputStream out,
			long checkpointId,
			long timestamp) throws Exception {

		HashMap<String, KvStateSnapshot<?, ?, ?, ?, ?>> keyedState =
				stateBackend.snapshotPartitionedState(checkpointId,timestamp);

		// Materialize asynchronous snapshots, if any
		if (keyedState != null) {
			Set<String> keys = keyedState.keySet();
			for (String key: keys) {
				if (keyedState.get(key) instanceof AsynchronousKvStateSnapshot) {
					AsynchronousKvStateSnapshot<?, ?, ?, ?, ?> asyncHandle = (AsynchronousKvStateSnapshot<?, ?, ?, ?, ?>) keyedState.get(key);
					keyedState.put(key, asyncHandle.materialize());
				}
			}
		}

		byte[] serializedSnapshot = InstantiationUtil.serializeObject(keyedState);

		DataOutputStream dos = new DataOutputStream(out);
		dos.writeInt(serializedSnapshot.length);
		dos.write(serializedSnapshot);

		dos.flush();

	}

	@Override
	public void restoreState(FSDataInputStream in) throws Exception {
		DataInputStream dis = new DataInputStream(in);
		int size = dis.readInt();
		byte[] serializedSnapshot = new byte[size];
		dis.readFully(serializedSnapshot);

		HashMap<String, KvStateSnapshot> keyedState =
				InstantiationUtil.deserializeObject(serializedSnapshot, getUserCodeClassloader());

		stateBackend.injectKeyValueStateSnapshots(keyedState);
	}
	
	@Override
	public void notifyOfCompletedCheckpoint(long checkpointId) throws Exception {
		if (stateBackend != null) {
			stateBackend.notifyOfCompletedCheckpoint(checkpointId);
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

	public AbstractStateBackend getStateBackend() {
		return stateBackend;
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

	protected long getCurrentProcessingTime() {
		return container.getCurrentProcessingTime();
	}

	/**
	 * Creates a partitioned state handle, using the state backend configured for this task.
	 * 
	 * @throws IllegalStateException Thrown, if the key/value state was already initialized.
	 * @throws Exception Thrown, if the state backend cannot create the key/value state.
	 */
	protected <S extends State> S getPartitionedState(StateDescriptor<S, ?> stateDescriptor) throws Exception {
		return getStateBackend().getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, stateDescriptor);
	}

	/**
	 * Creates a partitioned state handle, using the state backend configured for this task.
	 *
	 * @throws IllegalStateException Thrown, if the key/value state was already initialized.
	 * @throws Exception Thrown, if the state backend cannot create the key/value state.
	 */
	@SuppressWarnings("unchecked")
	protected <S extends State, N> S getPartitionedState(N namespace, TypeSerializer<N> namespaceSerializer, StateDescriptor<S, ?> stateDescriptor) throws Exception {
		if (stateBackend != null) {
			return stateBackend.getPartitionedState(
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
			getStateBackend().setCurrentKey(key);
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
		if (stateBackend != null) {
			try {
				stateBackend.setCurrentKey(key);
			} catch (Exception e) {
				throw new RuntimeException("Exception occurred while setting the current key context.", e);
			}
		} else {
			throw new RuntimeException("Could not set the current key context, because the " +
				"AbstractStateBackend has not been initialized.");
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
}
