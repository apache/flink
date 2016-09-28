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
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.InstantiationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.concurrent.RunnableFuture;

import static java.util.Objects.requireNonNull;

/**
 * Base class for all stream operators.
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
@Internal
public abstract class AbstractStreamOperator<OUT>
		implements StreamOperator<OUT>, StreamCheckpointedOperator, OutputTypeConfigurable<OUT>, Serializable {

	private static final long serialVersionUID = 1L;
	
	/** The logger used by the operator class and its subclasses */
	protected static final Logger LOG = LoggerFactory.getLogger(AbstractStreamOperator.class);

	// ----------- configuration properties -------------

	// A sane default for most operators
	protected ChainingStrategy chainingStrategy = ChainingStrategy.HEAD;

	// ---------------- runtime fields ------------------

	/** The task that contains this operator (and other operators in the same chain) */
	protected transient StreamTask<?, ?> container;
	
	protected transient StreamConfig config;

	protected transient Output<StreamRecord<OUT>> output;

	/** The runtime context for UDFs */
	private transient StreamingRuntimeContext runtimeContext;


	// ---------------- operator state ------------------

	/** Operator state backend */
	private transient OperatorStateBackend operatorStateBackend;

	private transient Collection<OperatorStateHandle> lazyRestoreStateHandles;

	protected transient MetricGroup metrics;

	// ---------------- user function ------------------

	/** the user function */
	protected final Function userFunction;

	/** Flag to prevent duplicate function.close() calls in close() and dispose() */
	private transient boolean functionsClosed = false;

	public AbstractStreamOperator() {
		this.userFunction = null;
	}

	public AbstractStreamOperator(Function userFunction) {
		this.userFunction = requireNonNull(userFunction);
	}

	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		in.defaultReadObject();
		functionsClosed = false;
	}

	public Function getUserFunction() {
		return userFunction;
	}

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

		FunctionUtils.setFunctionRuntimeContext(userFunction, getRuntimeContext());
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

		FunctionUtils.openFunction(userFunction, new Configuration());
	}

	private final void initOperatorState() {
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
	public void close() throws Exception {
		functionsClosed = true;
		FunctionUtils.closeFunction(userFunction);
	}
	
	/**
	 * This method is called at the very end of the operator's life, both in the case of a successful
	 * completion of the operation, and in the case of a failure and canceling.
	 *
	 * This method is expected to make a thorough effort to release all resources
	 * that the operator has acquired.
	 */
	@Override
	public void dispose() throws Exception {
		if (!functionsClosed) {
			functionsClosed = true;
			FunctionUtils.closeFunction(userFunction);
		}
		if (operatorStateBackend != null) {
			IOUtils.closeQuietly(operatorStateBackend);
			operatorStateBackend.dispose();
		}
	}

	// ------------------------------------------------------------------------
	//  checkpointing and recovery
	// ------------------------------------------------------------------------

	@Override
	public RunnableFuture<OperatorStateHandle> snapshotState(
			long checkpointId, long timestamp, CheckpointStreamFactory streamFactory) throws Exception {

		return operatorStateBackend != null ?
				operatorStateBackend.snapshot(checkpointId, timestamp, streamFactory) : null;
	}

	@Override
	public void snapshotState(FSDataOutputStream out, long checkpointId, long timestamp) throws Exception {

		if (userFunction instanceof Checkpointed) {
			@SuppressWarnings("unchecked")
			Checkpointed<Serializable> chkFunction = (Checkpointed<Serializable>) userFunction;

			Serializable udfState;
			try {
				udfState = chkFunction.snapshotState(checkpointId, timestamp);
				if (udfState != null) {
					out.write(1);
					InstantiationUtil.serializeObject(out, udfState);
				} else {
					out.write(0);
				}
			} catch (Exception e) {
				throw new Exception("Failed to draw state snapshot from function: " + e.getMessage(), e);
			}
		}
	}

	@Override
	public void restoreState(FSDataInputStream in) throws Exception {
		if (userFunction instanceof Checkpointed) {
			@SuppressWarnings("unchecked")
			Checkpointed<Serializable> chkFunction = (Checkpointed<Serializable>) userFunction;

			int hasUdfState = in.read();

			if (hasUdfState == 1) {
				Serializable functionState = InstantiationUtil.deserializeObject(in, getUserCodeClassloader());
				if (functionState != null) {
					try {
						chkFunction.restoreState(functionState);
					} catch (Exception e) {
						throw new Exception("Failed to restore state to function: " + e.getMessage(), e);
					}
				}
			}
		}
	}

	@Override
	public void notifyOfCompletedCheckpoint(long checkpointId) throws Exception {
		if (userFunction instanceof CheckpointListener) {
			((CheckpointListener) userFunction).notifyCheckpointComplete(checkpointId);
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
	
	protected ClassLoader getUserCodeClassloader() {
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

	protected OperatorStateBackend getOperatorStateBackend() {

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
	 * Returns the {@link MetricGroup} of this operator.
	 */
	public MetricGroup getMetricGroup() {
		return metrics;
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

	private class CountingOutput implements Output<StreamRecord<OUT>> {
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
	//  Keyed State
	// ------------------------------------------------------------------------

	/**
	 * Creates a partitioned state handle, using the state backend configured for this task.
	 *
	 * @throws IllegalStateException Thrown, if the key/value state was already initialized.
	 * @throws Exception Thrown, if the state backend cannot create the key/value state.
	 */
	protected <S extends State> S getPartitionedState(StateDescriptor<S, ?> stateDescriptor) throws Exception {
		throw new UnsupportedOperationException("Keyed state is only supported on a KeyedStream.");
	}

	/**
	 * Creates a partitioned state handle, using the state backend configured for this task.
	 *
	 * @throws IllegalStateException Thrown, if the key/value state was already initialized.
	 * @throws Exception Thrown, if the state backend cannot create the key/value state.
	 */
	protected <S extends State, N> S getPartitionedState(
			N namespace,
			TypeSerializer<N> namespaceSerializer,
			StateDescriptor<S, ?> stateDescriptor) throws Exception {
		throw new UnsupportedOperationException("Keyed state is only supported on a KeyedStream.");
	}

	// ------------------------------------------------------------------------
	//  Output type configuration
	// ------------------------------------------------------------------------

	@Override
	public void setOutputType(TypeInformation<OUT> outTypeInfo, ExecutionConfig executionConfig) {
		if (userFunction instanceof OutputTypeConfigurable) {
			@SuppressWarnings("unchecked")
			OutputTypeConfigurable<OUT> outputTypeConfigurable = (OutputTypeConfigurable<OUT>) userFunction;
			outputTypeConfigurable.setOutputType(outTypeInfo, executionConfig);
		}
	}
}
