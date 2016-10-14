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
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
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
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.TimeServiceProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.ConcurrentModificationException;
import java.util.Collection;
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

	/** Backend for keyed state. This might be empty if we're not on a keyed stream. */
	private transient AbstractKeyedStateBackend<?> keyedStateBackend;

	/** Operator state backend */
	private transient OperatorStateBackend operatorStateBackend;

	private transient Collection<OperatorStateHandle> lazyRestoreStateHandles;


	// --------------- Metrics ---------------------------

	/** Metric group for the operator */
	protected MetricGroup metrics;

	protected LatencyGauge latencyGauge;

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
		Configuration taskManagerConfig = container.getEnvironment().getTaskManagerInfo().getConfiguration();
		int historySize = taskManagerConfig.getInteger(ConfigConstants.METRICS_LATENCY_HISTORY_SIZE, ConfigConstants.DEFAULT_METRICS_LATENCY_HISTORY_SIZE);
		if (historySize <= 0) {
			LOG.warn("{} has been set to a value equal or below 0: {}. Using default.", ConfigConstants.METRICS_LATENCY_HISTORY_SIZE, historySize);
			historySize = ConfigConstants.DEFAULT_METRICS_LATENCY_HISTORY_SIZE;
		}

		latencyGauge = this.metrics.gauge("latency", new LatencyGauge(historySize));
		this.runtimeContext = new StreamingRuntimeContext(this, container.getEnvironment(), container.getAccumulatorMap());

		stateKeySelector1 = config.getStatePartitioner(0, getUserCodeClassloader());
		stateKeySelector2 = config.getStatePartitioner(1, getUserCodeClassloader());
	}
	
	@Override
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
	 * Returns the {@link TimeServiceProvider} responsible for getting  the current
	 * processing time and registering timers.
	 */
	protected TimeServiceProvider getTimerService() {
		return container.getTimerService();
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
			setKeyContext(key);
		}
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	public void setKeyContext(Object key) {
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


	// ------------------------------------------------------------------------
	//  Metrics
	// ------------------------------------------------------------------------

	// ------- One input stream
	public void processLatencyMarker(LatencyMarker latencyMarker) throws Exception {
		reportOrForwardLatencyMarker(latencyMarker);
	}

	// ------- Two input stream
	public void processLatencyMarker1(LatencyMarker latencyMarker) throws Exception {
		reportOrForwardLatencyMarker(latencyMarker);
	}

	public void processLatencyMarker2(LatencyMarker latencyMarker) throws Exception {
		reportOrForwardLatencyMarker(latencyMarker);
	}


	protected void reportOrForwardLatencyMarker(LatencyMarker maker) {
		// all operators are tracking latencies
		this.latencyGauge.reportLatency(maker, false);

		// everything except sinks forwards latency markers
		this.output.emitLatencyMarker(maker);
	}

	// ----------------------- Helper classes -----------------------


	/**
	 * The gauge uses a HashMap internally to avoid classloading issues when accessing
	 * the values using JMX.
	 */
	protected static class LatencyGauge implements Gauge<Map<String, HashMap<String, Double>>> {
		private final Map<LatencySourceDescriptor, DescriptiveStatistics> latencyStats = new HashMap<>();
		private final int historySize;

		LatencyGauge(int historySize) {
			this.historySize = historySize;
		}

		public void reportLatency(LatencyMarker marker, boolean isSink) {
			LatencySourceDescriptor sourceDescriptor = LatencySourceDescriptor.of(marker, !isSink);
			DescriptiveStatistics sourceStats = latencyStats.get(sourceDescriptor);
			if (sourceStats == null) {
				// 512 element window (4 kb)
				sourceStats = new DescriptiveStatistics(this.historySize);
				latencyStats.put(sourceDescriptor, sourceStats);
			}
			long now = System.currentTimeMillis();
			sourceStats.addValue(now - marker.getMarkedTime());
		}

		@Override
		public Map<String, HashMap<String, Double>> getValue() {
			while (true) {
				try {
					Map<String, HashMap<String, Double>> ret = new HashMap<>();
					for (Map.Entry<LatencySourceDescriptor, DescriptiveStatistics> source : latencyStats.entrySet()) {
						HashMap<String, Double> sourceStatistics = new HashMap<>(6);
						sourceStatistics.put("max", source.getValue().getMax());
						sourceStatistics.put("mean", source.getValue().getMean());
						sourceStatistics.put("min", source.getValue().getMin());
						sourceStatistics.put("p50", source.getValue().getPercentile(50));
						sourceStatistics.put("p95", source.getValue().getPercentile(95));
						sourceStatistics.put("p99", source.getValue().getPercentile(99));
						ret.put(source.getKey().toString(), sourceStatistics);
					}
					return ret;
					// Concurrent access onto the "latencyStats" map could cause
					// ConcurrentModificationExceptions. To avoid unnecessary blocking
					// of the reportLatency() method, we retry this operation until
					// it succeeds.
				} catch(ConcurrentModificationException ignore) {
					LOG.debug("Unable to report latency statistics", ignore);
				}
			}
		}
	}

	/**
	 * Identifier for a latency source
	 */
	private static class LatencySourceDescriptor {
		/**
		 * A unique ID identifying a logical source in Flink
		 */
		private final int vertexID;

		/**
		 * Identifier for parallel subtasks of a logical source
		 */
		private final int subtaskIndex;

		/**
		 *
		 * @param marker The latency marker to extract the LatencySourceDescriptor from.
		 * @param ignoreSubtaskIndex Set to true to ignore the subtask index, to treat the latencies from all the parallel instances of a source as the same.
		 * @return A LatencySourceDescriptor for the given marker.
		 */
		public static LatencySourceDescriptor of(LatencyMarker marker, boolean ignoreSubtaskIndex) {
			if (ignoreSubtaskIndex) {
				return new LatencySourceDescriptor(marker.getVertexID(), -1);
			} else {
				return new LatencySourceDescriptor(marker.getVertexID(), marker.getSubtaskIndex());
			}

		}

		private LatencySourceDescriptor(int vertexID, int subtaskIndex) {
			this.vertexID = vertexID;
			this.subtaskIndex = subtaskIndex;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			LatencySourceDescriptor that = (LatencySourceDescriptor) o;

			if (vertexID != that.vertexID) {
				return false;
			}
			return subtaskIndex == that.subtaskIndex;
		}

		@Override
		public int hashCode() {
			int result = vertexID;
			result = 31 * result + subtaskIndex;
			return result;
		}

		@Override
		public String toString() {
			return "LatencySourceDescriptor{" +
					"vertexID=" + vertexID +
					", subtaskIndex=" + subtaskIndex +
					'}';
		}
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
		public void emitLatencyMarker(LatencyMarker latencyMarker) {
			output.emitLatencyMarker(latencyMarker);
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
