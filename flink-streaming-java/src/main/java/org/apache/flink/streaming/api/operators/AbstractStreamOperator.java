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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.DefaultKeyedStateStore;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyGroupStatePartitionStreamProvider;
import org.apache.flink.runtime.state.KeyGroupsList;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.KeyedStateCheckpointOutputStream;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateInitializationContextImpl;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.runtime.state.StateSnapshotContextSynchronousImpl;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OperatorStateHandles;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;

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
		implements StreamOperator<OUT>, java.io.Serializable, KeyContext {

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

	/** Keyed state store view on the keyed backend */
	private transient DefaultKeyedStateStore keyedStateStore;

	/** Operator state backend / store */
	private transient OperatorStateBackend operatorStateBackend;


	// --------------- Metrics ---------------------------

	/** Metric group for the operator */
	protected MetricGroup metrics;

	protected LatencyGauge latencyGauge;

	// ---------------- timers ------------------

	private transient Map<String, HeapInternalTimerService<?, ?>> timerServices;


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
		
		this.metrics = container.getEnvironment().getMetricGroup().addOperator(config.getOperatorName());
		this.output = new CountingOutput(output, ((OperatorMetricGroup) this.metrics).getIOMetricGroup().getNumRecordsOutCounter());
		if (config.isChainStart()) {
			((OperatorMetricGroup) this.metrics).getIOMetricGroup().reuseInputMetricsForTask();
		}
		if (config.isChainEnd()) {
			((OperatorMetricGroup) this.metrics).getIOMetricGroup().reuseOutputMetricsForTask();
		}
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
	public final void initializeState(OperatorStateHandles stateHandles) throws Exception {

		Collection<KeyGroupsStateHandle> keyedStateHandlesRaw = null;
		Collection<OperatorStateHandle> operatorStateHandlesRaw = null;
		Collection<OperatorStateHandle> operatorStateHandlesBackend = null;

		boolean restoring = null != stateHandles;

		initKeyedState(); //TODO we should move the actual initialization of this from StreamTask to this class

		if (restoring) {

			restoreStreamCheckpointed(stateHandles);

			//pass directly
			operatorStateHandlesBackend = stateHandles.getManagedOperatorState();
			operatorStateHandlesRaw = stateHandles.getRawOperatorState();

			if (null != getKeyedStateBackend()) {
				//only use the keyed state if it is meant for us (aka head operator)
				keyedStateHandlesRaw = stateHandles.getRawKeyedState();
			}
		}

		initOperatorState(operatorStateHandlesBackend);

		StateInitializationContext initializationContext = new StateInitializationContextImpl(
				restoring, // information whether we restore or start for the first time
				operatorStateBackend, // access to operator state backend
				keyedStateStore, // access to keyed state backend
				keyedStateHandlesRaw, // access to keyed state stream
				operatorStateHandlesRaw, // access to operator state stream
				getContainingTask().getCancelables()); // access to register streams for canceling

		initializeState(initializationContext);
	}

	@Deprecated
	private void restoreStreamCheckpointed(OperatorStateHandles stateHandles) throws Exception {
		StreamStateHandle state = stateHandles.getLegacyOperatorState();
		if (null != state) {
			if (this instanceof CheckpointedRestoringOperator) {

				LOG.debug("Restore state of task {} in chain ({}).",
						stateHandles.getOperatorChainIndex(), getContainingTask().getName());

				FSDataInputStream is = state.openInputStream();
				try {
					getContainingTask().getCancelables().registerClosable(is);
					((CheckpointedRestoringOperator) this).restoreState(is);
				} finally {
					getContainingTask().getCancelables().unregisterClosable(is);
					is.close();
				}
			} else {
				throw new Exception(
						"Found legacy operator state for operator that does not implement StreamCheckpointedOperator.");
			}
		}
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
						container.getEnvironment().getTaskInfo().getIndexOfThisSubtask());

				this.keyedStateBackend = container.createKeyedStateBackend(
						keySerializer,
						container.getEnvironment().getTaskInfo().getNumberOfKeyGroups(),
						subTaskKeyGroupRange);

				this.keyedStateStore = new DefaultKeyedStateStore(keyedStateBackend, getExecutionConfig());
			}

		} catch (Exception e) {
			throw new IllegalStateException("Could not initialize keyed state backend.", e);
		}
	}

	private void initOperatorState(Collection<OperatorStateHandle> operatorStateHandles) {
		try {
			// create an operator state backend
			this.operatorStateBackend = container.createOperatorStateBackend(this, operatorStateHandles);
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
	public final OperatorSnapshotResult snapshotState(
			long checkpointId, long timestamp, CheckpointStreamFactory streamFactory) throws Exception {

		KeyGroupRange keyGroupRange = null != keyedStateBackend ?
				keyedStateBackend.getKeyGroupRange() : KeyGroupRange.EMPTY_KEY_GROUP_RANGE;

		OperatorSnapshotResult snapshotInProgress = new OperatorSnapshotResult();

		try (StateSnapshotContextSynchronousImpl snapshotContext = new StateSnapshotContextSynchronousImpl(
				checkpointId,
				timestamp,
				streamFactory,
				keyGroupRange,
				getContainingTask().getCancelables())) {

			snapshotState(snapshotContext);

			snapshotInProgress.setKeyedStateRawFuture(snapshotContext.getKeyedStateStreamFuture());
			snapshotInProgress.setOperatorStateRawFuture(snapshotContext.getOperatorStateStreamFuture());

			if (null != operatorStateBackend) {
				snapshotInProgress.setOperatorStateManagedFuture(
					operatorStateBackend.snapshot(checkpointId, timestamp, streamFactory));
			}

			if (null != keyedStateBackend) {
				snapshotInProgress.setKeyedStateManagedFuture(
					keyedStateBackend.snapshot(checkpointId, timestamp, streamFactory));
			}
		}  catch (Exception snapshotException) {
			try {
				snapshotInProgress.cancel();
			} catch (Exception e) {
				snapshotException.addSuppressed(e);
			}

			throw new Exception("Could not complete snapshot " + checkpointId + " for operator " +
				getOperatorName() + '.', snapshotException);
		}

		return snapshotInProgress;
	}

	/**
	 * Stream operators with state, which want to participate in a snapshot need to override this hook method.
	 *
	 * @param context context that provides information and means required for taking a snapshot
	 */
	public void snapshotState(StateSnapshotContext context) throws Exception {
		if (getKeyedStateBackend() != null) {
			KeyedStateCheckpointOutputStream out;

			try {
				out = context.getRawKeyedOperatorStateOutput();
			} catch (Exception exception) {
				throw new Exception("Could not open raw keyed operator state stream for " +
					getOperatorName() + '.', exception);
			}

			try {
				KeyGroupsList allKeyGroups = out.getKeyGroupList();
				for (int keyGroupIdx : allKeyGroups) {
					out.startNewKeyGroup(keyGroupIdx);

					DataOutputViewStreamWrapper dov = new DataOutputViewStreamWrapper(out);
					dov.writeInt(timerServices.size());

					for (Map.Entry<String, HeapInternalTimerService<?, ?>> entry : timerServices.entrySet()) {
						String serviceName = entry.getKey();
						HeapInternalTimerService<?, ?> timerService = entry.getValue();

						dov.writeUTF(serviceName);
						timerService.snapshotTimersForKeyGroup(dov, keyGroupIdx);
					}
				}
			} catch (Exception exception) {
				throw new Exception("Could not write timer service of " + getOperatorName() +
					" to checkpoint state stream.", exception);
			} finally {
				try {
					out.close();
				} catch (Exception closeException) {
					LOG.warn("Could not close raw keyed operator state stream for {}. This " +
						"might have prevented deleting some state data.", getOperatorName(), closeException);
				}
			}
		}
	}

	/**
	 * Stream operators with state which can be restored need to override this hook method.
	 *
	 * @param context context that allows to register different states.
	 */
	public void initializeState(StateInitializationContext context) throws Exception {
		if (getKeyedStateBackend() != null) {
			int totalKeyGroups = getKeyedStateBackend().getNumberOfKeyGroups();
			KeyGroupsList localKeyGroupRange = getKeyedStateBackend().getKeyGroupRange();

			// initialize the map with the timer services
			this.timerServices = new HashMap<>();

			// and then initialize the timer services
			for (KeyGroupStatePartitionStreamProvider streamProvider : context.getRawKeyedStateInputs()) {
				DataInputViewStreamWrapper div = new DataInputViewStreamWrapper(streamProvider.getStream());

				int keyGroupIdx = streamProvider.getKeyGroupId();
				checkArgument(localKeyGroupRange.contains(keyGroupIdx),
					"Key Group " + keyGroupIdx + " does not belong to the local range.");

				int noOfTimerServices = div.readInt();
				for (int i = 0; i < noOfTimerServices; i++) {
					String serviceName = div.readUTF();

					HeapInternalTimerService<?, ?> timerService = this.timerServices.get(serviceName);
					if (timerService == null) {
						timerService = new HeapInternalTimerService<>(
							totalKeyGroups,
							localKeyGroupRange,
							this,
							getRuntimeContext().getProcessingTimeService());
						this.timerServices.put(serviceName, timerService);
					}
					timerService.restoreTimersForKeyGroup(div, keyGroupIdx, getUserCodeClassloader());
				}
			}
		}
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
	 * Return the operator name. If the runtime context has been set, then the task name with
	 * subtask index is returned. Otherwise, the simple class name is returned.
	 *
	 * @return If runtime context is set, then return task name with subtask index. Otherwise return
	 * 			simple class name.
	 */
	protected String getOperatorName() {
		if (runtimeContext != null) {
			return runtimeContext.getTaskNameWithSubtasks();
		} else {
			return getClass().getSimpleName();
		}
	}
	
	/**
	 * Returns a context that allows the operator to query information about the execution and also
	 * to interact with systems such as broadcast variables and managed state. This also allows
	 * to register timers.
	 */
	public StreamingRuntimeContext getRuntimeContext() {
		return runtimeContext;
	}

	@SuppressWarnings("unchecked")
	public <K> KeyedStateBackend<K> getKeyedStateBackend() {
		return (KeyedStateBackend<K>) keyedStateBackend;
	}

	public OperatorStateBackend getOperatorStateBackend() {
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
	protected <S extends State, N> S getPartitionedState(
			N namespace, TypeSerializer<N> namespaceSerializer,
			StateDescriptor<S, ?> stateDescriptor) throws Exception {

		if (keyedStateStore != null) {
			return keyedStateBackend.getPartitionedState(namespace, namespaceSerializer, stateDescriptor);
		} else {
			throw new RuntimeException("Cannot create partitioned state. The keyed state " +
				"backend has not been set. This indicates that the operator is not " +
				"partitioned/keyed.");
		}
	}

	@Override
	@SuppressWarnings({"unchecked", "rawtypes"})
	public void setKeyContextElement1(StreamRecord record) throws Exception {
		setKeyContextElement(record, stateKeySelector1);
	}

	@Override
	@SuppressWarnings({"unchecked", "rawtypes"})
	public void setKeyContextElement2(StreamRecord record) throws Exception {
		setKeyContextElement(record, stateKeySelector2);
	}

	private <T> void setKeyContextElement(StreamRecord<T> record, KeySelector<T, ?> selector) throws Exception {
		if (selector != null) {
			Object key = selector.getKey(record.getValue());
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

	public KeyedStateStore getKeyedStateStore() {
		return keyedStateStore;
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


	protected void reportOrForwardLatencyMarker(LatencyMarker marker) {
		// all operators are tracking latencies
		this.latencyGauge.reportLatency(marker, false);

		// everything except sinks forwards latency markers
		this.output.emitLatencyMarker(marker);
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
	 * @param namespaceSerializer {@code TypeSerializer} for the timer namespace.
	 * @param triggerable The {@link Triggerable} that should be invoked when timers fire
	 *
	 * @param <N> The type of the timer namespace.
	 */
	public <N> InternalTimerService<N> getInternalTimerService(
			String name,
			TypeSerializer<N> namespaceSerializer,
			Triggerable<?, N> triggerable) {
		if (getKeyedStateBackend() == null) {
			throw new UnsupportedOperationException("Timers can only be used on keyed operators.");
		}

		@SuppressWarnings("unchecked")
		HeapInternalTimerService<Object, N> timerService = (HeapInternalTimerService<Object, N>) timerServices.get(name);

		if (timerService == null) {
			timerService = new HeapInternalTimerService<>(
				getKeyedStateBackend().getNumberOfKeyGroups(),
				getKeyedStateBackend().getKeyGroupRange(),
				this,
				getRuntimeContext().getProcessingTimeService());
			timerServices.put(name, timerService);
		}
		@SuppressWarnings({"unchecked", "rawtypes"})
		Triggerable rawTriggerable = (Triggerable) triggerable;
		timerService.startTimerService(getKeyedStateBackend().getKeySerializer(), namespaceSerializer, rawTriggerable);
		return timerService;
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
