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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskManagerJobMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.DefaultKeyedStateStore;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupStatePartitionStreamProvider;
import org.apache.flink.runtime.state.KeyGroupsList;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.KeyedStateCheckpointOutputStream;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateInitializationContextImpl;
import org.apache.flink.runtime.state.StatePartitionStreamProvider;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.runtime.state.StateSnapshotContextSynchronousImpl;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.util.LatencyStats;
import org.apache.flink.util.CloseableIterable;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.Serializable;
import java.util.Locale;

/**
 * Base class for all stream operators. Operators that contain a user function should extend the class
 * {@link AbstractUdfStreamOperator} instead (which is a specialized subclass of this class).
 *
 * <p>For concrete implementations, one of the following two interfaces must also be implemented, to
 * mark the operator as unary or binary:
 * {@link OneInputStreamOperator} or {@link TwoInputStreamOperator}.
 *
 * <p>Methods of {@code StreamOperator} are guaranteed not to be called concurrently. Also, if using
 * the timer service, timer callbacks are also guaranteed not to be called concurrently with
 * methods on {@code StreamOperator}.
 *
 * @param <OUT> The output type of the operator
 */
@PublicEvolving
public abstract class AbstractStreamOperator<OUT>
		implements StreamOperator<OUT>, Serializable {

	private static final long serialVersionUID = 1L;

	/** The logger used by the operator class and its subclasses. */
	protected static final Logger LOG = LoggerFactory.getLogger(AbstractStreamOperator.class);

	// ----------- configuration properties -------------

	// A sane default for most operators
	protected ChainingStrategy chainingStrategy = ChainingStrategy.HEAD;

	// ---------------- runtime fields ------------------

	/** The task that contains this operator (and other operators in the same chain). */
	private transient StreamTask<?, ?> container;

	protected transient StreamConfig config;

	protected transient Output<StreamRecord<OUT>> output;

	/** The runtime context for UDFs. */
	private transient StreamingRuntimeContext runtimeContext;

	// ---------------- key/value state ------------------

	/**
	 * {@code KeySelector} for extracting a key from an element being processed. This is used to
	 * scope keyed state to a key. This is null if the operator is not a keyed operator.
	 *
	 * <p>This is for elements from the first input.
	 */
	private transient KeySelector<?, ?> stateKeySelector1;

	/**
	 * {@code KeySelector} for extracting a key from an element being processed. This is used to
	 * scope keyed state to a key. This is null if the operator is not a keyed operator.
	 *
	 * <p>This is for elements from the second input.
	 */
	private transient KeySelector<?, ?> stateKeySelector2;

	/** Backend for keyed state. This might be empty if we're not on a keyed stream. */
	private transient AbstractKeyedStateBackend<?> keyedStateBackend;

	/** Keyed state store view on the keyed backend. */
	private transient DefaultKeyedStateStore keyedStateStore;

	// ---------------- operator state ------------------

	/** Operator state backend / store. */
	private transient OperatorStateBackend operatorStateBackend;

	// --------------- Metrics ---------------------------

	/** Metric group for the operator. */
	protected transient OperatorMetricGroup metrics;

	protected transient LatencyStats latencyStats;

	// ---------------- time handler ------------------

	protected transient InternalTimeServiceManager<?> timeServiceManager;

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
		final Environment environment = containingTask.getEnvironment();
		this.container = containingTask;
		this.config = config;
		try {
			OperatorMetricGroup operatorMetricGroup = environment.getMetricGroup().getOrAddOperator(config.getOperatorID(), config.getOperatorName());
			this.output = new CountingOutput(output, operatorMetricGroup.getIOMetricGroup().getNumRecordsOutCounter());
			if (config.isChainStart()) {
				operatorMetricGroup.getIOMetricGroup().reuseInputMetricsForTask();
			}
			if (config.isChainEnd()) {
				operatorMetricGroup.getIOMetricGroup().reuseOutputMetricsForTask();
			}
			this.metrics = operatorMetricGroup;
		} catch (Exception e) {
			LOG.warn("An error occurred while instantiating task metrics.", e);
			this.metrics = UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup();
			this.output = output;
		}

		try {
			Configuration taskManagerConfig = environment.getTaskManagerInfo().getConfiguration();
			int historySize = taskManagerConfig.getInteger(MetricOptions.LATENCY_HISTORY_SIZE);
			if (historySize <= 0) {
				LOG.warn("{} has been set to a value equal or below 0: {}. Using default.", MetricOptions.LATENCY_HISTORY_SIZE, historySize);
				historySize = MetricOptions.LATENCY_HISTORY_SIZE.defaultValue();
			}

			final String configuredGranularity = taskManagerConfig.getString(MetricOptions.LATENCY_SOURCE_GRANULARITY);
			LatencyStats.Granularity granularity;
			try {
				granularity = LatencyStats.Granularity.valueOf(configuredGranularity.toUpperCase(Locale.ROOT));
			} catch (IllegalArgumentException iae) {
				granularity = LatencyStats.Granularity.OPERATOR;
				LOG.warn(
					"Configured value {} option for {} is invalid. Defaulting to {}.",
					configuredGranularity,
					MetricOptions.LATENCY_SOURCE_GRANULARITY.key(),
					granularity);
			}
			TaskManagerJobMetricGroup jobMetricGroup = this.metrics.parent().parent();
			this.latencyStats = new LatencyStats(jobMetricGroup.addGroup("latency"),
				historySize,
				container.getIndexInSubtaskGroup(),
				getOperatorID(),
				granularity);
		} catch (Exception e) {
			LOG.warn("An error occurred while instantiating latency metrics.", e);
			this.latencyStats = new LatencyStats(
				UnregisteredMetricGroups.createUnregisteredTaskManagerJobMetricGroup().addGroup("latency"),
				1,
				0,
				new OperatorID(),
				LatencyStats.Granularity.SINGLE);
		}

		this.runtimeContext = new StreamingRuntimeContext(this, environment, container.getAccumulatorMap());

		stateKeySelector1 = config.getStatePartitioner(0, getUserCodeClassloader());
		stateKeySelector2 = config.getStatePartitioner(1, getUserCodeClassloader());
	}

	@Override
	public MetricGroup getMetricGroup() {
		return metrics;
	}

	@Override
	public final void initializeState() throws Exception {

		final TypeSerializer<?> keySerializer = config.getStateKeySerializer(getUserCodeClassloader());

		final StreamTask<?, ?> containingTask =
			Preconditions.checkNotNull(getContainingTask());
		final CloseableRegistry streamTaskCloseableRegistry =
			Preconditions.checkNotNull(containingTask.getCancelables());
		final StreamTaskStateInitializer streamTaskStateManager =
			Preconditions.checkNotNull(containingTask.createStreamTaskStateInitializer());

		final StreamOperatorStateContext context =
			streamTaskStateManager.streamOperatorStateContext(
				getOperatorID(),
				getClass().getSimpleName(),
				this,
				keySerializer,
				streamTaskCloseableRegistry,
				metrics);

		this.operatorStateBackend = context.operatorStateBackend();
		this.keyedStateBackend = context.keyedStateBackend();

		if (keyedStateBackend != null) {
			this.keyedStateStore = new DefaultKeyedStateStore(keyedStateBackend, getExecutionConfig());
		}

		timeServiceManager = context.internalTimerServiceManager();

		CloseableIterable<KeyGroupStatePartitionStreamProvider> keyedStateInputs = context.rawKeyedStateInputs();
		CloseableIterable<StatePartitionStreamProvider> operatorStateInputs = context.rawOperatorStateInputs();

		try {
			StateInitializationContext initializationContext = new StateInitializationContextImpl(
				context.isRestored(), // information whether we restore or start for the first time
				operatorStateBackend, // access to operator state backend
				keyedStateStore, // access to keyed state backend
				keyedStateInputs, // access to keyed state stream
				operatorStateInputs); // access to operator state stream

			initializeState(initializationContext);
		} finally {
			closeFromRegistry(operatorStateInputs, streamTaskCloseableRegistry);
			closeFromRegistry(keyedStateInputs, streamTaskCloseableRegistry);
		}
	}

	private static void closeFromRegistry(Closeable closeable, CloseableRegistry registry) {
		if (registry.unregisterCloseable(closeable)) {
			IOUtils.closeQuietly(closeable);
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
	public void open() throws Exception {}

	/**
	 * This method is called after all records have been added to the operators via the methods
	 * {@link OneInputStreamOperator#processElement(StreamRecord)}, or
	 * {@link TwoInputStreamOperator#processElement1(StreamRecord)} and
	 * {@link TwoInputStreamOperator#processElement2(StreamRecord)}.
	 *
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
	 * <p>This method is expected to make a thorough effort to release all resources
	 * that the operator has acquired.
	 */
	@Override
	public void dispose() throws Exception {

		Exception exception = null;

		StreamTask<?, ?> containingTask = getContainingTask();
		CloseableRegistry taskCloseableRegistry = containingTask != null ?
			containingTask.getCancelables() :
			null;

		try {
			if (taskCloseableRegistry == null ||
				taskCloseableRegistry.unregisterCloseable(operatorStateBackend)) {
				operatorStateBackend.close();
			}
		} catch (Exception e) {
			exception = e;
		}

		try {
			if (taskCloseableRegistry == null ||
				taskCloseableRegistry.unregisterCloseable(keyedStateBackend)) {
				keyedStateBackend.close();
			}
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		try {
			if (operatorStateBackend != null) {
				operatorStateBackend.dispose();
			}
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		try {
			if (keyedStateBackend != null) {
				keyedStateBackend.dispose();
			}
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		if (exception != null) {
			throw exception;
		}
	}

	@Override
	public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
		// the default implementation does nothing and accepts the checkpoint
		// this is purely for subclasses to override
	}

	@Override
	public final OperatorSnapshotFutures snapshotState(long checkpointId, long timestamp, CheckpointOptions checkpointOptions,
			CheckpointStreamFactory factory) throws Exception {

		KeyGroupRange keyGroupRange = null != keyedStateBackend ?
				keyedStateBackend.getKeyGroupRange() : KeyGroupRange.EMPTY_KEY_GROUP_RANGE;

		OperatorSnapshotFutures snapshotInProgress = new OperatorSnapshotFutures();

		try (StateSnapshotContextSynchronousImpl snapshotContext = new StateSnapshotContextSynchronousImpl(
				checkpointId,
				timestamp,
				factory,
				keyGroupRange,
				getContainingTask().getCancelables())) {

			snapshotState(snapshotContext);

			snapshotInProgress.setKeyedStateRawFuture(snapshotContext.getKeyedStateStreamFuture());
			snapshotInProgress.setOperatorStateRawFuture(snapshotContext.getOperatorStateStreamFuture());

			if (null != operatorStateBackend) {
				snapshotInProgress.setOperatorStateManagedFuture(
					operatorStateBackend.snapshot(checkpointId, timestamp, factory, checkpointOptions));
			}

			if (null != keyedStateBackend) {
				snapshotInProgress.setKeyedStateManagedFuture(
					keyedStateBackend.snapshot(checkpointId, timestamp, factory, checkpointOptions));
			}
		} catch (Exception snapshotException) {
			try {
				snapshotInProgress.cancel();
			} catch (Exception e) {
				snapshotException.addSuppressed(e);
			}

			String snapshotFailMessage = "Could not complete snapshot " + checkpointId + " for operator " +
				getOperatorName() + ".";

			if (!getContainingTask().isCanceled()) {
				LOG.info(snapshotFailMessage, snapshotException);
			}
			throw new Exception(snapshotFailMessage, snapshotException);
		}

		return snapshotInProgress;
	}

	/**
	 * Stream operators with state, which want to participate in a snapshot need to override this hook method.
	 *
	 * @param context context that provides information and means required for taking a snapshot
	 */
	public void snapshotState(StateSnapshotContext context) throws Exception {
		final KeyedStateBackend<?> keyedStateBackend = getKeyedStateBackend();
		//TODO all of this can be removed once heap-based timers are integrated with RocksDB incremental snapshots
		if (keyedStateBackend instanceof AbstractKeyedStateBackend &&
			((AbstractKeyedStateBackend<?>) keyedStateBackend).requiresLegacySynchronousTimerSnapshots()) {

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

					timeServiceManager.snapshotStateForKeyGroup(
						new DataOutputViewStreamWrapper(out), keyGroupIdx);
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

	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		if (keyedStateBackend != null) {
			keyedStateBackend.notifyCheckpointComplete(checkpointId);
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

	protected <N, S extends State, T> S getOrCreateKeyedState(
			TypeSerializer<N> namespaceSerializer,
			StateDescriptor<S, T> stateDescriptor) throws Exception {

		if (keyedStateStore != null) {
			return keyedStateBackend.getOrCreateKeyedState(namespaceSerializer, stateDescriptor);
		}
		else {
			throw new IllegalStateException("Cannot create partitioned state. " +
					"The keyed state backend has not been set." +
					"This indicates that the operator is not partitioned/keyed.");
		}
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

		/*
	    TODO: NOTE: This method does a lot of work caching / retrieving states just to update the namespace.
	    This method should be removed for the sake of namespaces being lazily fetched from the keyed
	    state backend, or being set on the state directly.
	    */

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
		this.latencyStats.reportLatency(marker);

		// everything except sinks forwards latency markers
		this.output.emitLatencyMarker(marker);
	}

	// ----------------------- Helper classes -----------------------

	/**
	 * Wrapping {@link Output} that updates metrics on the number of emitted elements.
	 */
	public static class CountingOutput<OUT> implements Output<StreamRecord<OUT>> {
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
		public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
			numRecordsOut.inc();
			output.collect(outputTag, record);
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
	public <K, N> InternalTimerService<N> getInternalTimerService(
			String name,
			TypeSerializer<N> namespaceSerializer,
			Triggerable<K, N> triggerable) {

		checkTimerServiceInitialization();

		// the following casting is to overcome type restrictions.
		KeyedStateBackend<K> keyedStateBackend = getKeyedStateBackend();
		TypeSerializer<K> keySerializer = keyedStateBackend.getKeySerializer();
		InternalTimeServiceManager<K> keyedTimeServiceHandler = (InternalTimeServiceManager<K>) timeServiceManager;
		TimerSerializer<K, N> timerSerializer = new TimerSerializer<>(keySerializer, namespaceSerializer);
		return keyedTimeServiceHandler.getInternalTimerService(name, timerSerializer, triggerable);
	}

	public void processWatermark(Watermark mark) throws Exception {
		if (timeServiceManager != null) {
			timeServiceManager.advanceWatermark(mark);
		}
		output.emitWatermark(mark);
	}

	private void checkTimerServiceInitialization() {
		if (getKeyedStateBackend() == null) {
			throw new UnsupportedOperationException("Timers can only be used on keyed operators.");
		} else if (timeServiceManager == null) {
			throw new RuntimeException("The timer service has not been initialized.");
		}
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
	public OperatorID getOperatorID() {
		return config.getOperatorID();
	}

	@VisibleForTesting
	public int numProcessingTimeTimers() {
		return timeServiceManager == null ? 0 :
			timeServiceManager.numProcessingTimeTimers();
	}

	@VisibleForTesting
	public int numEventTimeTimers() {
		return timeServiceManager == null ? 0 :
			timeServiceManager.numEventTimeTimers();
	}
}
