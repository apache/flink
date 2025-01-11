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

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.IndexedCombinedWatermarkStatus;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.groups.InternalOperatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.StreamOperatorStateHandler.CheckpointedStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributesBuilder;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.streaming.util.LatencyStats;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Locale;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * New base class for all stream operators, intended to eventually replace {@link
 * AbstractStreamOperator}. Currently intended to work smoothly just with {@link
 * MultipleInputStreamOperator}.
 *
 * <p>One note-able difference in comparison to {@link AbstractStreamOperator} is lack of {@link
 * AbstractStreamOperator#setup(StreamTask, StreamConfig, Output)} in favor of initialisation in the
 * constructor, and removed some tight coupling with classes like {@link StreamTask}.
 *
 * <p>Methods are guaranteed not to be called concurrently.
 *
 * @param <OUT> The output type of the operator
 */
@Experimental
public abstract class AbstractStreamOperatorV2<OUT>
        implements StreamOperator<OUT>, CheckpointedStreamOperator {
    /** The logger used by the operator class and its subclasses. */
    protected static final Logger LOG = LoggerFactory.getLogger(AbstractStreamOperatorV2.class);

    protected final StreamConfig config;
    protected final Output<StreamRecord<OUT>> output;
    private final StreamingRuntimeContext runtimeContext;
    private final MailboxExecutor mailboxExecutor;

    private final ExecutionConfig executionConfig;
    private final ClassLoader userCodeClassLoader;
    private final CloseableRegistry cancelables;
    protected final IndexedCombinedWatermarkStatus combinedWatermark;

    /** Metric group for the operator. */
    protected final InternalOperatorMetricGroup metrics;

    protected final LatencyStats latencyStats;
    protected final ProcessingTimeService processingTimeService;
    protected final RecordAttributes[] lastRecordAttributes;

    protected StreamOperatorStateHandler stateHandler;
    protected InternalTimeServiceManager<?> timeServiceManager;
    private @Nullable MailboxWatermarkProcessor watermarkProcessor;

    public AbstractStreamOperatorV2(StreamOperatorParameters<OUT> parameters, int numberOfInputs) {
        final Environment environment = parameters.getContainingTask().getEnvironment();
        config = parameters.getStreamConfig();
        output = parameters.getOutput();
        metrics =
                environment
                        .getMetricGroup()
                        .getOrAddOperator(config.getOperatorID(), config.getOperatorName());
        latencyStats =
                createLatencyStats(
                        environment.getTaskManagerInfo().getConfiguration(),
                        parameters.getContainingTask().getIndexInSubtaskGroup());
        processingTimeService = Preconditions.checkNotNull(parameters.getProcessingTimeService());
        lastRecordAttributes = new RecordAttributes[numberOfInputs];
        for (int i = 0; i < numberOfInputs; ++i) {
            lastRecordAttributes[i] = RecordAttributes.EMPTY_RECORD_ATTRIBUTES;
        }
        executionConfig = parameters.getContainingTask().getExecutionConfig();
        userCodeClassLoader = parameters.getContainingTask().getUserCodeClassLoader();
        cancelables = parameters.getContainingTask().getCancelables();
        combinedWatermark = IndexedCombinedWatermarkStatus.forInputsCount(numberOfInputs);

        runtimeContext =
                new StreamingRuntimeContext(
                        environment,
                        environment.getAccumulatorRegistry().getUserMap(),
                        metrics,
                        getOperatorID(),
                        processingTimeService,
                        null,
                        environment.getExternalResourceInfoProvider());

        mailboxExecutor = parameters.getMailboxExecutor();
    }

    private LatencyStats createLatencyStats(
            Configuration taskManagerConfig, int indexInSubtaskGroup) {
        try {
            int historySize = taskManagerConfig.get(MetricOptions.LATENCY_HISTORY_SIZE);
            if (historySize <= 0) {
                LOG.warn(
                        "{} has been set to a value equal or below 0: {}. Using default.",
                        MetricOptions.LATENCY_HISTORY_SIZE,
                        historySize);
                historySize = MetricOptions.LATENCY_HISTORY_SIZE.defaultValue();
            }

            final String configuredGranularity =
                    taskManagerConfig.get(MetricOptions.LATENCY_SOURCE_GRANULARITY);
            LatencyStats.Granularity granularity;
            try {
                granularity =
                        LatencyStats.Granularity.valueOf(
                                configuredGranularity.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException iae) {
                granularity = LatencyStats.Granularity.OPERATOR;
                LOG.warn(
                        "Configured value {} option for {} is invalid. Defaulting to {}.",
                        configuredGranularity,
                        MetricOptions.LATENCY_SOURCE_GRANULARITY.key(),
                        granularity);
            }
            MetricGroup taskMetricGroup = this.metrics.getTaskMetricGroup();
            return new LatencyStats(
                    taskMetricGroup.addGroup("latency"),
                    historySize,
                    indexInSubtaskGroup,
                    getOperatorID(),
                    granularity);
        } catch (Exception e) {
            LOG.warn("An error occurred while instantiating latency metrics.", e);
            return new LatencyStats(
                    UnregisteredMetricGroups.createUnregisteredTaskMetricGroup()
                            .addGroup("latency"),
                    1,
                    0,
                    new OperatorID(),
                    LatencyStats.Granularity.SINGLE);
        }
    }

    @Override
    public OperatorMetricGroup getMetricGroup() {
        return metrics;
    }

    @Override
    public void initializeState(StreamTaskStateInitializer streamTaskStateManager)
            throws Exception {
        final TypeSerializer<?> keySerializer =
                config.getStateKeySerializer(getUserCodeClassloader());

        final StreamOperatorStateContext context =
                streamTaskStateManager.streamOperatorStateContext(
                        getOperatorID(),
                        getClass().getSimpleName(),
                        getProcessingTimeService(),
                        this,
                        keySerializer,
                        cancelables,
                        metrics,
                        config.getManagedMemoryFractionOperatorUseCaseOfSlot(
                                ManagedMemoryUseCase.STATE_BACKEND,
                                runtimeContext.getJobConfiguration(),
                                runtimeContext.getTaskManagerRuntimeInfo().getConfiguration(),
                                runtimeContext.getUserCodeClassLoader()),
                        isUsingCustomRawKeyedState(),
                        isAsyncStateProcessingEnabled());

        stateHandler = new StreamOperatorStateHandler(context, getExecutionConfig(), cancelables);
        timeServiceManager =
                isAsyncStateProcessingEnabled()
                        ? context.asyncInternalTimerServiceManager()
                        : context.internalTimerServiceManager();
        stateHandler.initializeOperatorState(this);

        if (useSplittableTimers()
                && areSplittableTimersConfigured()
                && getTimeServiceManager().isPresent()) {
            watermarkProcessor =
                    new MailboxWatermarkProcessor(
                            output, mailboxExecutor, getTimeServiceManager().get());
        }
    }

    /**
     * Can be overridden to disable splittable timers for this particular operator even if config
     * option is enabled. By default, splittable timers are disabled.
     *
     * @return {@code true} if splittable timers should be used (subject to {@link
     *     StreamConfig#isUnalignedCheckpointsEnabled()} and {@link
     *     StreamConfig#isUnalignedCheckpointsSplittableTimersEnabled()}. {@code false} if
     *     splittable timers should never be used.
     */
    @Internal
    public boolean useSplittableTimers() {
        return false;
    }

    @Internal
    private boolean areSplittableTimersConfigured() {
        return AbstractStreamOperator.areSplittableTimersConfigured(config);
    }

    /**
     * Indicates whether or not implementations of this class is writing to the raw keyed state
     * streams on snapshots, using {@link #snapshotState(StateSnapshotContext)}. If yes, subclasses
     * should override this method to return {@code true}.
     *
     * <p>Subclasses need to explicitly indicate the use of raw keyed state because, internally, the
     * {@link AbstractStreamOperator} may attempt to read from it as well to restore heap-based
     * timers and ultimately fail with read errors. By setting this flag to {@code true}, this
     * allows the {@link AbstractStreamOperator} to know that the data written in the raw keyed
     * states were not written by the timer services, and skips the timer restore attempt.
     *
     * <p>Please refer to FLINK-19741 for further details.
     *
     * <p>TODO: this method can be removed once all timers are moved to be managed by state
     * backends.
     *
     * @return flag indicating whether or not this operator is writing to raw keyed state via {@link
     *     #snapshotState(StateSnapshotContext)}.
     */
    @Internal
    protected boolean isUsingCustomRawKeyedState() {
        return false;
    }

    /**
     * Indicates whether this operator is enabling the async state. Can be overridden by subclasses.
     */
    @Internal
    public boolean isAsyncStateProcessingEnabled() {
        return false;
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

    @Override
    public void finish() throws Exception {}

    @Override
    public void close() throws Exception {
        if (stateHandler != null) {
            stateHandler.dispose();
        }
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        // the default implementation does nothing and accepts the checkpoint
        // this is purely for subclasses to override
    }

    @Override
    public OperatorSnapshotFutures snapshotState(
            long checkpointId,
            long timestamp,
            CheckpointOptions checkpointOptions,
            CheckpointStreamFactory factory)
            throws Exception {
        return stateHandler.snapshotState(
                this,
                Optional.ofNullable(timeServiceManager),
                getOperatorName(),
                checkpointId,
                timestamp,
                checkpointOptions,
                factory,
                isUsingCustomRawKeyedState(),
                isAsyncStateProcessingEnabled());
    }

    /**
     * Stream operators with state, which want to participate in a snapshot need to override this
     * hook method.
     *
     * @param context context that provides information and means required for taking a snapshot
     */
    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {}

    /**
     * Stream operators with state which can be restored need to override this hook method.
     *
     * @param context context that allows to register different states.
     */
    @Override
    public void initializeState(StateInitializationContext context) throws Exception {}

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        stateHandler.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        stateHandler.notifyCheckpointAborted(checkpointId);
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
        return executionConfig;
    }

    public StreamConfig getOperatorConfig() {
        return config;
    }

    public ClassLoader getUserCodeClassloader() {
        return userCodeClassLoader;
    }

    /**
     * Return the operator name. If the runtime context has been set, then the task name with
     * subtask index is returned. Otherwise, the simple class name is returned.
     *
     * @return If runtime context is set, then return task name with subtask index. Otherwise return
     *     simple class name.
     */
    protected String getOperatorName() {
        if (runtimeContext != null) {
            return runtimeContext.getTaskInfo().getTaskNameWithSubtasks();
        } else {
            return getClass().getSimpleName();
        }
    }

    /**
     * Returns a context that allows the operator to query information about the execution and also
     * to interact with systems such as broadcast variables and managed state. This also allows to
     * register timers.
     */
    public StreamingRuntimeContext getRuntimeContext() {
        return runtimeContext;
    }

    public <K> KeyedStateBackend<K> getKeyedStateBackend() {
        return stateHandler.getKeyedStateBackend();
    }

    @VisibleForTesting
    public OperatorStateBackend getOperatorStateBackend() {
        return stateHandler.getOperatorStateBackend();
    }

    /**
     * Returns the {@link ProcessingTimeService} responsible for getting the current processing time
     * and registering timers.
     */
    @VisibleForTesting
    public ProcessingTimeService getProcessingTimeService() {
        return processingTimeService;
    }

    /**
     * Creates a partitioned state handle, using the state backend configured for this task.
     *
     * @throws IllegalStateException Thrown, if the key/value state was already initialized.
     * @throws Exception Thrown, if the state backend cannot create the key/value state.
     */
    protected <S extends State> S getPartitionedState(StateDescriptor<S, ?> stateDescriptor)
            throws Exception {
        return getPartitionedState(
                VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, stateDescriptor);
    }

    protected <N, S extends State, T> S getOrCreateKeyedState(
            TypeSerializer<N> namespaceSerializer, StateDescriptor<S, T> stateDescriptor)
            throws Exception {
        return stateHandler.getOrCreateKeyedState(namespaceSerializer, stateDescriptor);
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
            StateDescriptor<S, ?> stateDescriptor)
            throws Exception {
        return stateHandler.getPartitionedState(namespace, namespaceSerializer, stateDescriptor);
    }

    protected <T> void internalSetKeyContextElement(
            StreamRecord<T> record, KeySelector<T, ?> selector) throws Exception {
        if (selector != null) {
            Object key = selector.getKey(record.getValue());
            setCurrentKey(key);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public void setCurrentKey(Object key) {
        stateHandler.setCurrentKey(key);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public Object getCurrentKey() {
        return stateHandler.getCurrentKey();
    }

    public Optional<KeyedStateStore> getKeyedStateStore() {
        if (stateHandler == null) {
            return Optional.empty();
        }
        return stateHandler.getKeyedStateStore();
    }

    protected void reportOrForwardLatencyMarker(LatencyMarker marker) {
        // all operators are tracking latencies
        this.latencyStats.reportLatency(marker);

        // everything except sinks forwards latency markers
        this.output.emitLatencyMarker(marker);
    }

    // ------------------------------------------------------------------------
    //  Watermark handling
    // ------------------------------------------------------------------------

    /**
     * Returns a {@link InternalTimerService} that can be used to query current processing time and
     * event time and to set timers. An operator can have several timer services, where each has its
     * own namespace serializer. Timer services are differentiated by the string key that is given
     * when requesting them, if you call this method with the same key multiple times you will get
     * the same timer service instance in subsequent requests.
     *
     * <p>Timers are always scoped to a key, the currently active key of a keyed stream operation.
     * When a timer fires, this key will also be set as the currently active key.
     *
     * <p>Each timer has attached metadata, the namespace. Different timer services can have a
     * different namespace type. If you don't need namespace differentiation you can use {@link
     * VoidNamespaceSerializer} as the namespace serializer.
     *
     * @param name The name of the requested timer service. If no service exists under the given
     *     name a new one will be created and returned.
     * @param namespaceSerializer {@code TypeSerializer} for the timer namespace.
     * @param triggerable The {@link Triggerable} that should be invoked when timers fire
     * @param <N> The type of the timer namespace.
     */
    public <K, N> InternalTimerService<N> getInternalTimerService(
            String name, TypeSerializer<N> namespaceSerializer, Triggerable<K, N> triggerable) {
        if (timeServiceManager == null) {
            throw new RuntimeException("The timer service has not been initialized.");
        }

        @SuppressWarnings("unchecked")
        InternalTimeServiceManager<K> keyedTimeServiceHandler =
                (InternalTimeServiceManager<K>) timeServiceManager;
        TypeSerializer<K> keySerializer = stateHandler.getKeySerializer();
        checkState(keySerializer != null, "Timers can only be used on keyed operators.");
        return keyedTimeServiceHandler.getInternalTimerService(
                name, keySerializer, namespaceSerializer, triggerable);
    }

    public void processWatermark(Watermark mark) throws Exception {
        if (watermarkProcessor != null) {
            watermarkProcessor.emitWatermarkInsideMailbox(mark);
        } else {
            emitWatermarkDirectly(mark);
        }
    }

    private void emitWatermarkDirectly(Watermark mark) throws Exception {
        if (timeServiceManager != null) {
            timeServiceManager.advanceWatermark(mark);
        }
        output.emitWatermark(mark);
    }

    protected void reportWatermark(Watermark mark, int inputId) throws Exception {
        if (combinedWatermark.updateWatermark(inputId - 1, mark.getTimestamp())) {
            processWatermark(new Watermark(combinedWatermark.getCombinedWatermark()));
        }
    }

    public void processWatermarkStatus(WatermarkStatus watermarkStatus, int inputId)
            throws Exception {
        boolean wasIdle = combinedWatermark.isIdle();
        // inputId is 1-based
        if (combinedWatermark.updateStatus(inputId - 1, watermarkStatus.isIdle())) {
            processWatermark(new Watermark(combinedWatermark.getCombinedWatermark()));
        }
        if (wasIdle != combinedWatermark.isIdle()) {
            output.emitWatermarkStatus(watermarkStatus);
        }
    }

    public void processRecordAttributes(RecordAttributes recordAttributes, int inputId)
            throws Exception {
        lastRecordAttributes[inputId - 1] = recordAttributes;
        output.emitRecordAttributes(
                new RecordAttributesBuilder(Arrays.asList(lastRecordAttributes)).build());
    }

    @Override
    public OperatorID getOperatorID() {
        return config.getOperatorID();
    }

    @Override
    public void setKeyContextElement1(StreamRecord<?> record) throws Exception {
        throw new IllegalStateException(
                "This method should never be called. Use Input class instead");
    }

    @Override
    public void setKeyContextElement2(StreamRecord<?> record) throws Exception {
        throw new IllegalStateException(
                "This method should never be called. Use Input class instead");
    }

    protected Optional<InternalTimeServiceManager<?>> getTimeServiceManager() {
        return Optional.ofNullable(timeServiceManager);
    }
}
