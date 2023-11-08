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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.api.writer.RecordWriterDelegate;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.InternalOperatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.operators.coordination.AcknowledgeCheckpointEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventDispatcher;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.streaming.api.graph.NonChainedOutput;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.CountingOutput;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.SourceOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactoryUtil;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializer;
import org.apache.flink.streaming.runtime.io.RecordWriterOutput;
import org.apache.flink.streaming.runtime.io.StreamTaskSourceInput;
import org.apache.flink.streaming.runtime.operators.sink.SinkWriterOperatorFactory;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxExecutorFactory;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.SerializedValue;

import org.apache.flink.shaded.guava31.com.google.common.io.Closer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The {@code OperatorChain} contains all operators that are executed as one chain within a single
 * {@link StreamTask}.
 *
 * <p>The main entry point to the chain is it's {@code mainOperator}. {@code mainOperator} is
 * driving the execution of the {@link StreamTask}, by pulling the records from network inputs
 * and/or source inputs and pushing produced records to the remaining chained operators.
 *
 * @param <OUT> The type of elements accepted by the chain, i.e., the input type of the chain's main
 *     operator.
 */
public abstract class OperatorChain<OUT, OP extends StreamOperator<OUT>>
        implements BoundedMultiInput, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(OperatorChain.class);

    protected final RecordWriterOutput<?>[] streamOutputs;

    protected final WatermarkGaugeExposingOutput<StreamRecord<OUT>> mainOperatorOutput;

    /**
     * For iteration, {@link StreamIterationHead} and {@link StreamIterationTail} used for executing
     * feedback edges do not contain any operators, in which case, {@code mainOperatorWrapper} and
     * {@code tailOperatorWrapper} are null.
     *
     * <p>Usually first operator in the chain is the same as {@link #mainOperatorWrapper}, but
     * that's not the case if there are chained source inputs. In this case, one of the source
     * inputs will be the first operator. For example the following operator chain is possible:
     *
     * <pre>
     * first
     *      \
     *      main (multi-input) -> ... -> tail
     *      /
     * second
     * </pre>
     *
     * <p>Where "first" and "second" (there can be more) are chained source operators. When it comes
     * to things like closing, stat initialisation or state snapshotting, the operator chain is
     * traversed: first, second, main, ..., tail or in reversed order: tail, ..., main, second,
     * first
     */
    @Nullable protected final StreamOperatorWrapper<OUT, OP> mainOperatorWrapper;

    @Nullable protected final StreamOperatorWrapper<?, ?> firstOperatorWrapper;
    @Nullable protected final StreamOperatorWrapper<?, ?> tailOperatorWrapper;

    protected final Map<StreamConfig.SourceInputConfig, ChainedSource> chainedSources;

    protected final int numOperators;

    protected final OperatorEventDispatcherImpl operatorEventDispatcher;

    protected final Closer closer = Closer.create();

    protected final @Nullable FinishedOnRestoreInput finishedOnRestoreInput;

    protected boolean isClosed;

    public OperatorChain(
            StreamTask<OUT, OP> containingTask,
            RecordWriterDelegate<SerializationDelegate<StreamRecord<OUT>>> recordWriterDelegate) {

        this.operatorEventDispatcher =
                new OperatorEventDispatcherImpl(
                        containingTask.getEnvironment().getUserCodeClassLoader().asClassLoader(),
                        containingTask.getEnvironment().getOperatorCoordinatorEventGateway());

        final ClassLoader userCodeClassloader = containingTask.getUserCodeClassLoader();
        final StreamConfig configuration = containingTask.getConfiguration();

        StreamOperatorFactory<OUT> operatorFactory =
                configuration.getStreamOperatorFactory(userCodeClassloader);

        // we read the chained configs, and the order of record writer registrations by output name
        Map<Integer, StreamConfig> chainedConfigs =
                configuration.getTransitiveChainedTaskConfigsWithSelf(userCodeClassloader);

        // create the final output stream writers
        // we iterate through all the out edges from this job vertex and create a stream output
        List<NonChainedOutput> outputsInOrder =
                configuration.getVertexNonChainedOutputs(userCodeClassloader);
        Map<IntermediateDataSetID, RecordWriterOutput<?>> recordWriterOutputs =
                CollectionUtil.newHashMapWithExpectedSize(outputsInOrder.size());
        this.streamOutputs = new RecordWriterOutput<?>[outputsInOrder.size()];
        this.finishedOnRestoreInput =
                this.isTaskDeployedAsFinished()
                        ? new FinishedOnRestoreInput(
                                streamOutputs, configuration.getInputs(userCodeClassloader).length)
                        : null;

        // from here on, we need to make sure that the output writers are shut down again on failure
        boolean success = false;
        try {
            createChainOutputs(
                    outputsInOrder,
                    recordWriterDelegate,
                    chainedConfigs,
                    containingTask,
                    recordWriterOutputs);

            // we create the chain of operators and grab the collector that leads into the chain
            List<StreamOperatorWrapper<?, ?>> allOpWrappers =
                    new ArrayList<>(chainedConfigs.size());
            this.mainOperatorOutput =
                    createOutputCollector(
                            containingTask,
                            configuration,
                            chainedConfigs,
                            userCodeClassloader,
                            recordWriterOutputs,
                            allOpWrappers,
                            containingTask.getMailboxExecutorFactory(),
                            operatorFactory != null);

            if (operatorFactory != null) {
                Tuple2<OP, Optional<ProcessingTimeService>> mainOperatorAndTimeService =
                        StreamOperatorFactoryUtil.createOperator(
                                operatorFactory,
                                containingTask,
                                configuration,
                                mainOperatorOutput,
                                operatorEventDispatcher);

                OP mainOperator = mainOperatorAndTimeService.f0;
                mainOperator
                        .getMetricGroup()
                        .gauge(
                                MetricNames.IO_CURRENT_OUTPUT_WATERMARK,
                                mainOperatorOutput.getWatermarkGauge());
                this.mainOperatorWrapper =
                        createOperatorWrapper(
                                mainOperator,
                                containingTask,
                                configuration,
                                mainOperatorAndTimeService.f1,
                                true);

                // add main operator to end of chain
                allOpWrappers.add(mainOperatorWrapper);

                this.tailOperatorWrapper = allOpWrappers.get(0);
            } else {
                checkState(allOpWrappers.size() == 0);
                this.mainOperatorWrapper = null;
                this.tailOperatorWrapper = null;
            }

            this.chainedSources =
                    createChainedSources(
                            containingTask,
                            configuration.getInputs(userCodeClassloader),
                            chainedConfigs,
                            userCodeClassloader,
                            allOpWrappers);

            this.numOperators = allOpWrappers.size();

            firstOperatorWrapper = linkOperatorWrappers(allOpWrappers);

            success = true;
        } finally {
            // make sure we clean up after ourselves in case of a failure after acquiring
            // the first resources
            if (!success) {
                for (int i = 0; i < streamOutputs.length; i++) {
                    if (streamOutputs[i] != null) {
                        streamOutputs[i].close();
                    }
                    streamOutputs[i] = null;
                }
            }
        }
    }

    @VisibleForTesting
    OperatorChain(
            List<StreamOperatorWrapper<?, ?>> allOperatorWrappers,
            RecordWriterOutput<?>[] streamOutputs,
            WatermarkGaugeExposingOutput<StreamRecord<OUT>> mainOperatorOutput,
            StreamOperatorWrapper<OUT, OP> mainOperatorWrapper) {
        this.streamOutputs = streamOutputs;
        this.finishedOnRestoreInput = null;
        this.mainOperatorOutput = checkNotNull(mainOperatorOutput);
        this.operatorEventDispatcher = null;

        checkState(allOperatorWrappers != null && allOperatorWrappers.size() > 0);
        this.mainOperatorWrapper = checkNotNull(mainOperatorWrapper);
        this.tailOperatorWrapper = allOperatorWrappers.get(0);
        this.numOperators = allOperatorWrappers.size();
        this.chainedSources = Collections.emptyMap();

        firstOperatorWrapper = linkOperatorWrappers(allOperatorWrappers);
    }

    public abstract boolean isTaskDeployedAsFinished();

    public abstract void dispatchOperatorEvent(
            OperatorID operator, SerializedValue<OperatorEvent> event) throws FlinkException;

    public abstract void prepareSnapshotPreBarrier(long checkpointId) throws Exception;

    /**
     * Ends the main operator input specified by {@code inputId}).
     *
     * @param inputId the input ID starts from 1 which indicates the first input.
     */
    public abstract void endInput(int inputId) throws Exception;

    /**
     * Initialize state and open all operators in the chain from <b>tail to heads</b>, contrary to
     * {@link StreamOperator#close()} which happens <b>heads to tail</b> (see {@link
     * #finishOperators(StreamTaskActionExecutor, StopMode)}).
     */
    public abstract void initializeStateAndOpenOperators(
            StreamTaskStateInitializer streamTaskStateInitializer) throws Exception;

    /**
     * Closes all operators in a chain effect way. Closing happens from <b>heads to tail</b>
     * operator in the chain, contrary to {@link StreamOperator#open()} which happens <b>tail to
     * heads</b> (see {@link #initializeStateAndOpenOperators(StreamTaskStateInitializer)}).
     */
    public abstract void finishOperators(StreamTaskActionExecutor actionExecutor, StopMode stopMode)
            throws Exception;

    public abstract void notifyCheckpointComplete(long checkpointId) throws Exception;

    public abstract void notifyCheckpointAborted(long checkpointId) throws Exception;

    public abstract void notifyCheckpointSubsumed(long checkpointId) throws Exception;

    public abstract void snapshotState(
            Map<OperatorID, OperatorSnapshotFutures> operatorSnapshotsInProgress,
            CheckpointMetaData checkpointMetaData,
            CheckpointOptions checkpointOptions,
            Supplier<Boolean> isRunning,
            ChannelStateWriter.ChannelStateWriteResult channelStateWriteResult,
            CheckpointStreamFactory storage)
            throws Exception;

    public OperatorEventDispatcher getOperatorEventDispatcher() {
        return operatorEventDispatcher;
    }

    public void broadcastEvent(AbstractEvent event) throws IOException {
        broadcastEvent(event, false);
    }

    public void broadcastEvent(AbstractEvent event, boolean isPriorityEvent) throws IOException {
        for (RecordWriterOutput<?> streamOutput : streamOutputs) {
            streamOutput.broadcastEvent(event, isPriorityEvent);
        }
    }

    public void alignedBarrierTimeout(long checkpointId) throws IOException {
        for (RecordWriterOutput<?> streamOutput : streamOutputs) {
            streamOutput.alignedBarrierTimeout(checkpointId);
        }
    }

    public void abortCheckpoint(long checkpointId, CheckpointException cause) {
        for (RecordWriterOutput<?> streamOutput : streamOutputs) {
            streamOutput.abortCheckpoint(checkpointId, cause);
        }
    }

    /**
     * Execute {@link StreamOperator#close()} of each operator in the chain of this {@link
     * StreamTask}. Closing happens from <b>tail to head</b> operator in the chain.
     */
    public void closeAllOperators() throws Exception {
        isClosed = true;
    }

    public RecordWriterOutput<?>[] getStreamOutputs() {
        return streamOutputs;
    }

    /** Returns an {@link Iterable} which traverses all operators in forward topological order. */
    @VisibleForTesting
    public Iterable<StreamOperatorWrapper<?, ?>> getAllOperators() {
        return getAllOperators(false);
    }

    /**
     * Returns an {@link Iterable} which traverses all operators in forward or reverse topological
     * order.
     */
    protected Iterable<StreamOperatorWrapper<?, ?>> getAllOperators(boolean reverse) {
        return reverse
                ? new StreamOperatorWrapper.ReadIterator(tailOperatorWrapper, true)
                : new StreamOperatorWrapper.ReadIterator(mainOperatorWrapper, false);
    }

    public Input getFinishedOnRestoreInputOrDefault(Input defaultInput) {
        return finishedOnRestoreInput == null ? defaultInput : finishedOnRestoreInput;
    }

    public int getNumberOfOperators() {
        return numOperators;
    }

    public WatermarkGaugeExposingOutput<StreamRecord<OUT>> getMainOperatorOutput() {
        return mainOperatorOutput;
    }

    public ChainedSource getChainedSource(StreamConfig.SourceInputConfig sourceInput) {
        checkArgument(
                chainedSources.containsKey(sourceInput),
                "Chained source with sourcedId = [%s] was not found",
                sourceInput);
        return chainedSources.get(sourceInput);
    }

    public List<Output<StreamRecord<?>>> getChainedSourceOutputs() {
        return chainedSources.values().stream()
                .map(ChainedSource::getSourceOutput)
                .collect(Collectors.toList());
    }

    public StreamTaskSourceInput<?> getSourceTaskInput(StreamConfig.SourceInputConfig sourceInput) {
        checkArgument(
                chainedSources.containsKey(sourceInput),
                "Chained source with sourcedId = [%s] was not found",
                sourceInput);
        return chainedSources.get(sourceInput).getSourceTaskInput();
    }

    public List<StreamTaskSourceInput<?>> getSourceTaskInputs() {
        return chainedSources.values().stream()
                .map(ChainedSource::getSourceTaskInput)
                .collect(Collectors.toList());
    }

    /**
     * This method should be called before finishing the record emission, to make sure any data that
     * is still buffered will be sent. It also ensures that all data sending related exceptions are
     * recognized.
     *
     * @throws IOException Thrown, if the buffered data cannot be pushed into the output streams.
     */
    public void flushOutputs() throws IOException {
        for (RecordWriterOutput<?> streamOutput : getStreamOutputs()) {
            streamOutput.flush();
        }
    }

    /**
     * This method releases all resources of the record writer output. It stops the output flushing
     * thread (if there is one) and releases all buffers currently held by the output serializers.
     *
     * <p>This method should never fail.
     */
    public void close() throws IOException {
        closer.close();
    }

    @Nullable
    public OP getMainOperator() {
        return (mainOperatorWrapper == null) ? null : mainOperatorWrapper.getStreamOperator();
    }

    @Nullable
    protected StreamOperator<?> getTailOperator() {
        return (tailOperatorWrapper == null) ? null : tailOperatorWrapper.getStreamOperator();
    }

    protected void snapshotChannelStates(
            StreamOperator<?> op,
            ChannelStateWriter.ChannelStateWriteResult channelStateWriteResult,
            OperatorSnapshotFutures snapshotInProgress) {
        if (op == getMainOperator()) {
            snapshotInProgress.setInputChannelStateFuture(
                    channelStateWriteResult
                            .getInputChannelStateHandles()
                            .thenApply(StateObjectCollection::new)
                            .thenApply(SnapshotResult::of));
        }
        if (op == getTailOperator()) {
            snapshotInProgress.setResultSubpartitionStateFuture(
                    channelStateWriteResult
                            .getResultSubpartitionStateHandles()
                            .thenApply(StateObjectCollection::new)
                            .thenApply(SnapshotResult::of));
        }
    }

    public boolean isClosed() {
        return isClosed;
    }

    /** Wrapper class to access the chained sources and their's outputs. */
    public static class ChainedSource {
        private final WatermarkGaugeExposingOutput<StreamRecord<?>> chainedSourceOutput;
        private final StreamTaskSourceInput<?> sourceTaskInput;

        public ChainedSource(
                WatermarkGaugeExposingOutput<StreamRecord<?>> chainedSourceOutput,
                StreamTaskSourceInput<?> sourceTaskInput) {
            this.chainedSourceOutput = chainedSourceOutput;
            this.sourceTaskInput = sourceTaskInput;
        }

        public WatermarkGaugeExposingOutput<StreamRecord<?>> getSourceOutput() {
            return chainedSourceOutput;
        }

        public StreamTaskSourceInput<?> getSourceTaskInput() {
            return sourceTaskInput;
        }
    }

    // ------------------------------------------------------------------------
    //  initialization utilities
    // ------------------------------------------------------------------------

    private void createChainOutputs(
            List<NonChainedOutput> outputsInOrder,
            RecordWriterDelegate<SerializationDelegate<StreamRecord<OUT>>> recordWriterDelegate,
            Map<Integer, StreamConfig> chainedConfigs,
            StreamTask<OUT, OP> containingTask,
            Map<IntermediateDataSetID, RecordWriterOutput<?>> recordWriterOutputs) {
        for (int i = 0; i < outputsInOrder.size(); ++i) {
            NonChainedOutput output = outputsInOrder.get(i);

            RecordWriterOutput<?> recordWriterOutput =
                    createStreamOutput(
                            recordWriterDelegate.getRecordWriter(i),
                            output,
                            chainedConfigs.get(output.getSourceNodeId()),
                            containingTask.getEnvironment());

            this.streamOutputs[i] = recordWriterOutput;
            recordWriterOutputs.put(output.getDataSetId(), recordWriterOutput);
        }
    }

    private RecordWriterOutput<OUT> createStreamOutput(
            RecordWriter<SerializationDelegate<StreamRecord<OUT>>> recordWriter,
            NonChainedOutput streamOutput,
            StreamConfig upStreamConfig,
            Environment taskEnvironment) {
        OutputTag sideOutputTag =
                streamOutput.getOutputTag(); // OutputTag, return null if not sideOutput

        TypeSerializer outSerializer;

        if (streamOutput.getOutputTag() != null) {
            // side output
            outSerializer =
                    upStreamConfig.getTypeSerializerSideOut(
                            streamOutput.getOutputTag(),
                            taskEnvironment.getUserCodeClassLoader().asClassLoader());
        } else {
            // main output
            outSerializer =
                    upStreamConfig.getTypeSerializerOut(
                            taskEnvironment.getUserCodeClassLoader().asClassLoader());
        }

        return closer.register(
                new RecordWriterOutput<OUT>(
                        recordWriter,
                        outSerializer,
                        sideOutputTag,
                        streamOutput.supportsUnalignedCheckpoints()));
    }

    @SuppressWarnings("rawtypes")
    private Map<StreamConfig.SourceInputConfig, ChainedSource> createChainedSources(
            StreamTask<OUT, OP> containingTask,
            StreamConfig.InputConfig[] configuredInputs,
            Map<Integer, StreamConfig> chainedConfigs,
            ClassLoader userCodeClassloader,
            List<StreamOperatorWrapper<?, ?>> allOpWrappers) {
        if (Arrays.stream(configuredInputs)
                .noneMatch(input -> input instanceof StreamConfig.SourceInputConfig)) {
            return Collections.emptyMap();
        }
        checkState(
                mainOperatorWrapper.getStreamOperator() instanceof MultipleInputStreamOperator,
                "Creating chained input is only supported with MultipleInputStreamOperator and MultipleInputStreamTask");
        Map<StreamConfig.SourceInputConfig, ChainedSource> chainedSourceInputs = new HashMap<>();
        MultipleInputStreamOperator<?> multipleInputOperator =
                (MultipleInputStreamOperator<?>) mainOperatorWrapper.getStreamOperator();
        List<Input> operatorInputs = multipleInputOperator.getInputs();

        int sourceInputGateIndex =
                Arrays.stream(containingTask.getEnvironment().getAllInputGates())
                                .mapToInt(IndexedInputGate::getInputGateIndex)
                                .max()
                                .orElse(-1)
                        + 1;

        for (int inputId = 0; inputId < configuredInputs.length; inputId++) {
            if (!(configuredInputs[inputId] instanceof StreamConfig.SourceInputConfig)) {
                continue;
            }
            StreamConfig.SourceInputConfig sourceInput =
                    (StreamConfig.SourceInputConfig) configuredInputs[inputId];
            int sourceEdgeId = sourceInput.getInputEdge().getSourceId();
            StreamConfig sourceInputConfig = chainedConfigs.get(sourceEdgeId);
            OutputTag outputTag = sourceInput.getInputEdge().getOutputTag();

            WatermarkGaugeExposingOutput chainedSourceOutput =
                    createChainedSourceOutput(
                            containingTask,
                            sourceInputConfig,
                            userCodeClassloader,
                            getFinishedOnRestoreInputOrDefault(operatorInputs.get(inputId)),
                            multipleInputOperator.getMetricGroup(),
                            outputTag);

            SourceOperator<?, ?> sourceOperator =
                    (SourceOperator<?, ?>)
                            createOperator(
                                    containingTask,
                                    sourceInputConfig,
                                    userCodeClassloader,
                                    (WatermarkGaugeExposingOutput<StreamRecord<OUT>>)
                                            chainedSourceOutput,
                                    allOpWrappers,
                                    true);
            chainedSourceInputs.put(
                    sourceInput,
                    new ChainedSource(
                            chainedSourceOutput,
                            this.isTaskDeployedAsFinished()
                                    ? new StreamTaskFinishedOnRestoreSourceInput<>(
                                            sourceOperator, sourceInputGateIndex++, inputId)
                                    : new StreamTaskSourceInput<>(
                                            sourceOperator, sourceInputGateIndex++, inputId)));
        }
        return chainedSourceInputs;
    }

    /**
     * Get the numRecordsOut counter for the operator represented by the given config. And re-use
     * the operator-level counter for the task-level numRecordsOut counter if this operator is at
     * the end of the operator chain.
     *
     * <p>Return null if we should not use the numRecordsOut counter to track the records emitted by
     * this operator.
     */
    @Nullable
    private Counter getOperatorRecordsOutCounter(
            StreamTask<?, ?> containingTask, StreamConfig operatorConfig) {
        ClassLoader userCodeClassloader = containingTask.getUserCodeClassLoader();
        Class<StreamOperatorFactory<?>> streamOperatorFactoryClass =
                operatorConfig.getStreamOperatorFactoryClass(userCodeClassloader);

        // Do not use the numRecordsOut counter on output if this operator is SinkWriterOperator.
        //
        // Metric "numRecordsOut" is defined as the total number of records written to the
        // external system in FLIP-33, but this metric is occupied in AbstractStreamOperator as the
        // number of records sent to downstream operators, which is number of Committable batches
        // sent to SinkCommitter. So we skip registering this metric on output and leave this metric
        // to sink writer implementations to report.
        try {
            Class<?> sinkWriterFactoryClass =
                    userCodeClassloader.loadClass(SinkWriterOperatorFactory.class.getName());
            if (sinkWriterFactoryClass.isAssignableFrom(streamOperatorFactoryClass)) {
                return null;
            }
        } catch (ClassNotFoundException e) {
            throw new StreamTaskException(
                    "Could not load SinkWriterOperatorFactory class from userCodeClassloader.", e);
        }

        InternalOperatorMetricGroup operatorMetricGroup =
                containingTask
                        .getEnvironment()
                        .getMetricGroup()
                        .getOrAddOperator(
                                operatorConfig.getOperatorID(), operatorConfig.getOperatorName());

        return operatorMetricGroup.getIOMetricGroup().getNumRecordsOutCounter();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private WatermarkGaugeExposingOutput<StreamRecord> createChainedSourceOutput(
            StreamTask<?, OP> containingTask,
            StreamConfig sourceInputConfig,
            ClassLoader userCodeClassloader,
            Input input,
            OperatorMetricGroup metricGroup,
            OutputTag outputTag) {

        Counter recordsOutCounter = getOperatorRecordsOutCounter(containingTask, sourceInputConfig);

        WatermarkGaugeExposingOutput<StreamRecord> chainedSourceOutput;
        if (containingTask.getExecutionConfig().isObjectReuseEnabled()) {
            chainedSourceOutput =
                    new ChainingOutput(input, recordsOutCounter, metricGroup, outputTag);
        } else {
            TypeSerializer<?> inSerializer =
                    sourceInputConfig.getTypeSerializerOut(userCodeClassloader);
            chainedSourceOutput =
                    new CopyingChainingOutput(
                            input, inSerializer, recordsOutCounter, metricGroup, outputTag);
        }
        /**
         * Chained sources are closed when {@link
         * org.apache.flink.streaming.runtime.io.StreamTaskSourceInput} are being closed.
         */
        return closer.register(chainedSourceOutput);
    }

    private <T> WatermarkGaugeExposingOutput<StreamRecord<T>> createOutputCollector(
            StreamTask<?, ?> containingTask,
            StreamConfig operatorConfig,
            Map<Integer, StreamConfig> chainedConfigs,
            ClassLoader userCodeClassloader,
            Map<IntermediateDataSetID, RecordWriterOutput<?>> recordWriterOutputs,
            List<StreamOperatorWrapper<?, ?>> allOperatorWrappers,
            MailboxExecutorFactory mailboxExecutorFactory,
            boolean shouldAddMetric) {
        List<OutputWithChainingCheck<StreamRecord<T>>> allOutputs = new ArrayList<>(4);

        // create collectors for the network outputs
        for (NonChainedOutput streamOutput :
                operatorConfig.getOperatorNonChainedOutputs(userCodeClassloader)) {
            @SuppressWarnings("unchecked")
            RecordWriterOutput<T> recordWriterOutput =
                    (RecordWriterOutput<T>) recordWriterOutputs.get(streamOutput.getDataSetId());

            allOutputs.add(recordWriterOutput);
        }

        // Create collectors for the chained outputs
        for (StreamEdge outputEdge : operatorConfig.getChainedOutputs(userCodeClassloader)) {
            int outputId = outputEdge.getTargetId();
            StreamConfig chainedOpConfig = chainedConfigs.get(outputId);

            WatermarkGaugeExposingOutput<StreamRecord<T>> output =
                    createOperatorChain(
                            containingTask,
                            operatorConfig,
                            chainedOpConfig,
                            chainedConfigs,
                            userCodeClassloader,
                            recordWriterOutputs,
                            allOperatorWrappers,
                            outputEdge.getOutputTag(),
                            mailboxExecutorFactory,
                            shouldAddMetric);
            checkState(output instanceof OutputWithChainingCheck);
            allOutputs.add((OutputWithChainingCheck) output);
            // If the operator has multiple downstream chained operators, only one of them should
            // increment the recordsOutCounter for this operator. Set shouldAddMetric to false
            // so that we would skip adding the counter to other downstream operators.
            shouldAddMetric = false;
        }

        WatermarkGaugeExposingOutput<StreamRecord<T>> result;

        if (allOutputs.size() == 1) {
            result = allOutputs.get(0);
            // only if this is a single RecordWriterOutput, reuse its numRecordOut for task.
            if (result instanceof RecordWriterOutput) {
                Counter numRecordsOutCounter = createNumRecordsOutCounter(containingTask);
                ((RecordWriterOutput<T>) result).setNumRecordsOut(numRecordsOutCounter);
            }
        } else {
            // send to N outputs. Note that this includes the special case
            // of sending to zero outputs
            @SuppressWarnings({"unchecked"})
            OutputWithChainingCheck<StreamRecord<T>>[] allOutputsArray =
                    new OutputWithChainingCheck[allOutputs.size()];
            for (int i = 0; i < allOutputs.size(); i++) {
                allOutputsArray[i] = allOutputs.get(i);
            }

            // This is the inverse of creating the normal ChainingOutput.
            // If the chaining output does not copy we need to copy in the broadcast output,
            // otherwise multi-chaining would not work correctly.
            Counter numRecordsOutForTask = createNumRecordsOutCounter(containingTask);
            if (containingTask.getExecutionConfig().isObjectReuseEnabled()) {
                result =
                        closer.register(
                                new CopyingBroadcastingOutputCollector<>(
                                        allOutputsArray, numRecordsOutForTask));
            } else {
                result =
                        closer.register(
                                new BroadcastingOutputCollector<>(
                                        allOutputsArray, numRecordsOutForTask));
            }
        }

        if (shouldAddMetric) {
            // Create a CountingOutput to increment the recordsOutCounter for this operator
            // if we have not added the counter to any downstream chained operator.
            Counter recordsOutCounter =
                    getOperatorRecordsOutCounter(containingTask, operatorConfig);
            if (recordsOutCounter != null) {
                result = new CountingOutput<>(result, recordsOutCounter);
            }
        }
        return result;
    }

    private static Counter createNumRecordsOutCounter(StreamTask<?, ?> containingTask) {
        TaskIOMetricGroup taskIOMetricGroup =
                containingTask.getEnvironment().getMetricGroup().getIOMetricGroup();
        Counter counter = new SimpleCounter();
        taskIOMetricGroup.reuseRecordsOutputCounter(counter);
        return counter;
    }

    /**
     * Recursively create chain of operators that starts from the given {@param operatorConfig}.
     * Operators are created tail to head and wrapped into an {@link WatermarkGaugeExposingOutput}.
     */
    private <IN, OUT> WatermarkGaugeExposingOutput<StreamRecord<IN>> createOperatorChain(
            StreamTask<OUT, ?> containingTask,
            StreamConfig prevOperatorConfig,
            StreamConfig operatorConfig,
            Map<Integer, StreamConfig> chainedConfigs,
            ClassLoader userCodeClassloader,
            Map<IntermediateDataSetID, RecordWriterOutput<?>> recordWriterOutputs,
            List<StreamOperatorWrapper<?, ?>> allOperatorWrappers,
            OutputTag<IN> outputTag,
            MailboxExecutorFactory mailboxExecutorFactory,
            boolean shouldAddMetricForPrevOperator) {
        // create the output that the operator writes to first. this may recursively create more
        // operators
        WatermarkGaugeExposingOutput<StreamRecord<OUT>> chainedOperatorOutput =
                createOutputCollector(
                        containingTask,
                        operatorConfig,
                        chainedConfigs,
                        userCodeClassloader,
                        recordWriterOutputs,
                        allOperatorWrappers,
                        mailboxExecutorFactory,
                        true);

        OneInputStreamOperator<IN, OUT> chainedOperator =
                createOperator(
                        containingTask,
                        operatorConfig,
                        userCodeClassloader,
                        chainedOperatorOutput,
                        allOperatorWrappers,
                        false);

        return wrapOperatorIntoOutput(
                chainedOperator,
                containingTask,
                prevOperatorConfig,
                operatorConfig,
                userCodeClassloader,
                outputTag,
                shouldAddMetricForPrevOperator);
    }

    /**
     * Create and return a single operator from the given {@param operatorConfig} that will be
     * producing records to the {@param output}.
     */
    private <OUT, OP extends StreamOperator<OUT>> OP createOperator(
            StreamTask<OUT, ?> containingTask,
            StreamConfig operatorConfig,
            ClassLoader userCodeClassloader,
            WatermarkGaugeExposingOutput<StreamRecord<OUT>> output,
            List<StreamOperatorWrapper<?, ?>> allOperatorWrappers,
            boolean isHead) {

        // now create the operator and give it the output collector to write its output to
        Tuple2<OP, Optional<ProcessingTimeService>> chainedOperatorAndTimeService =
                StreamOperatorFactoryUtil.createOperator(
                        operatorConfig.getStreamOperatorFactory(userCodeClassloader),
                        containingTask,
                        operatorConfig,
                        output,
                        operatorEventDispatcher);

        OP chainedOperator = chainedOperatorAndTimeService.f0;
        allOperatorWrappers.add(
                createOperatorWrapper(
                        chainedOperator,
                        containingTask,
                        operatorConfig,
                        chainedOperatorAndTimeService.f1,
                        isHead));

        chainedOperator
                .getMetricGroup()
                .gauge(
                        MetricNames.IO_CURRENT_OUTPUT_WATERMARK,
                        output.getWatermarkGauge()::getValue);
        return chainedOperator;
    }

    private <IN, OUT> WatermarkGaugeExposingOutput<StreamRecord<IN>> wrapOperatorIntoOutput(
            OneInputStreamOperator<IN, OUT> operator,
            StreamTask<OUT, ?> containingTask,
            StreamConfig prevOperatorConfig,
            StreamConfig operatorConfig,
            ClassLoader userCodeClassloader,
            OutputTag<IN> outputTag,
            boolean shouldAddMetricForPrevOperator) {

        Counter recordsOutCounter = null;

        if (shouldAddMetricForPrevOperator) {
            recordsOutCounter = getOperatorRecordsOutCounter(containingTask, prevOperatorConfig);
        }

        WatermarkGaugeExposingOutput<StreamRecord<IN>> currentOperatorOutput;
        if (containingTask.getExecutionConfig().isObjectReuseEnabled()) {
            currentOperatorOutput =
                    new ChainingOutput<>(
                            operator, recordsOutCounter, operator.getMetricGroup(), outputTag);
        } else {
            TypeSerializer<IN> inSerializer =
                    operatorConfig.getTypeSerializerIn1(userCodeClassloader);
            currentOperatorOutput =
                    new CopyingChainingOutput<>(
                            operator,
                            inSerializer,
                            recordsOutCounter,
                            operator.getMetricGroup(),
                            outputTag);
        }

        // wrap watermark gauges since registered metrics must be unique
        operator.getMetricGroup()
                .gauge(
                        MetricNames.IO_CURRENT_INPUT_WATERMARK,
                        currentOperatorOutput.getWatermarkGauge()::getValue);

        return closer.register(currentOperatorOutput);
    }

    /**
     * Links operator wrappers in forward topological order.
     *
     * @param allOperatorWrappers is an operator wrapper list of reverse topological order
     */
    private StreamOperatorWrapper<?, ?> linkOperatorWrappers(
            List<StreamOperatorWrapper<?, ?>> allOperatorWrappers) {
        StreamOperatorWrapper<?, ?> previous = null;
        for (StreamOperatorWrapper<?, ?> current : allOperatorWrappers) {
            if (previous != null) {
                previous.setPrevious(current);
            }
            current.setNext(previous);
            previous = current;
        }
        return previous;
    }

    private <T, P extends StreamOperator<T>> StreamOperatorWrapper<T, P> createOperatorWrapper(
            P operator,
            StreamTask<?, ?> containingTask,
            StreamConfig operatorConfig,
            Optional<ProcessingTimeService> processingTimeService,
            boolean isHead) {
        return new StreamOperatorWrapper<>(
                operator,
                processingTimeService,
                containingTask
                        .getMailboxExecutorFactory()
                        .createExecutor(operatorConfig.getChainIndex()),
                isHead);
    }

    protected void sendAcknowledgeCheckpointEvent(long checkpointId) {
        if (operatorEventDispatcher == null) {
            return;
        }

        operatorEventDispatcher
                .getRegisteredOperators()
                .forEach(
                        x ->
                                operatorEventDispatcher
                                        .getOperatorEventGateway(x)
                                        .sendEventToCoordinator(
                                                new AcknowledgeCheckpointEvent(checkpointId)));
    }
}
