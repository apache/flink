/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.operators.sink;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.runtime.metrics.groups.InternalSinkWriterMetricGroup;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.UserCodeClassLoader;
import org.apache.flink.util.function.BiFunctionWithException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.OptionalLong;

import static org.apache.flink.util.IOUtils.closeAll;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An operator that processes records to be written into a {@link
 * org.apache.flink.api.connector.sink.Sink}. It also has a way to process committables with the
 * same parallelism or send them downstream to a {@link CommitterOperator} with a different
 * parallelism.
 *
 * <p>The operator may be part of a sink pipeline and is the first operator. There are currently two
 * ways this operator is used:
 *
 * <ul>
 *   <li>In streaming mode, there is this operator with parallelism p containing {@link
 *       org.apache.flink.api.connector.sink.SinkWriter} and {@link
 *       org.apache.flink.api.connector.sink.Committer} and a {@link CommitterOperator} containing
 *       the {@link org.apache.flink.api.connector.sink.GlobalCommitter} with parallelism 1.
 *   <li>In batch mode, there is this operator with parallelism p containing {@link
 *       org.apache.flink.api.connector.sink.SinkWriter} and a {@link CommitterOperator} containing
 *       the {@link org.apache.flink.api.connector.sink.Committer} and {@link
 *       org.apache.flink.api.connector.sink.GlobalCommitter} with parallelism 1.
 * </ul>
 *
 * @param <InputT> the type of the committable
 * @param <CommT> the type of the committable (to send to downstream operators)
 * @param <WriterStateT> the type of the writer state for stateful sinks
 */
class SinkOperator<InputT, CommT, WriterStateT> extends AbstractStreamOperator<byte[]>
        implements OneInputStreamOperator<InputT, byte[]>, BoundedOneInput {

    /** The runtime information of the input element. */
    private final Context<InputT> context;

    // ------------------------------- runtime fields ---------------------------------------

    /** We listen to this ourselves because we don't have an {@link InternalTimerService}. */
    private Long currentWatermark = Long.MIN_VALUE;

    private SinkWriter<InputT, CommT, WriterStateT> sinkWriter;

    private final SinkWriterStateHandler<WriterStateT> sinkWriterStateHandler;

    private final CommitterHandler<CommT, CommT> committerHandler;

    private CommitRetrier commitRetrier;

    @Nullable private final SimpleVersionedSerializer<CommT> committableSerializer;

    private final BiFunctionWithException<
                    Sink.InitContext,
                    List<WriterStateT>,
                    SinkWriter<InputT, CommT, WriterStateT>,
                    IOException>
            writerFactory;

    private final MailboxExecutor mailboxExecutor;
    // record endOfInput state to avoid duplicate prepareCommit on final notifyCheckpointComplete
    // once FLIP-147 is fully operational all endOfInput processing needs to be removed
    private boolean endOfInput = false;

    SinkOperator(
            ProcessingTimeService processingTimeService,
            MailboxExecutor mailboxExecutor,
            BiFunctionWithException<
                            Sink.InitContext,
                            List<WriterStateT>,
                            SinkWriter<InputT, CommT, WriterStateT>,
                            IOException>
                    writerFactory,
            SinkWriterStateHandler<WriterStateT> sinkWriterStateHandler,
            CommitterHandler<CommT, CommT> committerHandler,
            @Nullable SimpleVersionedSerializer<CommT> committableSerializer) {
        this.processingTimeService = checkNotNull(processingTimeService);
        this.mailboxExecutor = checkNotNull(mailboxExecutor);
        this.writerFactory = checkNotNull(writerFactory);
        this.sinkWriterStateHandler = checkNotNull(sinkWriterStateHandler);
        this.committerHandler = checkNotNull(committerHandler);
        this.committableSerializer = committableSerializer;
        this.context = new Context<>();
        this.commitRetrier = new CommitRetrier(processingTimeService, committerHandler);
    }

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<byte[]>> output) {
        super.setup(containingTask, config, output);
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        OptionalLong checkpointId = context.getRestoredCheckpointId();
        sinkWriter =
                writerFactory.apply(
                        createInitContext(
                                checkpointId.isPresent() ? checkpointId.getAsLong() : null),
                        sinkWriterStateHandler.initializeState(context));
        committerHandler.initializeState(context);
        commitRetrier.retryWithDelay();
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        sinkWriterStateHandler.snapshotState(sinkWriter::snapshotState, context.getCheckpointId());
        committerHandler.snapshotState(context);
    }

    @Override
    public void processElement(StreamRecord<InputT> element) throws Exception {
        context.element = element;
        sinkWriter.write(element.getValue(), context);
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        super.prepareSnapshotPreBarrier(checkpointId);
        if (!endOfInput) {
            emitCommittables(committerHandler.processCommittables(sinkWriter.prepareCommit(false)));
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        super.notifyCheckpointComplete(checkpointId);
        emitCommittables(committerHandler.notifyCheckpointCompleted(checkpointId));
        commitRetrier.retryWithDelay();
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        super.processWatermark(mark);
        this.currentWatermark = mark.getTimestamp();
        sinkWriter.writeWatermark(
                new org.apache.flink.api.common.eventtime.Watermark(mark.getTimestamp()));
    }

    @Override
    public void endInput() throws Exception {
        endOfInput = true;
        emitCommittables(committerHandler.processCommittables(sinkWriter.prepareCommit(true)));
        emitCommittables(committerHandler.endOfInput());
        commitRetrier.retryIndefinitely();
    }

    private void emitCommittables(Collection<CommT> committables) throws IOException {
        if (committableSerializer != null) {
            for (CommT committable : committables) {
                output.collect(
                        new StreamRecord<>(
                                SimpleVersionedSerialization.writeVersionAndSerialize(
                                        committableSerializer, committable)));
            }
        }
    }

    @Override
    public void close() throws Exception {
        closeAll(committerHandler, sinkWriter, super::close);
    }

    private Sink.InitContext createInitContext(@Nullable Long restoredCheckpointId) {
        return new InitContextImpl(
                getRuntimeContext(),
                processingTimeService,
                mailboxExecutor,
                InternalSinkWriterMetricGroup.wrap(getMetricGroup()),
                restoredCheckpointId);
    }

    private class Context<IN> implements SinkWriter.Context {

        private StreamRecord<IN> element;

        @Override
        public long currentWatermark() {
            return currentWatermark;
        }

        @Override
        public Long timestamp() {
            if (element.hasTimestamp()) {
                return element.getTimestamp();
            }
            return null;
        }
    }

    private static class InitContextImpl implements Sink.InitContext {

        private final ProcessingTimeService processingTimeService;

        private final MailboxExecutor mailboxExecutor;

        private final SinkWriterMetricGroup metricGroup;

        @Nullable private final Long restoredCheckpointId;

        private final StreamingRuntimeContext runtimeContext;

        public InitContextImpl(
                StreamingRuntimeContext runtimeContext,
                ProcessingTimeService processingTimeService,
                MailboxExecutor mailboxExecutor,
                SinkWriterMetricGroup metricGroup,
                @Nullable Long restoredCheckpointId) {
            this.runtimeContext = checkNotNull(runtimeContext);
            this.mailboxExecutor = checkNotNull(mailboxExecutor);
            this.processingTimeService = checkNotNull(processingTimeService);
            this.metricGroup = checkNotNull(metricGroup);
            this.restoredCheckpointId = restoredCheckpointId;
        }

        @Override
        public UserCodeClassLoader getUserCodeClassLoader() {
            return new UserCodeClassLoader() {
                @Override
                public ClassLoader asClassLoader() {
                    return runtimeContext.getUserCodeClassLoader();
                }

                @Override
                public void registerReleaseHookIfAbsent(
                        String releaseHookName, Runnable releaseHook) {
                    runtimeContext.registerUserCodeClassLoaderReleaseHookIfAbsent(
                            releaseHookName, releaseHook);
                }
            };
        }

        @Override
        public int getNumberOfParallelSubtasks() {
            return runtimeContext.getNumberOfParallelSubtasks();
        }

        @Override
        public MailboxExecutor getMailboxExecutor() {
            return mailboxExecutor;
        }

        @Override
        public Sink.ProcessingTimeService getProcessingTimeService() {
            return new ProcessingTimerServiceImpl(processingTimeService);
        }

        @Override
        public int getSubtaskId() {
            return runtimeContext.getIndexOfThisSubtask();
        }

        @Override
        public SinkWriterMetricGroup metricGroup() {
            return metricGroup;
        }

        @Override
        public OptionalLong getRestoredCheckpointId() {
            return restoredCheckpointId == null
                    ? OptionalLong.empty()
                    : OptionalLong.of(restoredCheckpointId);
        }
    }

    private static class ProcessingTimerServiceImpl implements Sink.ProcessingTimeService {
        private final ProcessingTimeService processingTimeService;

        public ProcessingTimerServiceImpl(ProcessingTimeService processingTimeService) {
            this.processingTimeService = checkNotNull(processingTimeService);
        }

        @Override
        public long getCurrentProcessingTime() {
            return processingTimeService.getCurrentProcessingTime();
        }

        @Override
        public void registerProcessingTimer(
                long time,
                Sink.ProcessingTimeService.ProcessingTimeCallback processingTimerCallback) {
            checkNotNull(processingTimerCallback);
            processingTimeService.registerTimer(time, processingTimerCallback::onProcessingTime);
        }
    }
}
