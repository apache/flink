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

package org.apache.flink.streaming.api.transformations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService.ProcessingTimeCallback;
import org.apache.flink.api.common.serialization.SerializationSchema.InitializationContext;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink.ProcessingTimeService;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.api.connector.sink2.StatefulSink.StatefulSinkWriter;
import org.apache.flink.api.connector.sink2.StatefulSink.WithCompatibleState;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink.PrecommittingSinkWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.StandardSinkTopologies;
import org.apache.flink.streaming.api.connector.sink2.WithPostCommitTopology;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.UserCodeClassLoader;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

/** Translates Sink V1 into Sink V2. */
@Internal
public class SinkV1Adapter<InputT, CommT, WriterStateT, GlobalCommT> implements Sink<InputT> {

    private final org.apache.flink.api.connector.sink.Sink<InputT, CommT, WriterStateT, GlobalCommT>
            sink;

    private SinkV1Adapter(
            org.apache.flink.api.connector.sink.Sink<InputT, CommT, WriterStateT, GlobalCommT>
                    sink) {
        this.sink = sink;
    }

    public static <InputT> Sink<InputT> wrap(
            org.apache.flink.api.connector.sink.Sink<InputT, ?, ?, ?> sink) {
        return new SinkV1Adapter<>(sink).asSpecializedSink();
    }

    @Override
    public SinkWriterV1Adapter<InputT, CommT, WriterStateT> createWriter(InitContext context)
            throws IOException {
        org.apache.flink.api.connector.sink.SinkWriter<InputT, CommT, WriterStateT> writer =
                sink.createWriter(new InitContextAdapter(context), Collections.emptyList());
        return new SinkWriterV1Adapter<>(writer);
    }

    public Sink<InputT> asSpecializedSink() {
        boolean stateful = false;
        boolean globalCommitter = false;
        boolean committer = false;
        if (sink.getWriterStateSerializer().isPresent()) {
            stateful = true;
        }
        if (sink.getGlobalCommittableSerializer().isPresent()) {
            globalCommitter = true;
        }
        try {
            if (sink.createCommitter().isPresent()) {
                committer = true;
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to instantiate committer.", e);
        }

        if (globalCommitter && committer && stateful) {
            return new StatefulGlobalTwoPhaseCommittingSinkAdapter();
        }
        if (globalCommitter) {
            return new GlobalCommittingSinkAdapter();
        }
        if (committer && stateful) {
            return new StatefulTwoPhaseCommittingSinkAdapter();
        }
        if (committer) {
            return new TwoPhaseCommittingSinkAdapter();
        }
        if (stateful) {
            return new StatefulSinkAdapter();
        }
        return this;
    }

    private static class SinkWriterV1Adapter<InputT, CommT, WriterStateT>
            implements StatefulSinkWriter<InputT, WriterStateT>,
                    PrecommittingSinkWriter<InputT, CommT> {

        private final org.apache.flink.api.connector.sink.SinkWriter<InputT, CommT, WriterStateT>
                writer;
        private boolean endOfInput = false;
        private final WriterContextAdapter contextAdapter = new WriterContextAdapter();

        public SinkWriterV1Adapter(
                org.apache.flink.api.connector.sink.SinkWriter<InputT, CommT, WriterStateT>
                        writer) {
            this.writer = writer;
        }

        @Override
        public void write(InputT element, Context context)
                throws IOException, InterruptedException {
            contextAdapter.setContext(context);
            this.writer.write(element, contextAdapter);
        }

        @Override
        public void flush(boolean endOfInput) throws IOException, InterruptedException {
            this.endOfInput = endOfInput;
        }

        @Override
        public List<WriterStateT> snapshotState(long checkpointId) throws IOException {
            return writer.snapshotState(checkpointId);
        }

        @Override
        public Collection<CommT> prepareCommit() throws IOException, InterruptedException {
            return writer.prepareCommit(endOfInput);
        }

        @Override
        public void close() throws Exception {
            writer.close();
        }

        @Override
        public void writeWatermark(Watermark watermark) throws IOException, InterruptedException {
            writer.writeWatermark(watermark);
        }
    }

    private static class WriterContextAdapter implements SinkWriter.Context {
        private org.apache.flink.api.connector.sink2.SinkWriter.Context context;

        public void setContext(org.apache.flink.api.connector.sink2.SinkWriter.Context context) {
            this.context = context;
        }

        @Override
        public long currentWatermark() {
            return context.currentWatermark();
        }

        @Override
        public Long timestamp() {
            return context.timestamp();
        }
    }

    private static class InitContextAdapter
            implements org.apache.flink.api.connector.sink.Sink.InitContext {

        private final InitContext context;

        public InitContextAdapter(InitContext context) {
            this.context = context;
        }

        @Override
        public UserCodeClassLoader getUserCodeClassLoader() {
            return context.getUserCodeClassLoader();
        }

        @Override
        public MailboxExecutor getMailboxExecutor() {
            return context.getMailboxExecutor();
        }

        @Override
        public ProcessingTimeService getProcessingTimeService() {
            return new ProcessingTimeServiceAdapter(context.getProcessingTimeService());
        }

        @Override
        public int getSubtaskId() {
            return context.getSubtaskId();
        }

        @Override
        public int getNumberOfParallelSubtasks() {
            return context.getNumberOfParallelSubtasks();
        }

        @Override
        public SinkWriterMetricGroup metricGroup() {
            return context.metricGroup();
        }

        @Override
        public OptionalLong getRestoredCheckpointId() {
            return context.getRestoredCheckpointId();
        }

        public InitializationContext asSerializationSchemaInitializationContext() {
            return context.asSerializationSchemaInitializationContext();
        }
    }

    private static class ProcessingTimeCallbackAdapter implements ProcessingTimeCallback {

        private final ProcessingTimeService.ProcessingTimeCallback processingTimerCallback;

        public ProcessingTimeCallbackAdapter(
                ProcessingTimeService.ProcessingTimeCallback processingTimerCallback) {
            this.processingTimerCallback = processingTimerCallback;
        }

        @Override
        public void onProcessingTime(long time) throws IOException, InterruptedException {
            processingTimerCallback.onProcessingTime(time);
        }
    }

    private static class ProcessingTimeServiceAdapter implements ProcessingTimeService {

        private final org.apache.flink.api.common.operators.ProcessingTimeService
                processingTimeService;

        public ProcessingTimeServiceAdapter(
                org.apache.flink.api.common.operators.ProcessingTimeService processingTimeService) {
            this.processingTimeService = processingTimeService;
        }

        @Override
        public long getCurrentProcessingTime() {
            return processingTimeService.getCurrentProcessingTime();
        }

        @Override
        public void registerProcessingTimer(
                long time, ProcessingTimeCallback processingTimerCallback) {
            processingTimeService.registerTimer(
                    time, new ProcessingTimeCallbackAdapter(processingTimerCallback));
        }
    }

    private static class CommitterAdapter<CommT> implements Committer<CommT> {

        private final org.apache.flink.api.connector.sink.Committer<CommT> committer;

        public CommitterAdapter(org.apache.flink.api.connector.sink.Committer<CommT> committer) {
            this.committer = committer;
        }

        @Override
        public void commit(Collection<CommitRequest<CommT>> commitRequests)
                throws IOException, InterruptedException {
            List<CommT> failed =
                    committer.commit(
                            commitRequests.stream()
                                    .map(CommitRequest::getCommittable)
                                    .collect(Collectors.toList()));
            if (!failed.isEmpty()) {
                Set<CommT> indexed = Collections.newSetFromMap(new IdentityHashMap<>());
                indexed.addAll(failed);
                commitRequests.stream()
                        .filter(request -> indexed.contains(request.getCommittable()))
                        .forEach(CommitRequest::retryLater);
            }
        }

        @Override
        public void close() throws Exception {
            committer.close();
        }
    }

    /** Main class to simulate SinkV1 with SinkV2. */
    class PlainSinkAdapter implements Sink<InputT> {
        @Override
        public SinkWriterV1Adapter<InputT, CommT, WriterStateT> createWriter(InitContext context)
                throws IOException {
            return SinkV1Adapter.this.createWriter(context);
        }

        public org.apache.flink.api.connector.sink.Sink<InputT, CommT, WriterStateT, GlobalCommT>
                getSink() {
            return sink;
        }
    }

    private class StatefulSinkAdapter extends PlainSinkAdapter
            implements StatefulSink<InputT, WriterStateT> {
        @Override
        public StatefulSinkWriter<InputT, WriterStateT> restoreWriter(
                InitContext context, Collection<WriterStateT> recoveredState) throws IOException {
            org.apache.flink.api.connector.sink.SinkWriter<InputT, CommT, WriterStateT> writer =
                    sink.createWriter(
                            new InitContextAdapter(context), new ArrayList<>(recoveredState));
            return new SinkWriterV1Adapter<>(writer);
        }

        @Override
        public SimpleVersionedSerializer<WriterStateT> getWriterStateSerializer() {
            return sink.getWriterStateSerializer()
                    .orElseThrow(
                            () ->
                                    new IllegalStateException(
                                            "This method should only be called after adapter established that the result is non-empty."));
        }
    }

    private class TwoPhaseCommittingSinkAdapter extends PlainSinkAdapter
            implements TwoPhaseCommittingSink<InputT, CommT>, WithCompatibleState {
        @Override
        public Committer<CommT> createCommitter() throws IOException {
            return new CommitterAdapter<>(
                    sink.createCommitter().orElse(new SinkV1Adapter.NoopCommitter<>()));
        }

        @Override
        public SimpleVersionedSerializer<CommT> getCommittableSerializer() {
            return sink.getCommittableSerializer()
                    .orElseThrow(
                            () ->
                                    new IllegalStateException(
                                            "This method should only be called after adapter established that the result is non-empty."));
        }

        @Override
        public Collection<String> getCompatibleWriterStateNames() {
            return sink.getCompatibleStateNames();
        }
    }

    private class GlobalCommittingSinkAdapter extends TwoPhaseCommittingSinkAdapter
            implements WithPostCommitTopology<InputT, CommT> {

        @Override
        public void addPostCommitTopology(DataStream<CommittableMessage<CommT>> committables) {

            StandardSinkTopologies.addGlobalCommitter(
                    committables,
                    GlobalCommitterAdapter::new,
                    () -> sink.getCommittableSerializer().get());
        }
    }

    private class StatefulTwoPhaseCommittingSinkAdapter extends StatefulSinkAdapter
            implements TwoPhaseCommittingSink<InputT, CommT>, WithCompatibleState {
        TwoPhaseCommittingSinkAdapter adapter = new TwoPhaseCommittingSinkAdapter();

        @Override
        public Committer<CommT> createCommitter() throws IOException {
            return adapter.createCommitter();
        }

        @Override
        public SimpleVersionedSerializer<CommT> getCommittableSerializer() {
            return adapter.getCommittableSerializer();
        }

        @Override
        public Collection<String> getCompatibleWriterStateNames() {
            return adapter.getCompatibleWriterStateNames();
        }
    }

    private class StatefulGlobalTwoPhaseCommittingSinkAdapter
            extends StatefulTwoPhaseCommittingSinkAdapter
            implements WithPostCommitTopology<InputT, CommT> {
        GlobalCommittingSinkAdapter globalCommittingSinkAdapter = new GlobalCommittingSinkAdapter();

        @Override
        public void addPostCommitTopology(DataStream<CommittableMessage<CommT>> committables) {
            globalCommittingSinkAdapter.addPostCommitTopology(committables);
        }
    }

    /**
     * A committer that fakes successful commits such that the global committer is called. This is
     * only used for topologies without committer but with global committer and avoids a special
     * case (post-commit topology without committer).
     */
    private static class NoopCommitter<CommT>
            implements org.apache.flink.api.connector.sink.Committer<CommT> {

        @Override
        public List<CommT> commit(List<CommT> committables) {
            return Collections.emptyList();
        }

        @Override
        public void close() throws Exception {}
    }

    /** Simulate the global committer behaviour with a committer. */
    @Internal
    public class GlobalCommitterAdapter implements Committer<CommT> {
        final GlobalCommitter<CommT, GlobalCommT> globalCommitter;
        final SimpleVersionedSerializer<GlobalCommT> globalCommittableSerializer;

        GlobalCommitterAdapter() {
            try {
                globalCommitter = sink.createGlobalCommitter().get();
                globalCommittableSerializer = sink.getGlobalCommittableSerializer().get();
            } catch (IOException e) {
                throw new UncheckedIOException("Cannot create global committer", e);
            }
        }

        @Override
        public void close() throws Exception {
            globalCommitter.close();
        }

        @Override
        public void commit(Collection<CommitRequest<CommT>> committables)
                throws IOException, InterruptedException {
            if (committables.isEmpty()) {
                return;
            }

            List<CommT> rawCommittables =
                    committables.stream()
                            .map(CommitRequest::getCommittable)
                            .collect(Collectors.toList());
            List<GlobalCommT> globalCommittables =
                    Collections.singletonList(globalCommitter.combine(rawCommittables));
            List<GlobalCommT> failures = globalCommitter.commit(globalCommittables);
            // Only committables are retriable so the complete batch of committables is retried
            // because we cannot trace back the committable to which global committable it belongs.
            // This might lead to committing the same global committable twice, but we assume that
            // the GlobalCommitter commit call is idempotent.
            if (!failures.isEmpty()) {
                committables.forEach(CommitRequest::retryLater);
            }
        }

        public GlobalCommitter<CommT, GlobalCommT> getGlobalCommitter() {
            return globalCommitter;
        }

        public SimpleVersionedSerializer<GlobalCommT> getGlobalCommittableSerializer() {
            return globalCommittableSerializer;
        }
    }
}
