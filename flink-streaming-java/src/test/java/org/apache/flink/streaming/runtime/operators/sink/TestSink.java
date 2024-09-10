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

package org.apache.flink.streaming.runtime.operators.sink;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializerAdapter;
import org.apache.flink.streaming.api.transformations.SinkV1Adapter;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A {@link Sink TestSink} for all the sink related tests. Use only for tests where {@link
 * SinkV1Adapter} should be tested.
 *
 * @deprecated Use {@link TestSinkV2} instead.
 */
@Deprecated
public class TestSink<T> implements Sink<T, String, Integer, String> {
    public static final SimpleVersionedSerializerAdapter<String> COMMITTABLE_SERIALIZER =
            TestSinkV2.COMMITTABLE_SERIALIZER;
    public static final SimpleVersionedSerializerAdapter<Integer> WRITER_SERIALIZER =
            TestSinkV2.WRITER_SERIALIZER;

    public static final String END_OF_INPUT_STR = "end of input";

    private final DefaultSinkWriter<T> writer;

    @Nullable private final SimpleVersionedSerializer<Integer> writerStateSerializer;

    @Nullable private final Committer<String> committer;

    @Nullable private final SimpleVersionedSerializer<String> committableSerializer;

    @Nullable private final GlobalCommitter<String, String> globalCommitter;

    @Nullable private final SimpleVersionedSerializer<String> globalCommittableSerializer;

    private final Collection<String> compatibleStateNames;

    private TestSink(
            DefaultSinkWriter<T> writer,
            @Nullable SimpleVersionedSerializer<Integer> writerStateSerializer,
            @Nullable Committer<String> committer,
            @Nullable SimpleVersionedSerializer<String> committableSerializer,
            @Nullable GlobalCommitter<String, String> globalCommitter,
            @Nullable SimpleVersionedSerializer<String> globalCommittableSerializer,
            Collection<String> compatibleStateNames) {
        this.writer = writer;
        this.writerStateSerializer = writerStateSerializer;
        this.committer = committer;
        this.committableSerializer = committableSerializer;
        this.globalCommitter = globalCommitter;
        this.globalCommittableSerializer = globalCommittableSerializer;
        this.compatibleStateNames = compatibleStateNames;
    }

    @Override
    public SinkWriter<T, String, Integer> createWriter(InitContext context, List<Integer> states) {
        writer.init(context);
        writer.restoredFrom(states);
        writer.setProcessingTimerService(context.getProcessingTimeService());
        return writer;
    }

    @Override
    public Optional<Committer<String>> createCommitter() {
        return Optional.ofNullable(committer);
    }

    @Override
    public Optional<GlobalCommitter<String, String>> createGlobalCommitter() {
        return Optional.ofNullable(globalCommitter);
    }

    @Override
    public Optional<SimpleVersionedSerializer<String>> getCommittableSerializer() {
        return Optional.ofNullable(committableSerializer);
    }

    @Override
    public Optional<SimpleVersionedSerializer<String>> getGlobalCommittableSerializer() {
        return Optional.ofNullable(globalCommittableSerializer);
    }

    @Override
    public Optional<SimpleVersionedSerializer<Integer>> getWriterStateSerializer() {
        return Optional.ofNullable(writerStateSerializer);
    }

    @Override
    public Collection<String> getCompatibleStateNames() {
        return compatibleStateNames;
    }

    public static Builder<Integer> newBuilder() {
        return new Builder<>();
    }

    public org.apache.flink.api.connector.sink2.Sink<T> asV2() {
        return SinkV1Adapter.wrap(this);
    }

    public DefaultSinkWriter<T> getWriter() {
        return writer;
    }

    /** A builder class for {@link TestSink}. */
    public static class Builder<T> {

        private DefaultSinkWriter<T> writer = new DefaultSinkWriter<>();

        private SimpleVersionedSerializer<Integer> writerStateSerializer;

        private Committer<String> committer;

        private GlobalCommitter<String, String> globalCommitter;

        private Collection<String> compatibleStateNames = Collections.emptyList();

        public <W> Builder<W> setWriter(DefaultSinkWriter<W> writer) {
            this.writer = (DefaultSinkWriter<T>) checkNotNull(writer);
            return (Builder<W>) this;
        }

        public Builder<T> withWriterState() {
            this.writerStateSerializer = WRITER_SERIALIZER;
            return this;
        }

        public Builder<T> setCommitter(Committer<String> committer) {
            this.committer = committer;
            return this;
        }

        public Builder<T> setDefaultCommitter() {
            this.committer = new DefaultCommitter();
            return this;
        }

        public Builder<T> setDefaultCommitter(Supplier<Queue<String>> queueSupplier) {
            this.committer = new DefaultCommitter(queueSupplier);
            return this;
        }

        public Builder<T> setDefaultGlobalCommitter() {
            this.globalCommitter = new DefaultGlobalCommitter("");
            return this;
        }

        public Builder<T> setGlobalCommitter(Supplier<Queue<String>> queueSupplier) {
            this.globalCommitter = new DefaultGlobalCommitter(queueSupplier);
            return this;
        }

        public Builder<T> setCompatibleStateNames(Collection<String> compatibleStateNames) {
            this.compatibleStateNames = compatibleStateNames;
            return this;
        }

        public Builder<T> setCompatibleStateNames(String... compatibleStateNames) {
            return setCompatibleStateNames(Arrays.asList(compatibleStateNames));
        }

        public TestSink<T> build() {
            return new TestSink<>(
                    writer,
                    writerStateSerializer,
                    committer,
                    committer == null && globalCommitter == null ? null : COMMITTABLE_SERIALIZER,
                    globalCommitter,
                    globalCommitter == null ? null : COMMITTABLE_SERIALIZER,
                    compatibleStateNames);
        }
    }

    // -------------------------------------- Sink Writer ------------------------------------------

    /** Base class for out testing {@link SinkWriter Writers}. */
    public static class DefaultSinkWriter<T>
            implements SinkWriter<T, String, Integer>, Serializable {

        protected List<String> elements;

        protected List<Watermark> watermarks;

        protected ProcessingTimeService processingTimerService;

        private int recordCount;

        protected long lastCheckpointId = -1;

        protected DefaultSinkWriter() {
            this.elements = new ArrayList<>();
            this.watermarks = new ArrayList<>();
        }

        @Override
        public void write(T element, Context context) {
            elements.add(
                    Tuple3.of(element, context.timestamp(), context.currentWatermark()).toString());
            recordCount++;
        }

        @Override
        public void writeWatermark(Watermark watermark) throws IOException {
            watermarks.add(watermark);
        }

        @Override
        public List<String> prepareCommit(boolean flush) {
            List<String> result = elements;
            elements = new ArrayList<>();
            return result;
        }

        @Override
        public List<Integer> snapshotState(long checkpointId) throws IOException {
            lastCheckpointId = checkpointId;
            return Collections.singletonList(recordCount);
        }

        @Override
        public void close() throws Exception {}

        void restoredFrom(List<Integer> states) {
            recordCount = states.isEmpty() ? 0 : states.get(0);
        }

        void setProcessingTimerService(ProcessingTimeService processingTimerService) {
            this.processingTimerService = processingTimerService;
        }

        public void init(InitContext context) {}

        public int getRecordCount() {
            return recordCount;
        }
    }

    // -------------------------------------- Sink Committer ---------------------------------------

    /** Base class for testing {@link Committer} and {@link GlobalCommitter}. */
    static class DefaultCommitter implements Committer<String>, Serializable {

        @Nullable protected Queue<String> committedData;

        private boolean isClosed;

        @Nullable private final Supplier<Queue<String>> queueSupplier;

        public DefaultCommitter() {
            this.committedData = new ConcurrentLinkedQueue<>();
            this.isClosed = false;
            this.queueSupplier = null;
        }

        public DefaultCommitter(@Nullable Supplier<Queue<String>> queueSupplier) {
            this.queueSupplier = queueSupplier;
            this.isClosed = false;
            this.committedData = null;
        }

        public List<String> getCommittedData() {
            if (committedData != null) {
                return new ArrayList<>(committedData);
            } else {
                return Collections.emptyList();
            }
        }

        @Override
        public List<String> commit(List<String> committables) {
            if (committedData == null) {
                assertThat(queueSupplier).isNotNull();
                committedData = queueSupplier.get();
            }
            committedData.addAll(committables);
            return Collections.emptyList();
        }

        public void close() throws Exception {
            isClosed = true;
        }

        public boolean isClosed() {
            return isClosed;
        }
    }

    /** A {@link Committer} that always re-commits the committables data it received. */
    static class RetryOnceCommitter extends DefaultCommitter implements Committer<String> {

        private final Set<String> seen = new LinkedHashSet<>();

        @Override
        public List<String> commit(List<String> committables) {
            committables.forEach(
                    c -> {
                        if (seen.remove(c)) {
                            checkNotNull(committedData);
                            committedData.add(c);
                        } else {
                            seen.add(c);
                        }
                    });
            return new ArrayList<>(seen);
        }
    }

    // ------------------------------------- Sink Global Committer ---------------------------------

    /** A {@link GlobalCommitter} that always commits global committables successfully. */
    static class DefaultGlobalCommitter extends DefaultCommitter
            implements GlobalCommitter<String, String> {

        static final Function<List<String>, String> COMBINER =
                strings -> {
                    // we sort here because we want to have a deterministic result during the unit
                    // test
                    Collections.sort(strings);
                    return String.join("+", strings);
                };

        private final String committedSuccessData;

        DefaultGlobalCommitter(String committedSuccessData) {
            this.committedSuccessData = committedSuccessData;
        }

        DefaultGlobalCommitter(Supplier<Queue<String>> queueSupplier) {
            super(queueSupplier);
            committedSuccessData = "";
        }

        @Override
        public List<String> filterRecoveredCommittables(List<String> globalCommittables) {
            if (committedSuccessData == null) {
                return globalCommittables;
            }
            return globalCommittables.stream()
                    .filter(s -> !s.equals(committedSuccessData))
                    .collect(Collectors.toList());
        }

        @Override
        public String combine(List<String> committables) {
            return COMBINER.apply(committables);
        }

        @Override
        public void endOfInput() {
            commit(Collections.singletonList(END_OF_INPUT_STR));
        }
    }
}
