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

package org.apache.flink.streaming.runtime.operators.sink.deprecated;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializerAdapter;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.WithPostCommitTopology;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava32.com.google.common.collect.ImmutableSet;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A {@link org.apache.flink.api.connector.sink2.Sink} for all the sink related tests.
 *
 * <p>Should be removed along with {@link
 * org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink}.
 */
@Deprecated
public class TestSinkV2<InputT> implements Sink<InputT> {

    public static final SimpleVersionedSerializerAdapter<String> COMMITTABLE_SERIALIZER =
            org.apache.flink.streaming.runtime.operators.sink.TestSinkV2.COMMITTABLE_SERIALIZER;
    public static final SimpleVersionedSerializerAdapter<Integer> WRITER_SERIALIZER =
            org.apache.flink.streaming.runtime.operators.sink.TestSinkV2.WRITER_SERIALIZER;
    private final DefaultSinkWriter<InputT> writer;

    private TestSinkV2(DefaultSinkWriter<InputT> writer) {
        this.writer = writer;
    }

    public SinkWriter<InputT> createWriter(InitContext context) {
        writer.init(context);
        return writer;
    }

    public DefaultSinkWriter<InputT> getWriter() {
        return writer;
    }

    public static <InputT> Builder<InputT> newBuilder() {
        return new Builder<>();
    }

    /** A builder class for {@link TestSinkV2}. */
    public static class Builder<InputT> {
        private DefaultSinkWriter<InputT> writer = null;
        private DefaultCommitter committer;
        private boolean withPostCommitTopology = false;
        private boolean withWriterState = false;
        private String compatibleStateNames;

        public Builder<InputT> setWriter(DefaultSinkWriter<InputT> writer) {
            this.writer = checkNotNull(writer);
            return this;
        }

        public Builder<InputT> setCommitter(DefaultCommitter committer) {
            this.committer = committer;
            return this;
        }

        public Builder<InputT> setDefaultCommitter() {
            this.committer = new DefaultCommitter();
            return this;
        }

        public Builder<InputT> setDefaultCommitter(
                Supplier<Queue<Committer.CommitRequest<String>>> queueSupplier) {
            this.committer = new DefaultCommitter(queueSupplier);
            return this;
        }

        public Builder<InputT> setWithPostCommitTopology(boolean withPostCommitTopology) {
            this.withPostCommitTopology = withPostCommitTopology;
            return this;
        }

        public Builder<InputT> setWriterState(boolean withWriterState) {
            this.withWriterState = withWriterState;
            return this;
        }

        public Builder<InputT> setCompatibleStateNames(String compatibleStateNames) {
            this.compatibleStateNames = compatibleStateNames;
            return this;
        }

        public TestSinkV2<InputT> build() {
            if (committer == null) {
                if (writer == null) {
                    writer = new DefaultSinkWriter<>();
                }
                // SinkV2 with a simple writer
                return new TestSinkV2<>(writer);
            } else {
                if (writer == null) {
                    writer = new DefaultCommittingSinkWriter<>();
                }
                if (!withPostCommitTopology) {
                    // TwoPhaseCommittingSink with a stateless writer and a committer
                    return new TestSinkV2TwoPhaseCommittingSink<>(
                            writer, COMMITTABLE_SERIALIZER, committer);
                } else {
                    if (withWriterState) {
                        // TwoPhaseCommittingSink with a stateful writer and a committer and post
                        // commit topology
                        Preconditions.checkArgument(
                                writer instanceof DefaultStatefulSinkWriter,
                                "Please provide a DefaultStatefulSinkWriter instance");
                        return new TestStatefulSinkV2(
                                (DefaultStatefulSinkWriter) writer,
                                COMMITTABLE_SERIALIZER,
                                committer,
                                compatibleStateNames);
                    } else {
                        // TwoPhaseCommittingSink with a stateless writer and a committer and post
                        // commit topology
                        Preconditions.checkArgument(
                                writer instanceof DefaultCommittingSinkWriter,
                                "Please provide a DefaultCommittingSinkWriter instance");
                        return new TestSinkV2WithPostCommitTopology<>(
                                (DefaultCommittingSinkWriter) writer,
                                COMMITTABLE_SERIALIZER,
                                committer);
                    }
                }
            }
        }
    }

    private static class TestSinkV2TwoPhaseCommittingSink<InputT> extends TestSinkV2<InputT>
            implements TwoPhaseCommittingSink<InputT, String> {
        private final DefaultCommitter committer;
        private final SimpleVersionedSerializer<String> committableSerializer;

        public TestSinkV2TwoPhaseCommittingSink(
                DefaultSinkWriter<InputT> writer,
                SimpleVersionedSerializer<String> committableSerializer,
                DefaultCommitter committer) {
            super(writer);
            this.committer = committer;
            this.committableSerializer = committableSerializer;
        }

        @Override
        public Committer<String> createCommitter() {
            committer.init();
            return committer;
        }

        @Override
        public SimpleVersionedSerializer<String> getCommittableSerializer() {
            return committableSerializer;
        }

        @Override
        public PrecommittingSinkWriter<InputT, String> createWriter(InitContext context) {
            return (PrecommittingSinkWriter<InputT, String>) super.createWriter(context);
        }
    }

    // -------------------------------------- Sink With PostCommitTopology -------------------------

    private static class TestSinkV2WithPostCommitTopology<InputT>
            extends TestSinkV2TwoPhaseCommittingSink<InputT>
            implements WithPostCommitTopology<InputT, String> {
        public TestSinkV2WithPostCommitTopology(
                DefaultSinkWriter<InputT> writer,
                SimpleVersionedSerializer<String> committableSerializer,
                DefaultCommitter committer) {
            super(writer, committableSerializer, committer);
        }

        @Override
        public void addPostCommitTopology(DataStream<CommittableMessage<String>> committables) {
            // We do not need to do anything for tests
        }
    }

    private static class TestStatefulSinkV2<InputT> extends TestSinkV2WithPostCommitTopology<InputT>
            implements StatefulSink<InputT, Integer>, StatefulSink.WithCompatibleState {
        private String compatibleState;

        public TestStatefulSinkV2(
                DefaultStatefulSinkWriter<InputT> writer,
                SimpleVersionedSerializer<String> committableSerializer,
                DefaultCommitter committer,
                String compatibleState) {
            super(writer, committableSerializer, committer);
            this.compatibleState = compatibleState;
        }

        @Override
        public DefaultStatefulSinkWriter<InputT> createWriter(InitContext context) {
            return (DefaultStatefulSinkWriter<InputT>) super.createWriter(context);
        }

        @Override
        public StatefulSinkWriter<InputT, Integer> restoreWriter(
                InitContext context, Collection<Integer> recoveredState) {
            DefaultStatefulSinkWriter<InputT> statefulWriter =
                    (DefaultStatefulSinkWriter) getWriter();

            statefulWriter.restore(recoveredState);
            return statefulWriter;
        }

        @Override
        public SimpleVersionedSerializer<Integer> getWriterStateSerializer() {
            return WRITER_SERIALIZER;
        }

        @Override
        public Collection<String> getCompatibleWriterStateNames() {
            return compatibleState == null ? ImmutableSet.of() : ImmutableSet.of(compatibleState);
        }
    }

    // -------------------------------------- Sink Writer ------------------------------------------

    /** Base class for out testing {@link SinkWriter}. */
    public static class DefaultSinkWriter<InputT> implements SinkWriter<InputT>, Serializable {

        public List<String> elements;

        public List<Watermark> watermarks;

        public long lastCheckpointId = -1;

        public DefaultSinkWriter() {
            this.elements = new ArrayList<>();
            this.watermarks = new ArrayList<>();
        }

        @Override
        public void write(InputT element, Context context) {
            elements.add(
                    Tuple3.of(element, context.timestamp(), context.currentWatermark()).toString());
        }

        @Override
        public void flush(boolean endOfInput) throws IOException, InterruptedException {
            elements = new ArrayList<>();
        }

        @Override
        public void writeWatermark(Watermark watermark) {
            watermarks.add(watermark);
        }

        @Override
        public void close() throws Exception {
            // noting to do here
        }

        public void init(InitContext context) {
            // context is not used in default case
        }
    }

    /** Base class for out testing {@link TwoPhaseCommittingSink.PrecommittingSinkWriter}. */
    public static class DefaultCommittingSinkWriter<InputT> extends DefaultSinkWriter<InputT>
            implements TwoPhaseCommittingSink.PrecommittingSinkWriter<InputT, String>,
                    Serializable {

        @Override
        public void flush(boolean endOfInput) throws IOException, InterruptedException {
            // We empty the elements on prepareCommit
        }

        @Override
        public Collection<String> prepareCommit() {
            List<String> result = elements;
            elements = new ArrayList<>();
            return result;
        }
    }

    /**
     * Base class for out testing {@link StatefulSink.StatefulSinkWriter}. Extends the {@link
     * DefaultCommittingSinkWriter} for simplicity.
     */
    public static class DefaultStatefulSinkWriter<InputT>
            extends DefaultCommittingSinkWriter<InputT>
            implements StatefulSink.StatefulSinkWriter<InputT, Integer> {
        private int recordCount;

        @Override
        public List<Integer> snapshotState(long checkpointId) throws IOException {
            lastCheckpointId = checkpointId;
            return Collections.singletonList(recordCount);
        }

        @Override
        public void write(InputT element, Context context) {
            super.write(element, context);
            recordCount++;
        }

        public int getRecordCount() {
            return recordCount;
        }

        protected void restore(Collection<Integer> recoveredState) {
            this.recordCount = recoveredState.isEmpty() ? 0 : recoveredState.iterator().next();
        }
    }

    // -------------------------------------- Sink Committer ---------------------------------------

    /** Base class for testing {@link Committer}. */
    public static class DefaultCommitter implements Committer<String>, Serializable {

        @Nullable protected Queue<CommitRequest<String>> committedData;

        private boolean isClosed;

        @Nullable private final Supplier<Queue<CommitRequest<String>>> queueSupplier;

        public DefaultCommitter() {
            this.committedData = new ConcurrentLinkedQueue<>();
            this.isClosed = false;
            this.queueSupplier = null;
        }

        public DefaultCommitter(@Nullable Supplier<Queue<CommitRequest<String>>> queueSupplier) {
            this.queueSupplier = queueSupplier;
            this.isClosed = false;
            this.committedData = null;
        }

        public List<CommitRequest<String>> getCommittedData() {
            if (committedData != null) {
                return new ArrayList<>(committedData);
            } else {
                return Collections.emptyList();
            }
        }

        @Override
        public void commit(Collection<CommitRequest<String>> committables) {
            if (committedData == null) {
                assertThat(queueSupplier).isNotNull();
                committedData = queueSupplier.get();
            }
            committedData.addAll(committables);
        }

        public void close() throws Exception {
            isClosed = true;
        }

        public boolean isClosed() {
            return isClosed;
        }

        public void init() {
            // context is not used for this implementation
        }
    }

    /** A {@link Committer} that always re-commits the committables data it received. */
    public static class RetryOnceCommitter extends DefaultCommitter {

        private final Set<CommitRequest<String>> seen = new LinkedHashSet<>();

        @Override
        public void commit(Collection<CommitRequest<String>> committables) {
            committables.forEach(
                    c -> {
                        if (seen.remove(c)) {
                            checkNotNull(committedData);
                            committedData.add(c);
                        } else {
                            seen.add(c);
                            c.retryLater();
                        }
                    });
        }
    }
}
