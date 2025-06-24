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
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.api.connector.sink2.SupportsCommitter;
import org.apache.flink.api.connector.sink2.SupportsWriterState;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializerAdapter;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessageTypeInfo;
import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.connector.sink2.StandardSinkTopologies;
import org.apache.flink.streaming.api.connector.sink2.SupportsPostCommitTopology;
import org.apache.flink.streaming.api.connector.sink2.SupportsPreCommitTopology;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.function.SerializableFunction;
import org.apache.flink.util.function.SerializableSupplier;

import org.apache.flink.shaded.guava33.com.google.common.collect.ImmutableSet;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A {@link Sink} for all the sink related tests. */
public class TestSinkV2<InputT> implements Sink<InputT> {
    public static final SimpleVersionedSerializer<Integer> WRITER_SERIALIZER =
            new SimpleVersionedSerializerAdapter<>(IntSerializer.INSTANCE);

    private final SinkWriter<InputT> writer;

    private TestSinkV2(SinkWriter<InputT> writer) {
        this.writer = writer;
    }

    public SinkWriter<InputT> createWriter(WriterInitContext context) {
        if (writer instanceof DefaultSinkWriter) {
            ((DefaultSinkWriter<InputT>) writer).init(context);
        }
        return writer;
    }

    SinkWriter<InputT> getWriter() {
        return writer;
    }

    public static <InputT> Builder<InputT, Record<InputT>> newBuilder() {
        return new Builder<>();
    }

    public static <InputT> Builder<InputT, Record<InputT>> newBuilder(
            DefaultSinkWriter<InputT> writer) {
        return new Builder<InputT, Record<InputT>>().setWriter(writer);
    }

    /** A builder class for {@link TestSinkV2}. */
    public static class Builder<InputT, CommT> {
        private SinkWriter<InputT> writer = null;
        private Committer<CommT> committer;
        private boolean withPostCommitTopology = false;
        private SerializableFunction<CommT, CommT> preCommitTopology = null;
        private boolean withWriterState = false;
        private String compatibleStateNames;
        private SerializableSupplier<SimpleVersionedSerializer<CommT>> commSerializerFactory;

        @SuppressWarnings("unchecked")
        public <NewInputT, NewCommT> Builder<NewInputT, NewCommT> setWriter(
                CommittingSinkWriter<NewInputT, NewCommT> writer) {
            Builder<NewInputT, NewCommT> self = (Builder<NewInputT, NewCommT>) this;
            self.writer = checkNotNull(writer);
            return self;
        }

        @SuppressWarnings("unchecked")
        public <NewInputT> Builder<NewInputT, CommT> setWriter(SinkWriter<NewInputT> writer) {
            Builder<NewInputT, CommT> self = (Builder<NewInputT, CommT>) this;
            self.writer = checkNotNull(writer);
            return self;
        }

        public Builder<InputT, CommT> setCommitter(
                Committer<CommT> committer,
                SerializableSupplier<SimpleVersionedSerializer<CommT>> commSerializerFactory) {
            this.committer = committer;
            this.commSerializerFactory = commSerializerFactory;
            return this;
        }

        public Builder<InputT, CommT> setWithPostCommitTopology(boolean withPostCommitTopology) {
            this.withPostCommitTopology = withPostCommitTopology;
            return this;
        }

        public Builder<InputT, CommT> setWithPreCommitTopology(
                SerializableFunction<CommT, CommT> preCommitTopology) {
            this.preCommitTopology = preCommitTopology;
            return this;
        }

        public Builder<InputT, CommT> setWriterState(boolean withWriterState) {
            this.withWriterState = withWriterState;
            return this;
        }

        public Builder<InputT, CommT> setCompatibleStateNames(String compatibleStateNames) {
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
                    if (preCommitTopology == null) {
                        // TwoPhaseCommittingSink with a stateless writer and a committer
                        return new TestSinkV2TwoPhaseCommittingSink<>(
                                writer, commSerializerFactory, committer);
                    } else {
                        // TwoPhaseCommittingSink with a stateless writer, pre commit topology,
                        // committer
                        return new TestSinkV2WithPreCommitTopology<>(
                                writer, commSerializerFactory, committer, preCommitTopology);
                    }
                } else {
                    if (withWriterState) {
                        // TwoPhaseCommittingSink with a stateful writer and a committer and post
                        // commit topology
                        return new TestStatefulSinkV2<>(
                                writer, commSerializerFactory, committer, compatibleStateNames);
                    } else {
                        // TwoPhaseCommittingSink with a stateless writer and a committer and post
                        // commit topology
                        return new TestSinkV2WithPostCommitTopology<>(
                                writer, commSerializerFactory, committer);
                    }
                }
            }
        }
    }

    private static class TestSinkV2TwoPhaseCommittingSink<InputT, CommT> extends TestSinkV2<InputT>
            implements SupportsCommitter<CommT> {
        private final Committer<CommT> committer;
        protected final SerializableSupplier<SimpleVersionedSerializer<CommT>>
                commSerializerFactory;

        public TestSinkV2TwoPhaseCommittingSink(
                SinkWriter<InputT> writer,
                SerializableSupplier<SimpleVersionedSerializer<CommT>> commSerializerFactory,
                Committer<CommT> committer) {
            super(writer);
            this.committer = committer;
            this.commSerializerFactory = commSerializerFactory;
        }

        @Override
        public Committer<CommT> createCommitter(CommitterInitContext context) {
            if (committer instanceof DefaultCommitter) {
                ((DefaultCommitter<CommT>) committer).init();
            }
            return committer;
        }

        @Override
        public SimpleVersionedSerializer<CommT> getCommittableSerializer() {
            return commSerializerFactory.get();
        }
    }

    // -------------------------------------- Sink With PostCommitTopology -------------------------

    private static class TestSinkV2WithPostCommitTopology<InputT, CommT>
            extends TestSinkV2TwoPhaseCommittingSink<InputT, CommT>
            implements SupportsPostCommitTopology<CommT> {
        public TestSinkV2WithPostCommitTopology(
                SinkWriter<InputT> writer,
                SerializableSupplier<SimpleVersionedSerializer<CommT>> commSerializerFactory,
                Committer<CommT> committer) {
            super(writer, commSerializerFactory, committer);
        }

        @Override
        public void addPostCommitTopology(DataStream<CommittableMessage<CommT>> committables) {
            StandardSinkTopologies.addGlobalCommitter(
                    committables, this::createCommitter, this::getCommittableSerializer);
        }
    }

    private static class TestSinkV2WithPreCommitTopology<InputT, CommT>
            extends TestSinkV2TwoPhaseCommittingSink<InputT, CommT>
            implements SupportsPreCommitTopology<CommT, CommT> {
        private final SerializableFunction<CommT, CommT> preCommitTopology;

        public TestSinkV2WithPreCommitTopology(
                SinkWriter<InputT> writer,
                SerializableSupplier<SimpleVersionedSerializer<CommT>> commSerializerFactory,
                Committer<CommT> committer,
                SerializableFunction<CommT, CommT> preCommitTopology) {
            super(writer, commSerializerFactory, committer);
            this.preCommitTopology = preCommitTopology;
        }

        @Override
        public DataStream<CommittableMessage<CommT>> addPreCommitTopology(
                DataStream<CommittableMessage<CommT>> committables) {
            return committables
                    .map(
                            m -> {
                                if (m instanceof CommittableSummary) {
                                    return m;
                                } else {
                                    CommittableWithLineage<CommT> withLineage =
                                            (CommittableWithLineage<CommT>) m;
                                    return withLineage.map(preCommitTopology);
                                }
                            })
                    .returns(CommittableMessageTypeInfo.of(commSerializerFactory));
        }

        @Override
        public SimpleVersionedSerializer<CommT> getWriteResultSerializer() {
            return commSerializerFactory.get();
        }
    }

    private static class TestStatefulSinkV2<InputT, CommT>
            extends TestSinkV2WithPostCommitTopology<InputT, CommT>
            implements SupportsWriterState<InputT, Integer>,
                    SupportsWriterState.WithCompatibleState {
        private final String compatibleState;

        public TestStatefulSinkV2(
                SinkWriter<InputT> writer,
                SerializableSupplier<SimpleVersionedSerializer<CommT>> commSerializerFactory,
                Committer<CommT> committer,
                String compatibleState) {
            super(writer, commSerializerFactory, committer);
            this.compatibleState = compatibleState;
        }

        @Override
        public DefaultStatefulSinkWriter<InputT> createWriter(WriterInitContext context) {
            return (DefaultStatefulSinkWriter<InputT>) super.createWriter(context);
        }

        @Override
        public StatefulSinkWriter<InputT, Integer> restoreWriter(
                WriterInitContext context, Collection<Integer> recoveredState) {
            DefaultStatefulSinkWriter<InputT> statefulWriter =
                    (DefaultStatefulSinkWriter<InputT>) getWriter();

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

    public static class Record<T> implements Serializable {
        private final T value;
        private final Long timestamp;
        private final long watermark;

        public Record(T value, Long timestamp, long watermark) {
            this.value = value;
            this.timestamp = timestamp;
            this.watermark = watermark;
        }

        public T getValue() {
            return value;
        }

        public Long getTimestamp() {
            return timestamp;
        }

        public long getWatermark() {
            return watermark;
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (object == null || getClass() != object.getClass()) {
                return false;
            }
            Record<?> record = (Record<?>) object;
            return Objects.equals(timestamp, record.timestamp)
                    && watermark == record.watermark
                    && Objects.equals(value, record.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(value, timestamp, watermark);
        }

        @Override
        public String toString() {
            return "Record{"
                    + "value="
                    + value
                    + ", timestamp="
                    + timestamp
                    + ", watermark="
                    + watermark
                    + '}';
        }

        public Record<T> withValue(T value) {
            return new Record<>(value, timestamp, watermark);
        }
    }

    public static class RecordSerializer<T> implements SimpleVersionedSerializer<Record<T>> {
        @Override
        public int getVersion() {
            return 0;
        }

        @Override
        public byte[] serialize(Record<T> record) throws IOException {
            return InstantiationUtil.serializeObject(record);
        }

        @Override
        public Record<T> deserialize(int version, byte[] serialized) throws IOException {
            try {
                return InstantiationUtil.deserializeObject(serialized, getClass().getClassLoader());
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /** Base class for out testing {@link SinkWriter}. */
    public static class DefaultSinkWriter<InputT> implements SinkWriter<InputT>, Serializable {

        protected List<Record<InputT>> elements;

        protected List<Watermark> watermarks;

        public long lastCheckpointId = -1;

        protected DefaultSinkWriter() {
            this.elements = new ArrayList<>();
            this.watermarks = new ArrayList<>();
        }

        @Override
        public void write(InputT element, Context context) {
            elements.add(new Record<>(element, context.timestamp(), context.currentWatermark()));
        }

        @Override
        public void flush(boolean endOfInput) throws IOException, InterruptedException {
            elements = new ArrayList<>();
        }

        public List<Record<InputT>> getRecordsOfCurrentCheckpoint() {
            return elements;
        }

        public List<Watermark> getWatermarks() {
            return watermarks;
        }

        public long getLastCheckpointId() {
            return lastCheckpointId;
        }

        @Override
        public void writeWatermark(Watermark watermark) {
            watermarks.add(watermark);
        }

        @Override
        public void close() throws Exception {
            // noting to do here
        }

        public void init(WriterInitContext context) {
            // context is not used in default case
        }
    }

    /** Base class for out testing {@link CommittingSinkWriter}. */
    protected static class DefaultCommittingSinkWriter<InputT> extends DefaultSinkWriter<InputT>
            implements CommittingSinkWriter<InputT, Record<InputT>>, Serializable {

        @Override
        public void flush(boolean endOfInput) throws IOException, InterruptedException {
            // We empty the elements on prepareCommit
        }

        @Override
        public Collection<Record<InputT>> prepareCommit() {
            List<Record<InputT>> result = elements;
            elements = new ArrayList<>();
            return result;
        }
    }

    protected static class ForwardCommittingSinkWriter<InputT> extends DefaultSinkWriter<InputT>
            implements CommittingSinkWriter<InputT, InputT>, Serializable {

        @Override
        public void flush(boolean endOfInput) throws IOException, InterruptedException {
            // We empty the elements on prepareCommit
        }

        @Override
        public Collection<InputT> prepareCommit() {
            List<InputT> result =
                    elements.stream().map(Record::getValue).collect(Collectors.toList());
            elements = new ArrayList<>();
            return result;
        }
    }

    /**
     * Base class for out testing {@link StatefulSinkWriter}. Extends the {@link
     * DefaultCommittingSinkWriter} for simplicity.
     */
    protected static class DefaultStatefulSinkWriter<InputT>
            extends DefaultCommittingSinkWriter<InputT>
            implements StatefulSinkWriter<InputT, Integer> {
        private int recordCount = 0;

        @Override
        public void write(InputT element, Context context) {
            super.write(element, context);
            recordCount++;
        }

        public int getRecordCount() {
            return recordCount;
        }

        @Override
        public List<Integer> snapshotState(long checkpointId) throws IOException {
            lastCheckpointId = checkpointId;
            return Collections.singletonList(recordCount);
        }

        protected void restore(Collection<Integer> recoveredState) {
            this.recordCount = recoveredState.isEmpty() ? 0 : recoveredState.iterator().next();
        }
    }

    // -------------------------------------- Sink Committer ---------------------------------------

    /** Base class for testing {@link Committer}. */
    public static class DefaultCommitter<CommT> implements Committer<CommT>, Serializable {
        private boolean isClosed;

        public DefaultCommitter() {
            this.isClosed = false;
        }

        @Override
        public void commit(Collection<CommitRequest<CommT>> committables) {}

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
    static class RetryOnceCommitter<CommT> extends DefaultCommitter<CommT> {

        private final Set<CommT> seen = new LinkedHashSet<>();

        @Override
        public void commit(Collection<CommitRequest<CommT>> committables) {
            committables.forEach(
                    c -> {
                        if (!seen.remove(c.getCommittable())) {
                            seen.add(c.getCommittable());
                            c.retryLater();
                        }
                    });
        }
    }
}
