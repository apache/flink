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

package org.apache.flink.streaming.api.operators.sort;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.LongComparator;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArrayComparator;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.runtime.operators.sort.LargeRecordHandler;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.MutableObjectIterator;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Regression tests for extracting only sorting keys, not opaque record values. */
class ByteKeyComparatorLargeRecordTest {

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testExtractedKeys(boolean fixedLength) {
        final TypeComparator<Tuple2<byte[], StreamRecord<OpaquePayload>>> comparator =
                comparator(fixedLength);
        final List<Tuple2<byte[], StreamRecord<OpaquePayload>>> records = records(fixedLength);
        final TypeComparator<?>[] flatComparators = comparator.getFlatComparators();
        assertThat(flatComparators).hasSize(2);
        for (Tuple2<byte[], StreamRecord<OpaquePayload>> record : records) {
            final Object sentinel = new Object();
            final Object[] keys = {sentinel, null, null, sentinel};
            assertThat(comparator.extractKeys(record, keys, 1)).isEqualTo(2);
            assertThat(keys)
                    .containsExactly(sentinel, record.f0, record.f1.getTimestamp(), sentinel);
        }
        for (int i = 0; i < records.size(); i++) {
            for (int j = 0; j < records.size(); j++) {
                final Object[] first = new Object[2];
                final Object[] second = new Object[2];
                comparator.extractKeys(records.get(i), first, 0);
                comparator.extractKeys(records.get(j), second, 0);
                int comparison =
                        ((BytePrimitiveArrayComparator) flatComparators[0])
                                .compare((byte[]) first[0], (byte[]) second[0]);
                if (comparison == 0) {
                    comparison =
                            ((LongComparator) flatComparators[1])
                                    .compare((Long) first[1], (Long) second[1]);
                }
                assertThat(Integer.signum(comparison)).isEqualTo(Integer.compare(i, j));
                assertThat(Integer.signum(comparator.compare(records.get(i), records.get(j))))
                        .isEqualTo(Integer.compare(i, j));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testLargeRecordSpillWithoutGenericTypes(boolean fixedLength) throws Exception {
        final ExecutionConfig config =
                new ExecutionConfig(new Configuration().set(PipelineOptions.GENERIC_TYPES, false));
        final MemoryManager memory =
                MemoryManagerBuilder.newBuilder()
                        .setMemorySize(24 * 4096)
                        .setPageSize(4096)
                        .build();
        final DummyInvokable owner = new DummyInvokable();
        final List<Tuple2<byte[], StreamRecord<OpaquePayload>>> records = records(fixedLength);
        try (IOManager io = new IOManagerAsync()) {
            final LargeRecordHandler<Tuple2<byte[], StreamRecord<OpaquePayload>>> handler =
                    new LargeRecordHandler<>(
                            new KeyAndValueSerializer<>(
                                    new PayloadSerializer(), fixedLength ? 2 : -1),
                            comparator(fixedLength),
                            io,
                            memory,
                            memory.allocatePages(owner, 6),
                            owner,
                            16,
                            config);
            try {
                for (int i = records.size() - 1; i >= 0; i--) {
                    handler.addRecord(records.get(i));
                }
                assertThat(handler.hasData()).isTrue();
                final MutableObjectIterator<Tuple2<byte[], StreamRecord<OpaquePayload>>> sorted =
                        handler.finishWriteAndSortKeys(memory.allocatePages(owner, 18));
                for (Tuple2<byte[], StreamRecord<OpaquePayload>> expected : records) {
                    final Tuple2<byte[], StreamRecord<OpaquePayload>> actual = sorted.next();
                    assertThat(actual).isNotNull();
                    assertThat(actual.f0).isEqualTo(expected.f0);
                    assertThat(actual.f1.getTimestamp()).isEqualTo(expected.f1.getTimestamp());
                    assertThat(actual.f1.getValue().bytes).isEqualTo(expected.f1.getValue().bytes);
                }
                assertThat(sorted.next()).isNull();
            } finally {
                handler.close();
            }
        } finally {
            final boolean empty = memory.verifyEmpty();
            memory.shutdown();
            assertThat(empty).isTrue();
        }
    }

    private static TypeComparator<Tuple2<byte[], StreamRecord<OpaquePayload>>> comparator(
            boolean fixedLength) {
        return fixedLength
                ? new FixedLengthByteKeyComparator<>(2)
                : new VariableLengthByteKeyComparator<>();
    }

    private static List<Tuple2<byte[], StreamRecord<OpaquePayload>>> records(boolean fixedLength) {
        final byte[][] keys =
                fixedLength
                        ? new byte[][] {{-128, 0}, {-1, 127}, {0, -128}, {0, 0}, {127, 127}}
                        : new byte[][] {{}, {-128}, {-128, 0}, {-1, 127}, {0}, {0, -128}, {127}};
        final List<Tuple2<byte[], StreamRecord<OpaquePayload>>> records = new ArrayList<>();
        for (byte[] key : keys) {
            for (long timestamp : new long[] {Long.MIN_VALUE, -1, 0, Long.MAX_VALUE}) {
                final byte[] bytes = new byte[64 * 1024];
                bytes[0] = (byte) records.size();
                records.add(
                        Tuple2.of(key, new StreamRecord<>(new OpaquePayload(bytes), timestamp)));
            }
        }
        return records;
    }

    private static final class OpaquePayload {
        private final byte[] bytes;

        private OpaquePayload(byte[] bytes) {
            this.bytes = bytes;
        }
    }

    private static final class PayloadSerializer extends TypeSerializerSingleton<OpaquePayload> {
        @Override
        public boolean isImmutableType() {
            return false;
        }

        @Override
        public OpaquePayload createInstance() {
            return new OpaquePayload(new byte[0]);
        }

        @Override
        public OpaquePayload copy(OpaquePayload from) {
            return new OpaquePayload(from.bytes.clone());
        }

        @Override
        public OpaquePayload copy(OpaquePayload from, OpaquePayload reuse) {
            return copy(from);
        }

        @Override
        public int getLength() {
            return -1;
        }

        @Override
        public void serialize(OpaquePayload record, DataOutputView target) throws IOException {
            BytePrimitiveArraySerializer.INSTANCE.serialize(record.bytes, target);
        }

        @Override
        public OpaquePayload deserialize(DataInputView source) throws IOException {
            return new OpaquePayload(BytePrimitiveArraySerializer.INSTANCE.deserialize(source));
        }

        @Override
        public OpaquePayload deserialize(OpaquePayload reuse, DataInputView source)
                throws IOException {
            return deserialize(source);
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            BytePrimitiveArraySerializer.INSTANCE.copy(source, target);
        }

        @Override
        public TypeSerializerSnapshot<OpaquePayload> snapshotConfiguration() {
            throw new UnsupportedOperationException("Only used for spilling in this test.");
        }
    }
}
