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

package org.apache.flink.runtime.operators.util;

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.base.IntComparator;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.api.writer.ChannelSelector;
import org.apache.flink.runtime.operators.shipping.OutputEmitter;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.runtime.testutils.recordutils.RecordComparatorFactory;
import org.apache.flink.runtime.testutils.recordutils.RecordSerializerFactory;
import org.apache.flink.types.DeserializationException;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.NullKeyFieldException;
import org.apache.flink.types.Record;
import org.apache.flink.types.StringValue;
import org.apache.flink.types.Value;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SuppressWarnings({"unchecked", "rawtypes"})
class OutputEmitterTest {

    @Test
    void testPartitionHash() {
        // Test for IntValue
        verifyPartitionHashSelectedChannels(50000, 100, RecordType.INTEGER);
        // Test for StringValue
        verifyPartitionHashSelectedChannels(10000, 100, RecordType.STRING);

        // Test hash corner cases
        final TestIntComparator testIntComp = new TestIntComparator();
        final ChannelSelector<SerializationDelegate<Integer>> selector =
                createChannelSelector(ShipStrategyType.PARTITION_HASH, testIntComp, 100);
        final SerializationDelegate<Integer> serializationDelegate =
                new SerializationDelegate<>(new IntSerializer());

        assertPartitionHashSelectedChannels(
                selector, serializationDelegate, Integer.MIN_VALUE, 100);
        assertPartitionHashSelectedChannels(selector, serializationDelegate, -1, 100);
        assertPartitionHashSelectedChannels(selector, serializationDelegate, 0, 100);
        assertPartitionHashSelectedChannels(selector, serializationDelegate, 1, 100);
        assertPartitionHashSelectedChannels(
                selector, serializationDelegate, Integer.MAX_VALUE, 100);
    }

    @Test
    void testForward() {
        final int numberOfChannels = 100;

        // Test for IntValue
        int numRecords = 50000 + numberOfChannels / 2;
        verifyForwardSelectedChannels(numRecords, numberOfChannels, RecordType.INTEGER);

        // Test for StringValue
        numRecords = 10000 + numberOfChannels / 2;
        verifyForwardSelectedChannels(numRecords, numberOfChannels, RecordType.STRING);
    }

    @Test
    void testForcedRebalance() {
        final int numberOfChannels = 100;
        int toTaskIndex = numberOfChannels * 6 / 7;
        int fromTaskIndex = toTaskIndex + numberOfChannels;
        int extraRecords = numberOfChannels / 3;
        int numRecords = 50000 + extraRecords;
        final SerializationDelegate<Record> delegate =
                new SerializationDelegate<>(new RecordSerializerFactory().getSerializer());
        final ChannelSelector<SerializationDelegate<Record>> selector =
                new OutputEmitter<>(ShipStrategyType.PARTITION_FORCED_REBALANCE, fromTaskIndex);
        selector.setup(numberOfChannels);

        // Test for IntValue
        int[] hits =
                getSelectedChannelsHitCount(
                        selector, delegate, RecordType.INTEGER, numRecords, numberOfChannels);
        int totalHitCount = 0;
        for (int i = 0; i < hits.length; i++) {
            if (toTaskIndex <= i || i < toTaskIndex + extraRecords - numberOfChannels) {
                assertThat(hits[i]).isEqualTo(numRecords / numberOfChannels + 1);
            } else {
                assertThat(hits[i]).isEqualTo(numRecords / numberOfChannels);
            }
            totalHitCount += hits[i];
        }
        assertThat(totalHitCount).isEqualTo(numRecords);

        toTaskIndex = numberOfChannels / 5;
        fromTaskIndex = toTaskIndex + 2 * numberOfChannels;
        extraRecords = numberOfChannels * 2 / 9;
        numRecords = 10000 + extraRecords;

        // Test for StringValue
        final ChannelSelector<SerializationDelegate<Record>> selector2 =
                new OutputEmitter<>(ShipStrategyType.PARTITION_FORCED_REBALANCE, fromTaskIndex);
        selector2.setup(numberOfChannels);
        hits =
                getSelectedChannelsHitCount(
                        selector2, delegate, RecordType.STRING, numRecords, numberOfChannels);
        totalHitCount = 0;
        for (int i = 0; i < hits.length; i++) {
            if (toTaskIndex <= i && i < toTaskIndex + extraRecords) {
                assertThat(hits[i]).isEqualTo(numRecords / numberOfChannels + 1);
            } else {
                assertThat(hits[i]).isEqualTo(numRecords / numberOfChannels);
            }
            totalHitCount += hits[i];
        }
        assertThat(totalHitCount).isEqualTo(numRecords);
    }

    @Test
    void testBroadcast() {
        // Test for IntValue
        verifyBroadcastSelectedChannels(100, 50000, RecordType.INTEGER);
        // Test for StringValue
        verifyBroadcastSelectedChannels(100, 50000, RecordType.STRING);
    }

    @Test
    void testMultiKeys() {
        final int numberOfChannels = 100;
        final int numRecords = 5000;
        final TypeComparator<Record> multiComp =
                new RecordComparatorFactory(
                                new int[] {0, 1, 3},
                                new Class[] {IntValue.class, StringValue.class, DoubleValue.class})
                        .createComparator();

        final ChannelSelector<SerializationDelegate<Record>> selector =
                createChannelSelector(ShipStrategyType.PARTITION_HASH, multiComp, numberOfChannels);
        final SerializationDelegate<Record> delegate =
                new SerializationDelegate<>(new RecordSerializerFactory().getSerializer());

        int[] hits = new int[numberOfChannels];
        for (int i = 0; i < numRecords; i++) {
            Record record = new Record(4);
            record.setField(0, new IntValue(i));
            record.setField(1, new StringValue("AB" + i + "CD" + i));
            record.setField(3, new DoubleValue(i * 3.141d));
            delegate.setInstance(record);

            int channel = selector.selectChannel(delegate);
            hits[channel]++;
        }

        int totalHitCount = 0;
        for (int hit : hits) {
            assertThat(hit).isGreaterThan(0);
            totalHitCount += hit;
        }
        assertThat(totalHitCount).isEqualTo(numRecords);
    }

    @Test
    void testMissingKey() {
        assertThat(verifyWrongPartitionHashKey(1, 0))
                .withFailMessage("Expected a KeyFieldOutOfBoundsException.")
                .isTrue();
    }

    @Test
    void testNullKey() {
        assertThat(verifyWrongPartitionHashKey(0, 1))
                .withFailMessage("Expected a NullKeyFieldException.")
                .isTrue();
    }

    @Test
    void testWrongKeyClass() throws Exception {
        // Test for IntValue
        final TypeComparator<Record> doubleComp =
                new RecordComparatorFactory(new int[] {0}, new Class[] {DoubleValue.class})
                        .createComparator();
        final ChannelSelector<SerializationDelegate<Record>> selector =
                createChannelSelector(ShipStrategyType.PARTITION_HASH, doubleComp, 100);
        final SerializationDelegate<Record> delegate =
                new SerializationDelegate<>(new RecordSerializerFactory().getSerializer());

        PipedInputStream pipedInput = new PipedInputStream(1024 * 1024);
        DataInputView in = new DataInputViewStreamWrapper(pipedInput);
        DataOutputView out = new DataOutputViewStreamWrapper(new PipedOutputStream(pipedInput));

        Record record = new Record(1);
        record.setField(0, new IntValue());
        record.write(out);
        record = new Record();
        record.read(in);

        delegate.setInstance(record);
        assertThatThrownBy(() -> selector.selectChannel(delegate))
                .isInstanceOf(DeserializationException.class);
    }

    private void verifyPartitionHashSelectedChannels(
            int numRecords, int numberOfChannels, Enum recordType) {
        int[] hits =
                getSelectedChannelsHitCount(
                        ShipStrategyType.PARTITION_HASH, numRecords, numberOfChannels, recordType);

        int totalHitCount = 0;
        for (int hit : hits) {
            assertThat(hit).isGreaterThan(0);
            totalHitCount += hit;
        }
        assertThat(totalHitCount).isEqualTo(numRecords);
    }

    private void verifyForwardSelectedChannels(
            int numRecords, int numberOfChannels, Enum recordType) {
        int[] hits =
                getSelectedChannelsHitCount(
                        ShipStrategyType.FORWARD, numRecords, numberOfChannels, recordType);

        assertThat(hits[0]).isEqualTo(numRecords);
        for (int i = 1; i < hits.length; i++) {
            assertThat(hits[i]).isZero();
        }
    }

    private void verifyBroadcastSelectedChannels(
            int numRecords, int numberOfChannels, Enum recordType) {

        assertThatThrownBy(
                        () ->
                                getSelectedChannelsHitCount(
                                        ShipStrategyType.BROADCAST,
                                        numRecords,
                                        numberOfChannels,
                                        recordType))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    private boolean verifyWrongPartitionHashKey(int position, int fieldNum) {
        final TypeComparator<Record> comparator =
                new RecordComparatorFactory(new int[] {position}, new Class[] {IntValue.class})
                        .createComparator();
        final ChannelSelector<SerializationDelegate<Record>> selector =
                createChannelSelector(ShipStrategyType.PARTITION_HASH, comparator, 100);
        final SerializationDelegate<Record> delegate =
                new SerializationDelegate<>(new RecordSerializerFactory().getSerializer());

        Record record = new Record(2);
        record.setField(fieldNum, new IntValue(1));
        delegate.setInstance(record);

        try {
            selector.selectChannel(delegate);
        } catch (NullKeyFieldException re) {
            assertThat(re.getFieldNumber()).isEqualTo(position);
            return true;
        }
        return false;
    }

    private int[] getSelectedChannelsHitCount(
            ShipStrategyType shipStrategyType,
            int numRecords,
            int numberOfChannels,
            Enum recordType) {
        final TypeComparator<Record> comparator =
                new RecordComparatorFactory(
                                new int[] {0},
                                new Class[] {
                                    recordType == RecordType.INTEGER
                                            ? IntValue.class
                                            : StringValue.class
                                })
                        .createComparator();
        final ChannelSelector<SerializationDelegate<Record>> selector =
                createChannelSelector(shipStrategyType, comparator, numberOfChannels);
        final SerializationDelegate<Record> delegate =
                new SerializationDelegate<>(new RecordSerializerFactory().getSerializer());

        return getSelectedChannelsHitCount(
                selector, delegate, recordType, numRecords, numberOfChannels);
    }

    private ChannelSelector createChannelSelector(
            ShipStrategyType shipStrategyType, TypeComparator comparator, int numberOfChannels) {
        final ChannelSelector selector = new OutputEmitter<>(shipStrategyType, comparator);
        selector.setup(numberOfChannels);
        assertThat(shipStrategyType == ShipStrategyType.BROADCAST)
                .isEqualTo(selector.isBroadcast());
        return selector;
    }

    private int[] getSelectedChannelsHitCount(
            ChannelSelector<SerializationDelegate<Record>> selector,
            SerializationDelegate<Record> delegate,
            Enum recordType,
            int numRecords,
            int numberOfChannels) {
        int[] hits = new int[numberOfChannels];
        Value value;
        for (int i = 0; i < numRecords; i++) {
            if (recordType == RecordType.INTEGER) {
                value = new IntValue(i);
            } else {
                value = new StringValue(i + "");
            }
            Record record = new Record(value);
            delegate.setInstance(record);

            int channel = selector.selectChannel(delegate);
            hits[channel]++;
        }
        return hits;
    }

    private void assertPartitionHashSelectedChannels(
            ChannelSelector selector,
            SerializationDelegate<Integer> serializationDelegate,
            int record,
            int numberOfChannels) {
        serializationDelegate.setInstance(record);
        int selectedChannel = selector.selectChannel(serializationDelegate);

        assertThat(selectedChannel)
                .isGreaterThanOrEqualTo(0)
                .isLessThanOrEqualTo(numberOfChannels - 1);
    }

    private static class TestIntComparator extends TypeComparator<Integer> {
        private TypeComparator[] comparators = new TypeComparator[] {new IntComparator(true)};

        @Override
        public int hash(Integer record) {
            return record;
        }

        @Override
        public void setReference(Integer toCompare) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean equalToReference(Integer candidate) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int compareToReference(TypeComparator<Integer> referencedComparator) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int compare(Integer first, Integer second) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int compareSerialized(DataInputView firstSource, DataInputView secondSource) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean supportsNormalizedKey() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean supportsSerializationWithKeyNormalization() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getNormalizeKeyLen() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putNormalizedKey(
                Integer record, MemorySegment target, int offset, int numBytes) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeWithKeyNormalization(Integer record, DataOutputView target)
                throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public Integer readWithKeyDenormalization(Integer reuse, DataInputView source)
                throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean invertNormalizedKey() {
            throw new UnsupportedOperationException();
        }

        @Override
        public TypeComparator<Integer> duplicate() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int extractKeys(Object record, Object[] target, int index) {
            target[index] = record;
            return 1;
        }

        @Override
        public TypeComparator[] getFlatComparators() {
            return comparators;
        }
    }

    private enum RecordType {
        STRING,
        INTEGER
    }
}
