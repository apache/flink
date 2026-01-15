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

package org.apache.flink.streaming.runtime.io.recovery;

import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.runtime.io.network.api.writer.ChannelSelector;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RecordFilter} interface and {@link PartitionerRecordFilter}. */
class RecordFilterTest {

    @Test
    void testAcceptAllFilterAcceptsEveryRecord() {
        RecordFilter<Long> filter = RecordFilter.acceptAll();
        assertThat(filter.filter(new StreamRecord<>(0L))).isTrue();
        assertThat(filter.filter(new StreamRecord<>(1L))).isTrue();
        assertThat(filter.filter(new StreamRecord<>(42L))).isTrue();
        assertThat(filter.filter(new StreamRecord<>(-1L))).isTrue();
    }

    @Test
    void testPartitionerRecordFilterAcceptsMatchingSubtask() {
        // Mod-based partitioner with 2 channels, subtask index 0 receives even values
        PartitionerRecordFilter<Long> filter =
                new PartitionerRecordFilter<>(
                        new ModChannelSelector(2), LongSerializer.INSTANCE, 0);

        assertThat(filter.filter(new StreamRecord<>(0L))).isTrue();
        assertThat(filter.filter(new StreamRecord<>(2L))).isTrue();
        assertThat(filter.filter(new StreamRecord<>(4L))).isTrue();
    }

    @Test
    void testPartitionerRecordFilterRejectsNonMatchingSubtask() {
        // Mod-based partitioner with 2 channels, subtask index 0 should reject odd values
        PartitionerRecordFilter<Long> filter =
                new PartitionerRecordFilter<>(
                        new ModChannelSelector(2), LongSerializer.INSTANCE, 0);

        assertThat(filter.filter(new StreamRecord<>(1L))).isFalse();
        assertThat(filter.filter(new StreamRecord<>(3L))).isFalse();
        assertThat(filter.filter(new StreamRecord<>(5L))).isFalse();
    }

    @Test
    void testPartitionerRecordFilterWithDifferentSubtaskIndex() {
        // Subtask index 1 should accept odd values (mod 2 == 1)
        PartitionerRecordFilter<Long> filter =
                new PartitionerRecordFilter<>(
                        new ModChannelSelector(2), LongSerializer.INSTANCE, 1);

        assertThat(filter.filter(new StreamRecord<>(1L))).isTrue();
        assertThat(filter.filter(new StreamRecord<>(3L))).isTrue();
        assertThat(filter.filter(new StreamRecord<>(0L))).isFalse();
        assertThat(filter.filter(new StreamRecord<>(2L))).isFalse();
    }

    @Test
    void testRecordFilterAsFunctionalInterface() {
        // RecordFilter is a FunctionalInterface and can be implemented as a lambda
        RecordFilter<Long> onlyPositive = record -> record.getValue() > 0;
        assertThat(onlyPositive.filter(new StreamRecord<>(5L))).isTrue();
        assertThat(onlyPositive.filter(new StreamRecord<>(-1L))).isFalse();
        assertThat(onlyPositive.filter(new StreamRecord<>(0L))).isFalse();
    }

    /** A simple mod-based channel selector for testing. */
    private static class ModChannelSelector
            implements ChannelSelector<SerializationDelegate<StreamRecord<Long>>> {
        private final int numberOfChannels;

        private ModChannelSelector(int numberOfChannels) {
            this.numberOfChannels = numberOfChannels;
        }

        @Override
        public void setup(int numberOfChannels) {}

        @Override
        public int selectChannel(SerializationDelegate<StreamRecord<Long>> record) {
            return (int) (record.getInstance().getValue() % numberOfChannels);
        }

        @Override
        public boolean isBroadcast() {
            return false;
        }
    }
}
