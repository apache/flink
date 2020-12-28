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

package org.apache.flink.api.connector.source.lib;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.fail;

/** Tests for the {@link NumberSequenceSource}. */
public class NumberSequenceSourceTest {

    @Test
    public void testReaderCheckpoints() throws Exception {
        final long from = 177;
        final long mid = 333;
        final long to = 563;
        final long elementsPerCycle = (to - from) / 3;

        final TestingReaderOutput<Long> out = new TestingReaderOutput<>();

        SourceReader<Long, NumberSequenceSource.NumberSequenceSplit> reader = createReader();
        reader.addSplits(
                Arrays.asList(
                        new NumberSequenceSource.NumberSequenceSplit("split-1", from, mid),
                        new NumberSequenceSource.NumberSequenceSplit("split-2", mid + 1, to)));

        long remainingInCycle = elementsPerCycle;
        while (reader.pollNext(out) != InputStatus.END_OF_INPUT) {
            if (--remainingInCycle <= 0) {
                remainingInCycle = elementsPerCycle;
                // checkpoint
                List<NumberSequenceSource.NumberSequenceSplit> splits = reader.snapshotState(1L);

                // re-create and restore
                reader = createReader();
                reader.addSplits(splits);
            }
        }

        final List<Long> result = out.getEmittedRecords();
        validateSequence(result, from, to);
    }

    private static void validateSequence(
            final List<Long> sequence, final long from, final long to) {
        if (sequence.size() != to - from + 1) {
            failSequence(sequence, from, to);
        }

        long nextExpected = from;
        for (Long next : sequence) {
            if (next != nextExpected++) {
                failSequence(sequence, from, to);
            }
        }
    }

    private static void failSequence(final List<Long> sequence, final long from, final long to) {
        fail(
                String.format(
                        "Expected: A sequence [%d, %d], but found: sequence (size %d) : %s",
                        from, to, sequence.size(), sequence));
    }

    private static SourceReader<Long, NumberSequenceSource.NumberSequenceSplit> createReader() {
        // the arguments passed in the source constructor matter only to the enumerator
        return new NumberSequenceSource(0L, 0L).createReader(new DummyReaderContext());
    }

    // ------------------------------------------------------------------------
    //  test utils / mocks
    //
    //  the "flink-connector-test-utils module has proper mocks and utils,
    //  but cannot be used here, because it would create a cyclic dependency.
    // ------------------------------------------------------------------------

    private static final class DummyReaderContext implements SourceReaderContext {

        @Override
        public MetricGroup metricGroup() {
            return new UnregisteredMetricsGroup();
        }

        @Override
        public Configuration getConfiguration() {
            return new Configuration();
        }

        @Override
        public String getLocalHostName() {
            return "localhost";
        }

        @Override
        public int getIndexOfSubtask() {
            return 0;
        }

        @Override
        public void sendSplitRequest() {}

        @Override
        public void sendSourceEventToCoordinator(SourceEvent sourceEvent) {}
    }

    private static final class TestingReaderOutput<E> implements ReaderOutput<E> {

        private final ArrayList<E> emittedRecords = new ArrayList<>();

        @Override
        public void collect(E record) {
            emittedRecords.add(record);
        }

        @Override
        public void collect(E record, long timestamp) {
            collect(record);
        }

        @Override
        public void emitWatermark(Watermark watermark) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void markIdle() {
            throw new UnsupportedOperationException();
        }

        @Override
        public SourceOutput<E> createOutputForSplit(String splitId) {
            return this;
        }

        @Override
        public void releaseOutputForSplit(String splitId) {}

        public ArrayList<E> getEmittedRecords() {
            return emittedRecords;
        }
    }
}
