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

package org.apache.flink.connector.datagen.source;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.util.SimpleUserCodeClassLoader;
import org.apache.flink.util.UserCodeClassLoader;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link DataGeneratorSource}. */
class DataGeneratorSourceTest {

    @Test
    @DisplayName("Correctly restores SplitEnumerator from a snapshot.")
    void testRestoreEnumerator() throws Exception {
        final GeneratorFunction<Long, Long> generatorFunctionStateless = index -> index;
        final DataGeneratorSource<Long> dataGeneratorSource =
                new DataGeneratorSource<>(generatorFunctionStateless, 100, Types.LONG);

        final int parallelism = 2;
        final MockSplitEnumeratorContext<NumberSequenceSource.NumberSequenceSplit> context =
                new MockSplitEnumeratorContext<>(parallelism);

        SplitEnumerator<
                        NumberSequenceSource.NumberSequenceSplit,
                        Collection<NumberSequenceSource.NumberSequenceSplit>>
                enumerator = dataGeneratorSource.createEnumerator(context);

        // start() is not strictly necessary in the current implementation, but should logically be
        // executed in this order (protect against any breaking changes in the start() method).
        enumerator.start();

        Collection<NumberSequenceSource.NumberSequenceSplit> enumeratorState =
                enumerator.snapshotState(0);
        assertThat(enumeratorState).hasSize(parallelism);

        enumerator = dataGeneratorSource.restoreEnumerator(context, enumeratorState);

        // Verify that splits were restored and can be assigned
        assertThat(context.getSplitsAssignmentSequence()).isEmpty();
        for (NumberSequenceSource.NumberSequenceSplit ignored : enumeratorState) {
            enumerator.handleSplitRequest(0, "hostname");
        }
        assertThat(context.getSplitsAssignmentSequence()).hasSize(enumeratorState.size());
    }

    @Test
    @DisplayName("Uses the underlying NumberSequenceSource correctly for checkpointing.")
    void testReaderCheckpoints() throws Exception {
        final int numCycles = 3;
        final long from = 0;
        final long mid = 156;
        final long to = 383;
        final long elementsPerCycle = (to - from + 1) / numCycles;

        final TestingReaderOutput<Long> out = new TestingReaderOutput<>();

        SourceReader<Long, NumberSequenceSource.NumberSequenceSplit> reader = createReader();
        reader.addSplits(
                Arrays.asList(
                        new NumberSequenceSource.NumberSequenceSplit("split-1", from, mid),
                        new NumberSequenceSource.NumberSequenceSplit("split-2", mid + 1, to)));

        for (int cycle = 0; cycle < numCycles; cycle++) {
            // this call is not required but mimics what happens at runtime
            assertThat(reader.pollNext(out))
                    .as(
                            "Each poll should return a NOTHING_AVAILABLE status to explicitly trigger the availability check through in SourceReader.isAvailable")
                    .isSameAs(InputStatus.NOTHING_AVAILABLE);
            for (int elementInCycle = 0; elementInCycle < elementsPerCycle; elementInCycle++) {
                assertThat(reader.isAvailable())
                        .as(
                                "There should be always data available because the test utilizes no rate-limiting strategy and splits are provided.")
                        .isCompleted();
                // this never returns END_OF_INPUT because IteratorSourceReaderBase#pollNext does
                // not immediately return END_OF_INPUT when the input is exhausted
                assertThat(reader.pollNext(out))
                        .as(
                                "Each poll should return a NOTHING_AVAILABLE status to explicitly trigger the availability check through in SourceReader.isAvailable")
                        .isSameAs(InputStatus.NOTHING_AVAILABLE);
            }
            // checkpoint
            List<NumberSequenceSource.NumberSequenceSplit> splits = reader.snapshotState(1L);
            // first cycle partially consumes the first split
            // second cycle consumes the remaining first split and partially consumes the second
            // third cycle consumes remaining second split
            assertThat(splits).hasSize(numCycles - cycle - 1);

            // re-create and restore
            reader = createReader();
            if (splits.isEmpty()) {
                reader.notifyNoMoreSplits();
            } else {
                reader.addSplits(splits);
            }
        }

        // we need to go again through isAvailable because IteratorSourceReaderBase#pollNext does
        // not immediately return END_OF_INPUT when the input is exhausted
        assertThat(reader.isAvailable())
                .as(
                        "There should be always data available because the test utilizes no rate-limiting strategy and splits are provided.")
                .isCompleted();
        assertThat(reader.pollNext(out)).isSameAs(InputStatus.END_OF_INPUT);

        final List<Long> result = out.getEmittedRecords();
        final Iterable<Long> expected = LongStream.range(from, to + 1)::iterator;

        assertThat(result).containsExactlyElementsOf(expected);
    }

    private static SourceReader<Long, NumberSequenceSource.NumberSequenceSplit> createReader()
            throws Exception {
        // the arguments passed in the source constructor matter only to the enumerator
        GeneratorFunction<Long, Long> generatorFunctionStateless = index -> index;
        DataGeneratorSource<Long> dataGeneratorSource =
                new DataGeneratorSource<>(generatorFunctionStateless, Long.MAX_VALUE, Types.LONG);

        return dataGeneratorSource.createReader(new DummyReaderContext());
    }

    // ------------------------------------------------------------------------
    //  test utils / mocks
    //
    //  the "flink-connector-test-utils module has proper mocks and utils,
    //  but cannot be used here, because it would create a cyclic dependency.
    // ------------------------------------------------------------------------

    private static final class DummyReaderContext implements SourceReaderContext {

        @Override
        public SourceReaderMetricGroup metricGroup() {
            return UnregisteredMetricsGroup.createSourceReaderMetricGroup();
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

        @Override
        public UserCodeClassLoader getUserCodeClassLoader() {
            return SimpleUserCodeClassLoader.create(getClass().getClassLoader());
        }

        @Override
        public int currentParallelism() {
            return 1;
        }
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
        public void markActive() {
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
