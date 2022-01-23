/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package org.apache.flink.connector.testutils.source.reader;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.util.TestLogger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * An abstract test class for all the unit tests of {@link SourceReader} to inherit.
 *
 * @param <SplitT> the type of the splits.
 */
public abstract class SourceReaderTestBase<SplitT extends SourceSplit> extends TestLogger {
    protected final int numSplits;
    protected final int totalNumRecords;
    protected static final int NUM_RECORDS_PER_SPLIT = 10;

    public SourceReaderTestBase() {
        this.numSplits = getNumSplits();
        this.totalNumRecords = this.numSplits * NUM_RECORDS_PER_SPLIT;
    }

    protected int getNumSplits() {
        return 10;
    }

    @AfterEach
    public void ensureNoDangling() {
        for (Thread t : Thread.getAllStackTraces().keySet()) {
            if (t.getName().equals("SourceFetcher")) {
                System.out.println("Dangling thread.");
            }
        }
    }

    /** Simply test the reader reads all the splits fine. */
    @Test
    void testRead() throws Exception {
        try (SourceReader<Integer, SplitT> reader = createReader()) {
            reader.addSplits(getSplits(numSplits, NUM_RECORDS_PER_SPLIT, Boundedness.BOUNDED));
            ValidatingSourceOutput output = new ValidatingSourceOutput();
            while (output.count < totalNumRecords) {
                reader.pollNext(output);
            }
            output.validate();
        }
    }

    @Test
    void testAddSplitToExistingFetcher() throws Exception {
        Thread.sleep(10);
        ValidatingSourceOutput output = new ValidatingSourceOutput();
        // Add a split to start the fetcher.
        List<SplitT> splits =
                Collections.singletonList(getSplit(0, NUM_RECORDS_PER_SPLIT, Boundedness.BOUNDED));
        // Poll 5 records and let it block on the element queue which only have capacity of 1;
        try (SourceReader<Integer, SplitT> reader = consumeRecords(splits, output, 5)) {
            List<SplitT> newSplits = new ArrayList<>();
            for (int i = 1; i < numSplits; i++) {
                newSplits.add(getSplit(i, NUM_RECORDS_PER_SPLIT, Boundedness.BOUNDED));
            }
            reader.addSplits(newSplits);

            while (output.count() < NUM_RECORDS_PER_SPLIT * numSplits) {
                reader.pollNext(output);
            }
            output.validate();
        }
    }

    @Test
    @Timeout(30)
    void testPollingFromEmptyQueue() throws Exception {
        ValidatingSourceOutput output = new ValidatingSourceOutput();
        List<SplitT> splits =
                Collections.singletonList(getSplit(0, NUM_RECORDS_PER_SPLIT, Boundedness.BOUNDED));
        // Consumer all the records in the s;oit.
        try (SourceReader<Integer, SplitT> reader =
                consumeRecords(splits, output, NUM_RECORDS_PER_SPLIT)) {
            // Now let the main thread poll again.
            assertThat(reader.pollNext(output))
                    .as("The status should be %s", InputStatus.NOTHING_AVAILABLE)
                    .isEqualTo(InputStatus.NOTHING_AVAILABLE);
        }
    }

    @Test
    @Timeout(30)
    void testAvailableOnEmptyQueue() throws Exception {
        // Consumer all the records in the split.
        try (SourceReader<Integer, SplitT> reader = createReader()) {
            CompletableFuture<?> future = reader.isAvailable();
            assertThat(future.isDone()).as("There should be no records ready for poll.").isFalse();
            // Add a split to the reader so there are more records to be read.
            reader.addSplits(
                    Collections.singletonList(
                            getSplit(0, NUM_RECORDS_PER_SPLIT, Boundedness.BOUNDED)));
            // The future should be completed fairly soon. Otherwise the test will hit timeout and
            // fail.
            future.get();
        }
    }

    @Test
    @Timeout(30)
    void testSnapshot() throws Exception {
        ValidatingSourceOutput output = new ValidatingSourceOutput();
        // Add a split to start the fetcher.
        List<SplitT> splits =
                getSplits(numSplits, NUM_RECORDS_PER_SPLIT, Boundedness.CONTINUOUS_UNBOUNDED);
        try (SourceReader<Integer, SplitT> reader =
                consumeRecords(splits, output, totalNumRecords)) {
            List<SplitT> state = reader.snapshotState(1L);
            assertThat(state).as("The snapshot should only have 10 splits. ").hasSize(numSplits);
            for (int i = 0; i < numSplits; i++) {
                assertThat(getNextRecordIndex(state.get(i)))
                        .as("The first four splits should have been fully consumed.")
                        .isEqualTo(NUM_RECORDS_PER_SPLIT);
            }
        }
    }

    // ---------------- helper methods -----------------

    protected abstract SourceReader<Integer, SplitT> createReader() throws Exception;

    protected abstract List<SplitT> getSplits(
            int numSplits, int numRecordsPerSplit, Boundedness boundedness);

    protected abstract SplitT getSplit(int splitId, int numRecords, Boundedness boundedness);

    protected abstract long getNextRecordIndex(SplitT split);

    private SourceReader<Integer, SplitT> consumeRecords(
            List<SplitT> splits, ValidatingSourceOutput output, int n) throws Exception {
        SourceReader<Integer, SplitT> reader = createReader();
        // Add splits to start the fetcher.
        reader.addSplits(splits);
        // Poll all the n records of the single split.
        while (output.count() < n) {
            reader.pollNext(output);
        }
        return reader;
    }

    // ---------------- helper classes -----------------

    /** A source output that validates the output. */
    public class ValidatingSourceOutput implements ReaderOutput<Integer> {
        private Set<Integer> consumedValues = new HashSet<>();
        private int max = Integer.MIN_VALUE;
        private int min = Integer.MAX_VALUE;

        private int count = 0;

        @Override
        public void collect(Integer element) {
            max = Math.max(element, max);
            min = Math.min(element, min);
            count++;
            consumedValues.add(element);
        }

        @Override
        public void collect(Integer element, long timestamp) {
            collect(element);
        }

        public void validate() {

            assertThat(consumedValues)
                    .as("Should be %d distinct elements in total", totalNumRecords)
                    .hasSize(totalNumRecords);
            assertThat(count)
                    .as("Should be %d elements in total", totalNumRecords)
                    .isEqualTo(totalNumRecords);
            assertThat(min).as("The min value should be 0", totalNumRecords).isZero();
            assertThat(max)
                    .as("The max value should be %d", totalNumRecords - 1)
                    .isEqualTo(totalNumRecords - 1);
        }

        public int count() {
            return count;
        }

        @Override
        public void emitWatermark(Watermark watermark) {}

        @Override
        public void markIdle() {}

        @Override
        public void markActive() {}

        @Override
        public SourceOutput<Integer> createOutputForSplit(String splitId) {
            return this;
        }

        @Override
        public void releaseOutputForSplit(String splitId) {}
    }
}
