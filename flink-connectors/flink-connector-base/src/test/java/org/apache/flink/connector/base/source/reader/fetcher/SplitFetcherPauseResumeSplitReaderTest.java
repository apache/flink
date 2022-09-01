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

package org.apache.flink.connector.base.source.reader.fetcher;

import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.mocks.MockSourceReader;
import org.apache.flink.connector.base.source.reader.mocks.MockSplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;
import org.apache.flink.connector.testutils.source.reader.TestingReaderOutput;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.testutils.CommonTestUtils;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests {@link SplitFetcher} integration to pause or resume {@link SplitReader} based on {@link
 * SourceReader} output.
 */
public class SplitFetcherPauseResumeSplitReaderTest {

    /**
     * Tests if pause or resume shows expected behavior which requires creation and execution of
     * {@link SplitFetcher} tasks.
     */
    @ParameterizedTest(name = "Individual reader per split: {0}")
    @ValueSource(booleans = {false, true})
    public void testPauseResumeSplitReaders(boolean individualReader) throws Exception {
        final AtomicInteger numSplitReaders = new AtomicInteger();
        final MockSplitReader.Builder readerBuilder =
                SteppingSourceReaderTestHarness.createSplitReaderBuilder();
        final SteppingSourceReaderTestHarness testHarness =
                new SteppingSourceReaderTestHarness(
                        () -> {
                            numSplitReaders.getAndIncrement();
                            return readerBuilder.build();
                        },
                        new Configuration());

        if (individualReader) {
            testHarness.addPrefilledSplitsIndividualReader(2, 5);
            assertThat(numSplitReaders.get()).isEqualTo(2);
        } else {
            testHarness.addPrefilledSplitsSingleReader(2, 5);
            assertThat(numSplitReaders.get()).isEqualTo(1);
        }

        TestingReaderOutput output = new TestingReaderOutput<>();
        testHarness.runUntilRecordsEmitted(output, 10, 2);
        Set<Integer> recordSet = new HashSet<>(output.getEmittedRecords());
        assertThat(recordSet).containsExactlyInAnyOrder(0, 1);

        testHarness.pauseOrResumeSplits(Collections.singleton("0"), Collections.emptyList());

        testHarness.runUntilRecordsEmitted(output, 10, 5);
        Set<Integer> recordSet2 = new HashSet<>(output.getEmittedRecords());
        assertThat(recordSet2).containsExactlyInAnyOrder(0, 1, 3, 5, 7);

        testHarness.pauseOrResumeSplits(Collections.emptyList(), Collections.singleton("0"));

        testHarness.runUntilAllRecordsEmitted(output, 10);
        Set<Integer> recordSet3 = new HashSet<>(output.getEmittedRecords());
        assertThat(recordSet3).containsExactlyInAnyOrder(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    }

    /**
     * Tests if pause or resume shows expected behavior in case of {@link SplitReader} that does not
     * support split or resume for scenarios (1) allowed and (2) not allowed.
     */
    @ParameterizedTest(name = "Allow unaligned source splits: {0}")
    @ValueSource(booleans = {true, false})
    public void testPauseResumeUnsupported(boolean allowUnalignedSourceSplits) throws Exception {
        final AtomicInteger numSplitReaders = new AtomicInteger();
        final Configuration configuration = new Configuration();
        configuration.setBoolean(
                "pipeline.watermark-alignment.allow-unaligned-source-splits",
                allowUnalignedSourceSplits);
        final MockSplitReader.Builder readerBuilder =
                SteppingSourceReaderTestHarness.createSplitReaderBuilder();

        final SteppingSourceReaderTestHarness testHarness =
                new SteppingSourceReaderTestHarness(
                        () -> {
                            if (numSplitReaders.getAndIncrement() == 0) {
                                return MockSplitReaderUnsupportedPause.cloneBuilder(readerBuilder)
                                        .build();
                            } else {
                                return readerBuilder.build();
                            }
                        },
                        configuration);

        testHarness.addPrefilledSplitsIndividualReader(2, 5);
        assertThat(numSplitReaders.get()).isEqualTo(2);

        TestingReaderOutput output = new TestingReaderOutput<>();
        testHarness.runUntilRecordsEmitted(output, 10, 2);
        Set<Integer> recordSet = new HashSet<>(output.getEmittedRecords());
        assertThat(recordSet).containsExactlyInAnyOrder(0, 1);

        testHarness.pauseOrResumeSplits(Collections.singleton("1"), Collections.emptyList());

        testHarness.runUntilRecordsEmitted(output, 10, 5);
        Set<Integer> recordSet2 = new HashSet<>(output.getEmittedRecords());
        assertThat(recordSet2).containsExactlyInAnyOrder(0, 1, 2, 4, 6);

        testHarness.pauseOrResumeSplits(Collections.singleton("0"), Collections.singleton("1"));

        if (allowUnalignedSourceSplits) {
            testHarness.runUntilAllRecordsEmitted(output, 10);
            Set<Integer> recordSet3 = new HashSet<>(output.getEmittedRecords());
            assertThat(recordSet3).containsExactlyInAnyOrder(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        } else {
            assertThatThrownBy(() -> testHarness.runUntilAllRecordsEmitted(output, 10))
                    .isInstanceOf(RuntimeException.class)
                    .hasCauseInstanceOf(UnsupportedOperationException.class);
        }
    }

    private static class MockSteppingSplitFetcherManager<E, SplitT extends SourceSplit>
            extends SingleThreadFetcherManager<E, SplitT> {

        public MockSteppingSplitFetcherManager(
                FutureCompletingBlockingQueue<RecordsWithSplitIds<E>> elementsQueue,
                Supplier<SplitReader<E, SplitT>> splitReaderSupplier,
                Configuration configuration) {
            super(elementsQueue, splitReaderSupplier, configuration);
        }

        @Override
        public void addSplits(List<SplitT> splitsToAdd) {
            SplitFetcher<E, SplitT> fetcher = createSplitFetcher();
            fetcher.addSplits(splitsToAdd);
        }

        public void runEachOnce() {
            for (SplitFetcher<E, SplitT> fetcher : fetchers.values()) {
                fetcher.runOnce();
            }
        }
    }

    private static class MockSplitReaderUnsupportedPause extends MockSplitReader {
        public MockSplitReaderUnsupportedPause(
                int numRecordsPerSplitPerFetch,
                boolean separatedFinishedRecord,
                boolean blockingFetch) {
            super(numRecordsPerSplitPerFetch, separatedFinishedRecord, blockingFetch);
        }

        @Override
        public void pauseOrResumeSplits(
                Collection<MockSourceSplit> splitsToPause,
                Collection<MockSourceSplit> splitsToResume) {
            throw new UnsupportedOperationException();
        }

        public static class Builder extends MockSplitReader.Builder {
            public Builder(MockSplitReader.Builder other) {
                super(other);
            }

            @Override
            public MockSplitReader build() {
                return new MockSplitReaderUnsupportedPause(
                        numRecordsPerSplitPerFetch, separatedFinishedRecord, blockingFetch);
            }
        }

        public static Builder cloneBuilder(MockSplitReader.Builder other) {
            return new MockSplitReaderUnsupportedPause.Builder(other);
        }
    }

    private static class SteppingSourceReaderTestHarness {
        private final MockSteppingSplitFetcherManager<int[], MockSourceSplit> fetcherManager;
        private final MockSourceReader sourceReader;

        public SteppingSourceReaderTestHarness(
                Supplier<SplitReader<int[], MockSourceSplit>> splitReaderSupplier,
                Configuration configuration) {
            FutureCompletingBlockingQueue<RecordsWithSplitIds<int[]>> queue =
                    new FutureCompletingBlockingQueue<>(10);
            this.fetcherManager =
                    new MockSteppingSplitFetcherManager<>(
                            queue, splitReaderSupplier, configuration);
            this.sourceReader =
                    new MockSourceReader(
                            queue, fetcherManager, configuration, new TestingReaderContext());
        }

        private static List<MockSourceSplit> createPrefilledSplits(int numSplits, int numRecords) {
            final List<MockSourceSplit> splits = new ArrayList<>(numSplits);
            for (int splitId = 0; splitId < numSplits; splitId++) {
                MockSourceSplit split = new MockSourceSplit(splitId, 0, numRecords);
                for (int i = 0; i < numRecords; i++) {
                    split.addRecord(i * numSplits + splitId);
                }
                splits.add(split);
            }
            return splits;
        }

        public void addPrefilledSplitsSingleReader(int numSplits, int numRecords) {
            sourceReader.addSplits(createPrefilledSplits(numSplits, numRecords));
            sourceReader.notifyNoMoreSplits();
        }

        public void addPrefilledSplitsIndividualReader(int numSplits, int numRecords) {
            for (MockSourceSplit split : createPrefilledSplits(numSplits, numRecords)) {
                sourceReader.addSplits(Collections.singletonList(split));
            }
            sourceReader.notifyNoMoreSplits();
        }

        public static MockSplitReader.Builder createSplitReaderBuilder() {
            return MockSplitReader.newBuilder()
                    .setNumRecordsPerSplitPerFetch(1)
                    .setBlockingFetch(false)
                    .setSeparatedFinishedRecord(true);
        }

        public int runUntilRecordsEmitted(
                TestingReaderOutput readerOutput, int timeoutSeconds, int numRecords)
                throws Exception {
            final AtomicReference<Exception> exception = new AtomicReference<>();
            final AtomicInteger numFetches = new AtomicInteger();
            CommonTestUtils.waitUtil(
                    () -> {
                        try {
                            this.fetcherManager.runEachOnce();
                            numFetches.getAndIncrement();
                            InputStatus status = this.sourceReader.pollNext(readerOutput);
                            while (status == InputStatus.MORE_AVAILABLE) {
                                status = this.sourceReader.pollNext(readerOutput);
                            }
                            if (status == InputStatus.END_OF_INPUT) {
                                return true;
                            } else if (numRecords < 0) {
                                return false;
                            } else {
                                return readerOutput.getEmittedRecords().size() >= numRecords;
                            }
                        } catch (Exception e) {
                            exception.set(e);
                            return true;
                        }
                    },
                    Duration.ofSeconds(timeoutSeconds),
                    String.format(
                            "%d %s records fetched within timeout",
                            readerOutput.getEmittedRecords().size(),
                            numRecords < 0 ? "but not all" : "out of " + numRecords));
            if (exception.get() != null) {
                throw exception.get();
            }
            return numFetches.get();
        }

        public int runUntilAllRecordsEmitted(TestingReaderOutput readerOutput, int timeoutSeconds)
                throws Exception {
            return runUntilRecordsEmitted(readerOutput, timeoutSeconds, -1);
        }

        public void pauseOrResumeSplits(
                Collection<String> splitsToPause, Collection<String> splitsToResume) {
            sourceReader.pauseOrResumeSplits(splitsToPause, splitsToResume);
        }
    }
}
