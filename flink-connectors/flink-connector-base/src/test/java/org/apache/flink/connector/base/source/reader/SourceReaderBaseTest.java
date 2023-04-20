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

package org.apache.flink.connector.base.source.reader;

import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.api.connector.source.mocks.MockSourceSplitSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.mocks.MockSourceReader;
import org.apache.flink.connector.base.source.reader.mocks.MockSplitReader;
import org.apache.flink.connector.base.source.reader.mocks.PassThroughRecordEmitter;
import org.apache.flink.connector.base.source.reader.mocks.TestingRecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.mocks.TestingSourceSplit;
import org.apache.flink.connector.base.source.reader.mocks.TestingSplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.testutils.source.reader.SourceReaderTestBase;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;
import org.apache.flink.connector.testutils.source.reader.TestingReaderOutput;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.source.event.AddSplitEvent;
import org.apache.flink.streaming.api.operators.SourceOperator;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.streaming.api.operators.source.TestingSourceOperator.createTestOperator;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** A unit test class for {@link SourceReaderBase}. */
public class SourceReaderBaseTest extends SourceReaderTestBase<MockSourceSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(SourceReaderBaseTest.class);

    @Test
    void testExceptionInSplitReader() {
        assertThatThrownBy(
                        () -> {
                            final String errMsg = "Testing Exception";

                            FutureCompletingBlockingQueue<RecordsWithSplitIds<int[]>>
                                    elementsQueue = new FutureCompletingBlockingQueue<>();
                            // We have to handle split changes first, otherwise fetch will not be
                            // called.
                            try (MockSourceReader reader =
                                    new MockSourceReader(
                                            elementsQueue,
                                            () ->
                                                    new SplitReader<int[], MockSourceSplit>() {
                                                        @Override
                                                        public RecordsWithSplitIds<int[]> fetch() {
                                                            throw new RuntimeException(errMsg);
                                                        }

                                                        @Override
                                                        public void handleSplitsChanges(
                                                                SplitsChange<MockSourceSplit>
                                                                        splitsChanges) {}

                                                        @Override
                                                        public void wakeUp() {}

                                                        @Override
                                                        public void close() {}
                                                    },
                                            getConfig(),
                                            new TestingReaderContext())) {
                                ValidatingSourceOutput output = new ValidatingSourceOutput();
                                reader.addSplits(
                                        Collections.singletonList(
                                                getSplit(
                                                        0,
                                                        NUM_RECORDS_PER_SPLIT,
                                                        Boundedness.CONTINUOUS_UNBOUNDED)));
                                reader.notifyNoMoreSplits();
                                // This is not a real infinite loop, it is supposed to throw
                                // exception after
                                // two polls.
                                while (true) {
                                    InputStatus inputStatus = reader.pollNext(output);
                                    assertThat(inputStatus).isNotEqualTo(InputStatus.END_OF_INPUT);
                                    // Add a sleep to avoid tight loop.
                                    Thread.sleep(1);
                                }
                            }
                        })
                .isInstanceOf(RuntimeException.class)
                .hasMessage("One or more fetchers have encountered exception");
    }

    @Test
    void testRecordsWithSplitsNotRecycledWhenRecordsLeft() throws Exception {
        final TestingRecordsWithSplitIds<String> records =
                new TestingRecordsWithSplitIds<>("test-split", "value1", "value2");
        final SourceReader<?, ?> reader = createReaderAndAwaitAvailable("test-split", records);

        reader.pollNext(new TestingReaderOutput<>());

        assertThat(records.isRecycled()).isFalse();
    }

    @Test
    void testRecordsWithSplitsRecycledWhenEmpty() throws Exception {
        final TestingRecordsWithSplitIds<String> records =
                new TestingRecordsWithSplitIds<>("test-split", "value1", "value2");
        final SourceReader<?, ?> reader = createReaderAndAwaitAvailable("test-split", records);

        // poll thrice: twice to get all records, one more to trigger recycle and moving to the next
        // split
        reader.pollNext(new TestingReaderOutput<>());
        reader.pollNext(new TestingReaderOutput<>());
        reader.pollNext(new TestingReaderOutput<>());

        assertThat(records.isRecycled()).isTrue();
    }

    @Test
    void testMultipleSplitsWithDifferentFinishingMoments() throws Exception {
        FutureCompletingBlockingQueue<RecordsWithSplitIds<int[]>> elementsQueue =
                new FutureCompletingBlockingQueue<>();
        MockSplitReader mockSplitReader =
                MockSplitReader.newBuilder()
                        .setNumRecordsPerSplitPerFetch(2)
                        .setSeparatedFinishedRecord(false)
                        .setBlockingFetch(false)
                        .build();
        MockSourceReader reader =
                new MockSourceReader(
                        elementsQueue,
                        () -> mockSplitReader,
                        getConfig(),
                        new TestingReaderContext());

        reader.start();

        List<MockSourceSplit> splits =
                Arrays.asList(
                        getSplit(0, 10, Boundedness.BOUNDED), getSplit(1, 12, Boundedness.BOUNDED));
        reader.addSplits(splits);
        reader.notifyNoMoreSplits();

        while (true) {
            InputStatus status = reader.pollNext(new TestingReaderOutput<>());
            if (status == InputStatus.END_OF_INPUT) {
                break;
            }
            if (status == InputStatus.NOTHING_AVAILABLE) {
                reader.isAvailable().get();
            }
        }
    }

    @Test
    void testMultipleSplitsWithSeparatedFinishedRecord() throws Exception {
        FutureCompletingBlockingQueue<RecordsWithSplitIds<int[]>> elementsQueue =
                new FutureCompletingBlockingQueue<>();
        MockSplitReader mockSplitReader =
                MockSplitReader.newBuilder()
                        .setNumRecordsPerSplitPerFetch(2)
                        .setSeparatedFinishedRecord(true)
                        .setBlockingFetch(false)
                        .build();
        MockSourceReader reader =
                new MockSourceReader(
                        elementsQueue,
                        () -> mockSplitReader,
                        getConfig(),
                        new TestingReaderContext());

        reader.start();

        List<MockSourceSplit> splits =
                Arrays.asList(
                        getSplit(0, 10, Boundedness.BOUNDED), getSplit(1, 10, Boundedness.BOUNDED));
        reader.addSplits(splits);
        reader.notifyNoMoreSplits();

        while (true) {
            InputStatus status = reader.pollNext(new TestingReaderOutput<>());
            if (status == InputStatus.END_OF_INPUT) {
                break;
            }
            if (status == InputStatus.NOTHING_AVAILABLE) {
                reader.isAvailable().get();
            }
        }
    }

    @Test
    void testPollNextReturnMoreAvailableWhenAllSplitFetcherCloseWithLeftoverElementInQueue()
            throws Exception {

        FutureCompletingBlockingQueue<RecordsWithSplitIds<int[]>> elementsQueue =
                new FutureCompletingBlockingQueue<>();
        MockSplitReader mockSplitReader =
                MockSplitReader.newBuilder()
                        .setNumRecordsPerSplitPerFetch(1)
                        .setBlockingFetch(true)
                        .build();
        BlockingShutdownSplitFetcherManager<int[], MockSourceSplit> splitFetcherManager =
                new BlockingShutdownSplitFetcherManager<>(
                        elementsQueue, () -> mockSplitReader, getConfig());
        final MockSourceReader sourceReader =
                new MockSourceReader(
                        elementsQueue,
                        splitFetcherManager,
                        getConfig(),
                        new TestingReaderContext());

        // Create and add a split that only contains one record
        final MockSourceSplit split = new MockSourceSplit(0, 0, 1);
        sourceReader.addSplits(Collections.singletonList(split));
        sourceReader.notifyNoMoreSplits();

        // Add the last record to the split when the splitFetcherManager shutting down SplitFetchers
        splitFetcherManager.getInShutdownSplitFetcherFuture().thenRun(() -> split.addRecord(1));
        assertThat(sourceReader.pollNext(new TestingReaderOutput<>()))
                .isEqualTo(InputStatus.MORE_AVAILABLE);
    }

    @ParameterizedTest(name = "Emit record before split addition: {0}")
    @ValueSource(booleans = {true, false})
    void testPerSplitWatermark(boolean emitRecordBeforeSplitAddition) throws Exception {
        MockSplitReader mockSplitReader =
                MockSplitReader.newBuilder()
                        .setNumRecordsPerSplitPerFetch(3)
                        .setBlockingFetch(true)
                        .build();

        MockSourceReader reader =
                new MockSourceReader(
                        new FutureCompletingBlockingQueue<>(),
                        () -> mockSplitReader,
                        new Configuration(),
                        new TestingReaderContext());

        SourceOperator<Integer, MockSourceSplit> sourceOperator =
                createTestOperator(
                        reader,
                        WatermarkStrategy.forGenerator(
                                (context) -> new OnEventWatermarkGenerator()),
                        true);

        MockSourceSplit splitA = new MockSourceSplit(0, 0, 3);
        splitA.addRecord(100);
        splitA.addRecord(200);
        splitA.addRecord(300);

        MockSourceSplit splitB = new MockSourceSplit(1, 0, 3);
        splitB.addRecord(150);
        splitB.addRecord(250);
        splitB.addRecord(350);

        WatermarkCollectingDataOutput output = new WatermarkCollectingDataOutput();

        if (emitRecordBeforeSplitAddition) {
            sourceOperator.emitNext(output);
        }

        AddSplitEvent<MockSourceSplit> addSplitsEvent =
                new AddSplitEvent<>(Arrays.asList(splitA, splitB), new MockSourceSplitSerializer());
        sourceOperator.handleOperatorEvent(addSplitsEvent);

        // First 3 records from split A should not generate any watermarks
        CommonTestUtils.waitUtil(
                () -> {
                    try {
                        sourceOperator.emitNext(output);
                    } catch (Exception e) {
                        LOG.warn("Exception caught at emitting records", e);
                        return false;
                    }
                    return output.numRecords == 3;
                },
                Duration.ofSeconds(10),
                String.format(
                        "%d out of 3 records are received within timeout", output.numRecords));
        assertThat(output.watermarks).isEmpty();

        CommonTestUtils.waitUtil(
                () -> {
                    try {
                        sourceOperator.emitNext(output);
                    } catch (Exception e) {
                        LOG.warn("Exception caught at emitting records", e);
                        return false;
                    }
                    return output.numRecords == 6;
                },
                Duration.ofSeconds(10),
                String.format(
                        "%d out of 6 records are received within timeout", output.numRecords));

        assertThat(output.watermarks).hasSize(3);
        assertThat(output.watermarks).containsExactly(150L, 250L, 300L);
    }

    @Test
    void testMultipleSplitsAndFinishedByRecordEvaluator() throws Exception {
        int split0End = 7;
        int split1End = 15;
        FutureCompletingBlockingQueue<RecordsWithSplitIds<int[]>> elementsQueue =
                new FutureCompletingBlockingQueue<>();
        MockSplitReader mockSplitReader =
                MockSplitReader.newBuilder()
                        .setNumRecordsPerSplitPerFetch(2)
                        .setSeparatedFinishedRecord(false)
                        .setBlockingFetch(false)
                        .build();
        MockSourceReader reader =
                new MockSourceReader(
                        elementsQueue,
                        new SingleThreadFetcherManager<>(
                                elementsQueue, () -> mockSplitReader, getConfig()),
                        getConfig(),
                        new TestingReaderContext(),
                        i -> i == split0End || i == split1End);
        reader.start();

        List<MockSourceSplit> splits =
                Arrays.asList(
                        getSplit(0, NUM_RECORDS_PER_SPLIT, Boundedness.BOUNDED),
                        getSplit(1, NUM_RECORDS_PER_SPLIT, Boundedness.BOUNDED));
        reader.addSplits(splits);
        reader.notifyNoMoreSplits();

        TestingReaderOutput<Integer> output = new TestingReaderOutput<>();
        while (true) {
            InputStatus status = reader.pollNext(output);
            if (status == InputStatus.END_OF_INPUT) {
                break;
            }
            if (status == InputStatus.NOTHING_AVAILABLE) {
                reader.isAvailable().get();
            }
        }
        List<Integer> excepted =
                IntStream.concat(
                                IntStream.range(0, split0End),
                                IntStream.range(NUM_RECORDS_PER_SPLIT, split1End))
                        .boxed()
                        .collect(Collectors.toList());
        assertThat(output.getEmittedRecords())
                .containsExactlyInAnyOrder(excepted.toArray(new Integer[excepted.size()]));
    }

    // ---------------- helper methods -----------------

    @Override
    protected MockSourceReader createReader() {
        FutureCompletingBlockingQueue<RecordsWithSplitIds<int[]>> elementsQueue =
                new FutureCompletingBlockingQueue<>();
        MockSplitReader mockSplitReader =
                MockSplitReader.newBuilder()
                        .setNumRecordsPerSplitPerFetch(2)
                        .setBlockingFetch(true)
                        .build();
        return new MockSourceReader(
                elementsQueue, () -> mockSplitReader, getConfig(), new TestingReaderContext());
    }

    @Override
    protected List<MockSourceSplit> getSplits(
            int numSplits, int numRecordsPerSplit, Boundedness boundedness) {
        List<MockSourceSplit> mockSplits = new ArrayList<>();
        for (int i = 0; i < numSplits; i++) {
            mockSplits.add(getSplit(i, numRecordsPerSplit, boundedness));
        }
        return mockSplits;
    }

    @Override
    protected MockSourceSplit getSplit(int splitId, int numRecords, Boundedness boundedness) {
        MockSourceSplit mockSplit;
        if (boundedness == Boundedness.BOUNDED) {
            mockSplit = new MockSourceSplit(splitId, 0, numRecords);
        } else {
            mockSplit = new MockSourceSplit(splitId);
        }
        for (int j = 0; j < numRecords; j++) {
            mockSplit.addRecord(splitId * 10 + j);
        }
        return mockSplit;
    }

    @Override
    protected long getNextRecordIndex(MockSourceSplit split) {
        return split.index();
    }

    private Configuration getConfig() {
        Configuration config = new Configuration();
        config.setInteger(SourceReaderOptions.ELEMENT_QUEUE_CAPACITY, 1);
        config.setLong(SourceReaderOptions.SOURCE_READER_CLOSE_TIMEOUT, 30000L);
        return config;
    }

    // ------------------------------------------------------------------------
    //  Testing Setup Helpers
    // ------------------------------------------------------------------------

    private static <E> SourceReader<E, ?> createReaderAndAwaitAvailable(
            final String splitId, final RecordsWithSplitIds<E> records) throws Exception {

        final FutureCompletingBlockingQueue<RecordsWithSplitIds<E>> elementsQueue =
                new FutureCompletingBlockingQueue<>();

        final SourceReader<E, TestingSourceSplit> reader =
                new SingleThreadMultiplexSourceReaderBase<
                        E, E, TestingSourceSplit, TestingSourceSplit>(
                        elementsQueue,
                        () -> new TestingSplitReader<>(records),
                        new PassThroughRecordEmitter<>(),
                        new Configuration(),
                        new TestingReaderContext()) {

                    @Override
                    public void notifyCheckpointComplete(long checkpointId) {}

                    @Override
                    protected void onSplitFinished(
                            Map<String, TestingSourceSplit> finishedSplitIds) {}

                    @Override
                    protected TestingSourceSplit initializedState(TestingSourceSplit split) {
                        return split;
                    }

                    @Override
                    protected TestingSourceSplit toSplitType(
                            String splitId, TestingSourceSplit splitState) {
                        return splitState;
                    }
                };

        reader.start();

        final List<TestingSourceSplit> splits =
                Collections.singletonList(new TestingSourceSplit(splitId));
        reader.addSplits(splits);

        reader.isAvailable().get();

        return reader;
    }

    // ------------------ Test helper classes -------------------
    /**
     * When maybeShutdownFinishedFetchers is invoke, BlockingShutdownSplitFetcherManager will
     * complete the inShutdownSplitFetcherFuture and ensures that all the split fetchers are
     * shutdown.
     */
    private static class BlockingShutdownSplitFetcherManager<E, SplitT extends SourceSplit>
            extends SingleThreadFetcherManager<E, SplitT> {

        private final CompletableFuture<Void> inShutdownSplitFetcherFuture;

        public BlockingShutdownSplitFetcherManager(
                FutureCompletingBlockingQueue<RecordsWithSplitIds<E>> elementsQueue,
                Supplier<SplitReader<E, SplitT>> splitReaderSupplier,
                Configuration configuration) {
            super(elementsQueue, splitReaderSupplier, configuration);
            this.inShutdownSplitFetcherFuture = new CompletableFuture<>();
        }

        @Override
        public boolean maybeShutdownFinishedFetchers() {
            shutdownAllSplitFetcher();
            return true;
        }

        public CompletableFuture<Void> getInShutdownSplitFetcherFuture() {
            return inShutdownSplitFetcherFuture;
        }

        private void shutdownAllSplitFetcher() {
            inShutdownSplitFetcherFuture.complete(null);
            while (!super.maybeShutdownFinishedFetchers()) {
                try {
                    // avoid tight loop
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static class OnEventWatermarkGenerator implements WatermarkGenerator<Integer> {

        @Override
        public void onEvent(Integer event, long eventTimestamp, WatermarkOutput output) {
            output.emitWatermark(new org.apache.flink.api.common.eventtime.Watermark(event));
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {}
    }

    private static class WatermarkCollectingDataOutput
            implements PushingAsyncDataInput.DataOutput<Integer> {
        int numRecords = 0;
        final List<Long> watermarks = new ArrayList<>();

        @Override
        public void emitRecord(StreamRecord<Integer> streamRecord) {
            numRecords++;
        }

        @Override
        public void emitWatermark(org.apache.flink.streaming.api.watermark.Watermark watermark)
                throws Exception {
            watermarks.add(watermark.getTimestamp());
        }

        @Override
        public void emitWatermarkStatus(WatermarkStatus watermarkStatus) {}

        @Override
        public void emitLatencyMarker(LatencyMarker latencyMarker) {}
    }
}
