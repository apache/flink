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

package org.apache.flink.connector.base.source.hybrid;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.mocks.MockSource;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SourceReaderOptions;
import org.apache.flink.connector.base.source.reader.mocks.MockBaseSource;
import org.apache.flink.connector.base.source.reader.mocks.MockSourceReader;
import org.apache.flink.connector.base.source.reader.mocks.MockSplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;
import org.apache.flink.connector.testutils.source.reader.TestingReaderOutput;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.mock.Whitebox;

import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link HybridSourceReader}. */
public class HybridSourceReaderTest {

    @Test
    public void testReader() throws Exception {
        TestingReaderContext readerContext = new TestingReaderContext();
        TestingReaderOutput<Integer> readerOutput = new TestingReaderOutput<>();
        MockBaseSource source = new MockBaseSource(1, 1, Boundedness.BOUNDED);

        // 2 underlying readers to exercise switch
        SourceReader<Integer, MockSourceSplit> mockSplitReader1 =
                source.createReader(readerContext);
        SourceReader<Integer, MockSourceSplit> mockSplitReader2 =
                source.createReader(readerContext);

        HybridSourceReader<Integer> reader = new HybridSourceReader<>(readerContext);

        assertThat(readerContext.getSentEvents()).isEmpty();
        reader.start();
        assertAndClearSourceReaderFinishedEvent(readerContext, -1);
        assertThat(currentReader(reader)).isNull();
        assertThat(reader.pollNext(readerOutput)).isEqualTo(InputStatus.NOTHING_AVAILABLE);

        Source source1 =
                new MockSource(null, 0) {
                    @Override
                    public SourceReader<Integer, MockSourceSplit> createReader(
                            SourceReaderContext readerContext) {
                        return mockSplitReader1;
                    }
                };
        reader.handleSourceEvents(new SwitchSourceEvent(0, source1, false));

        MockSourceSplit mockSplit = new MockSourceSplit(0, 0, 1);
        mockSplit.addRecord(0);

        SwitchedSources switchedSources = new SwitchedSources();
        switchedSources.put(0, source);
        HybridSourceSplit hybridSplit = HybridSourceSplit.wrapSplit(mockSplit, 0, switchedSources);
        reader.addSplits(Collections.singletonList(hybridSplit));

        // drain splits
        InputStatus status = reader.pollNext(readerOutput);
        while (readerOutput.getEmittedRecords().isEmpty() || status == InputStatus.MORE_AVAILABLE) {
            status = reader.pollNext(readerOutput);
            Thread.sleep(10);
        }
        assertThat(readerOutput.getEmittedRecords()).contains(0);
        reader.pollNext(readerOutput);
        assertThat(reader.pollNext(readerOutput))
                .as("before notifyNoMoreSplits")
                .isEqualTo(InputStatus.NOTHING_AVAILABLE);

        reader.notifyNoMoreSplits();
        reader.pollNext(readerOutput);
        assertAndClearSourceReaderFinishedEvent(readerContext, 0);

        assertThat(currentReader(reader))
                .as("reader before switch source event")
                .isEqualTo(mockSplitReader1);

        Source source2 =
                new MockSource(null, 0) {
                    @Override
                    public SourceReader<Integer, MockSourceSplit> createReader(
                            SourceReaderContext readerContext) {
                        return mockSplitReader2;
                    }
                };
        reader.handleSourceEvents(new SwitchSourceEvent(1, source2, true));
        assertThat(currentReader(reader))
                .as("reader after switch source event")
                .isEqualTo(mockSplitReader2);

        reader.notifyNoMoreSplits();
        assertThat(reader.pollNext(readerOutput))
                .as("reader 1 after notifyNoMoreSplits")
                .isEqualTo(InputStatus.END_OF_INPUT);

        reader.close();
    }

    @Test
    public void testAvailabilityFutureSwitchover() throws Exception {
        TestingReaderContext readerContext = new TestingReaderContext();
        TestingReaderOutput<Integer> readerOutput = new TestingReaderOutput<>();
        MockBaseSource source = new MockBaseSource(1, 1, Boundedness.BOUNDED);

        // 2 underlying readers to exercise switch
        MutableFutureSourceReader mockSplitReader1 =
                MutableFutureSourceReader.createReader(readerContext);
        MutableFutureSourceReader mockSplitReader2 =
                MutableFutureSourceReader.createReader(readerContext);

        HybridSourceReader<Integer> reader = new HybridSourceReader<>(readerContext);

        assertThat(readerContext.getSentEvents()).isEmpty();
        reader.start();
        assertAndClearSourceReaderFinishedEvent(readerContext, -1);
        assertThat(currentReader(reader)).isNull();
        assertThat(reader.pollNext(readerOutput)).isEqualTo(InputStatus.NOTHING_AVAILABLE);
        CompletableFuture<Void> hybridSourceFutureBeforeFirstReader = reader.isAvailable();
        assertThat(hybridSourceFutureBeforeFirstReader).isNotDone();

        Source source1 =
                new MockSource(null, 0) {
                    @Override
                    public SourceReader<Integer, MockSourceSplit> createReader(
                            SourceReaderContext readerContext) {
                        return mockSplitReader1;
                    }
                };
        reader.handleSourceEvents(new SwitchSourceEvent(0, source1, false));
        assertThat(hybridSourceFutureBeforeFirstReader)
                .isDone()
                .as("the previous underlying future should be completed after switch event");

        MockSourceSplit mockSplit = new MockSourceSplit(0, 0, 1);
        mockSplit.addRecord(0);

        SwitchedSources switchedSources = new SwitchedSources();
        switchedSources.put(0, source);
        HybridSourceSplit hybridSplit = HybridSourceSplit.wrapSplit(mockSplit, 0, switchedSources);
        reader.addSplits(Collections.singletonList(hybridSplit));

        // drain splits
        CompletableFuture<Void> futureBeforeDraining = reader.isAvailable();
        mockSplitReader1.completeFuture();
        assertThat(futureBeforeDraining)
                .isDone()
                .as("underlying future is complete and hybrid source should poll");

        InputStatus status = reader.pollNext(readerOutput);
        while (readerOutput.getEmittedRecords().isEmpty() || status == InputStatus.MORE_AVAILABLE) {
            status = reader.pollNext(readerOutput);
            Thread.sleep(10);
        }
        // mock reader no more records
        mockSplitReader1.resetFuture();

        CompletableFuture<Void> futureAfterDraining = reader.isAvailable();
        assertThat(futureBeforeDraining)
                .isNotEqualTo(futureAfterDraining)
                .as("Future should have been refreshed since the previous future is complete");
        assertThat(futureAfterDraining).isNotDone().as("Future should not be complete");

        assertThat(readerOutput.getEmittedRecords()).contains(0);
        reader.pollNext(readerOutput);
        assertThat(reader.pollNext(readerOutput))
                .as("before notifyNoMoreSplits")
                .isEqualTo(InputStatus.NOTHING_AVAILABLE);

        reader.notifyNoMoreSplits();
        reader.pollNext(readerOutput);
        assertAndClearSourceReaderFinishedEvent(readerContext, 0);

        assertThat(futureAfterDraining)
                .isNotDone()
                .as("still no more records and runtime should not poll");

        assertThat(currentReader(reader))
                .as("reader before switch source event")
                .isEqualTo(mockSplitReader1);

        Source source2 =
                new MockSource(null, 0) {
                    @Override
                    public SourceReader<Integer, MockSourceSplit> createReader(
                            SourceReaderContext readerContext) {
                        return mockSplitReader2;
                    }
                };

        reader.handleSourceEvents(new SwitchSourceEvent(1, source2, true));
        assertThat(futureAfterDraining)
                .isDone()
                .as("switching should signal completion to poll the new reader");
        CompletableFuture<Void> futureReader2 = reader.isAvailable();

        // futures must be different
        assertThat(futureBeforeDraining).isNotSameAs(futureReader2);
        assertThat(currentReader(reader))
                .as("reader after switch source event")
                .isEqualTo(mockSplitReader2);

        reader.notifyNoMoreSplits();
        assertThat(reader.pollNext(readerOutput))
                .as("reader 1 after notifyNoMoreSplits")
                .isEqualTo(InputStatus.END_OF_INPUT);
        assertThat(futureReader2)
                .isSameAs(reader.isAvailable())
                .as("future should not have been refreshed");

        reader.close();
    }

    @Test
    public void testReaderRecovery() throws Exception {
        TestingReaderContext readerContext = new TestingReaderContext();
        TestingReaderOutput<Integer> readerOutput = new TestingReaderOutput<>();
        MockBaseSource source = new MockBaseSource(1, 1, Boundedness.BOUNDED);

        HybridSourceReader<Integer> reader = new HybridSourceReader<>(readerContext);

        reader.start();
        assertAndClearSourceReaderFinishedEvent(readerContext, -1);
        reader.handleSourceEvents(new SwitchSourceEvent(0, source, false));

        MockSourceSplit mockSplit = new MockSourceSplit(0, 0, 2147483647);

        SwitchedSources switchedSources = new SwitchedSources();
        switchedSources.put(0, source);
        HybridSourceSplit hybridSplit = HybridSourceSplit.wrapSplit(mockSplit, 0, switchedSources);
        reader.addSplits(Collections.singletonList(hybridSplit));

        List<HybridSourceSplit> snapshot = reader.snapshotState(0);
        assertThat(snapshot).contains(hybridSplit);

        // reader recovery
        readerContext.clearSentEvents();
        reader = new HybridSourceReader<>(readerContext);

        reader.addSplits(snapshot);
        assertThat(currentReader(reader)).isNull();

        reader.start();
        assertThat(currentReader(reader)).isNull();

        assertAndClearSourceReaderFinishedEvent(readerContext, -1);
        reader.handleSourceEvents(new SwitchSourceEvent(0, source, false));
        assertThat(currentReader(reader)).isNotNull();
        assertThat(reader.snapshotState(1)).contains(hybridSplit);

        reader.close();
    }

    @Test
    public void testDefaultMethodDelegation() throws Exception {
        TestingReaderContext readerContext = new TestingReaderContext();
        TestingReaderOutput<Integer> readerOutput = new TestingReaderOutput<>();
        MockBaseSource source =
                new MockBaseSource(1, 1, Boundedness.BOUNDED) {
                    @Override
                    public SourceReader<Integer, MockSourceSplit> createReader(
                            SourceReaderContext readerContext) {
                        return Mockito.spy(super.createReader(readerContext));
                    }
                };

        HybridSourceReader<Integer> reader = new HybridSourceReader<>(readerContext);

        reader.start();
        assertAndClearSourceReaderFinishedEvent(readerContext, -1);
        reader.handleSourceEvents(new SwitchSourceEvent(0, source, false));
        SourceReader<Integer, MockSourceSplit> underlyingReader = currentReader(reader);

        reader.notifyCheckpointComplete(1);
        Mockito.verify(underlyingReader).notifyCheckpointComplete(1);

        reader.notifyCheckpointAborted(1);
        Mockito.verify(underlyingReader).notifyCheckpointAborted(1);

        reader.close();
    }

    private static SourceReader<Integer, MockSourceSplit> currentReader(
            HybridSourceReader<?> reader) {
        return Whitebox.getInternalState(reader, "currentReader");
    }

    private static void assertAndClearSourceReaderFinishedEvent(
            TestingReaderContext context, int sourceIndex) {
        assertThat(context.getSentEvents()).hasSize(1);
        assertThat(((SourceReaderFinishedEvent) context.getSentEvents().get(0)).sourceIndex())
                .isEqualTo(sourceIndex);
        context.clearSentEvents();
    }

    private static class MutableFutureSourceReader extends MockSourceReader {

        private CompletableFuture<Void> availabilityFuture = new CompletableFuture<>();

        public MutableFutureSourceReader(
                FutureCompletingBlockingQueue<RecordsWithSplitIds<int[]>> elementsQueue,
                Supplier<SplitReader<int[], MockSourceSplit>> splitFetcherSupplier,
                Configuration config,
                SourceReaderContext context) {
            super(elementsQueue, splitFetcherSupplier, config, context);
        }

        public static MutableFutureSourceReader createReader(SourceReaderContext readerContext) {
            FutureCompletingBlockingQueue<RecordsWithSplitIds<int[]>> elementsQueue =
                    new FutureCompletingBlockingQueue<>();

            Configuration config = new Configuration();
            config.setInteger(SourceReaderOptions.ELEMENT_QUEUE_CAPACITY, 2);
            config.setLong(SourceReaderOptions.SOURCE_READER_CLOSE_TIMEOUT, 30000L);
            MockSplitReader.Builder builder =
                    MockSplitReader.newBuilder()
                            .setNumRecordsPerSplitPerFetch(2)
                            .setBlockingFetch(true);
            return new MutableFutureSourceReader(
                    elementsQueue, builder::build, config, readerContext);
        }

        @Override
        public CompletableFuture<Void> isAvailable() {
            return availabilityFuture;
        }

        public void completeFuture() {
            availabilityFuture.complete(null);
        }

        public void resetFuture() {
            if (this.availabilityFuture.isDone()) {
                availabilityFuture = new CompletableFuture<>();
            }
        }
    }
}
