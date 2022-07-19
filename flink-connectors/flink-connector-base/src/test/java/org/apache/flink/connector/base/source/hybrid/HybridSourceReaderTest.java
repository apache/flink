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
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.mocks.MockSource;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.connector.base.source.reader.mocks.MockBaseSource;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;
import org.apache.flink.connector.testutils.source.reader.TestingReaderOutput;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.mock.Whitebox;

import org.junit.Test;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

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
        assertAndClearSourceReaderFinishedEvent(readerContext, -1, 0);
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
        pollUntil(
                reader,
                readerOutput,
                status ->
                        !readerOutput.getEmittedRecords().isEmpty()
                                && status != InputStatus.MORE_AVAILABLE,
                "Splits are not drained before timeout.");

        assertThat(readerOutput.getEmittedRecords()).contains(0);
        reader.pollNext(readerOutput);
        assertThat(reader.pollNext(readerOutput))
                .as("before notifyNoMoreSplits")
                .isEqualTo(InputStatus.NOTHING_AVAILABLE);

        reader.notifyNoMoreSplits();
        reader.pollNext(readerOutput);
        assertAndClearSourceReaderFinishedEvent(readerContext, 0, 1);

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
    public void testReaderRecovery() throws Exception {
        TestingReaderContext readerContext = new TestingReaderContext();
        TestingReaderOutput<Integer> readerOutput = new TestingReaderOutput<>();
        MockBaseSource source = new MockBaseSource(2, 10, Boundedness.BOUNDED);

        HybridSourceReader<Integer> reader = new HybridSourceReader<>(readerContext);

        reader.start();
        assertAndClearSourceReaderFinishedEvent(readerContext, -1, 0);
        reader.handleSourceEvents(new SwitchSourceEvent(0, source, false));

        Integer numRecordsUntilFinished = 10;
        MockSourceSplit mockSplit = new MockSourceSplit(0, 0, numRecordsUntilFinished * 2);
        MockSourceSplit mockFinishedSplit = new MockSourceSplit(1, 0, numRecordsUntilFinished);

        List<MockSourceSplit> mockSplits = new ArrayList<>();
        mockSplits.add(mockSplit);
        mockSplits.add(mockFinishedSplit);
        for (MockSourceSplit split : mockSplits) {
            addRecords(split, numRecordsUntilFinished, 0);
        }

        SwitchedSources switchedSources = new SwitchedSources();
        switchedSources.put(0, source);

        List<HybridSourceSplit> hybridSplits =
                HybridSourceSplit.wrapSplits(mockSplits, 0, switchedSources);
        reader.addSplits(hybridSplits);

        pollUntil(
                reader,
                readerOutput,
                status ->
                        readerOutput.getEmittedRecords().size() == numRecordsUntilFinished * 2
                                && status == InputStatus.NOTHING_AVAILABLE,
                "Not enough records are pulled before time out.");
        reader.pollNext(readerOutput);

        List<HybridSourceSplit> snapshot = reader.snapshotState(0);

        MockSourceSplit snapshotMockSplit =
                new MockSourceSplit(0, numRecordsUntilFinished, numRecordsUntilFinished * 2);
        MockSourceSplit snapshotMockFinishedSplit =
                new MockSourceSplit(1, numRecordsUntilFinished, numRecordsUntilFinished);
        List<HybridSourceSplit> snapshotHybridSplits = new ArrayList<>();
        snapshotHybridSplits.add(
                HybridSourceSplit.wrapSplit(snapshotMockSplit, 0, switchedSources));
        snapshotHybridSplits.add(
                HybridSourceSplit.wrapSplit(snapshotMockFinishedSplit, 0, switchedSources, true));

        assertThat(snapshot.size()).isEqualTo(2);
        assertThat(snapshot).hasSameElementsAs(snapshotHybridSplits);

        reader.close();

        // reader recovery
        readerContext.clearSentEvents();
        readerOutput.clearEmittedRecords();
        reader = new HybridSourceReader<>(readerContext);

        List<HybridSourceSplit> snapshotWithRecords = new ArrayList<>();
        for (HybridSourceSplit split : snapshot) {
            if (!split.isFinished) {
                MockSourceSplit mockSplitWithRecords =
                        (MockSourceSplit) HybridSourceSplit.unwrapSplit(split, switchedSources);
                addRecords(mockSplitWithRecords, numRecordsUntilFinished, 10);

                snapshotWithRecords.add(
                        HybridSourceSplit.wrapSplit(mockSplitWithRecords, 0, switchedSources));
            } else {
                snapshotWithRecords.add(split);
            }
        }

        reader.addSplits(snapshotWithRecords);
        assertThat(currentReader(reader)).isNull();

        reader.start();
        assertThat(currentReader(reader)).isNull();

        assertAndClearSourceReaderFinishedEvent(readerContext, -1, 0);
        reader.handleSourceEvents(new SwitchSourceEvent(0, source, false));
        assertThat(currentReader(reader)).isNotNull();

        snapshot = reader.snapshotState(1);
        assertThat(snapshot.size()).isEqualTo(2);
        assertThat(snapshot).hasSameElementsAs(snapshotHybridSplits);

        // pull all records from the remaining split, and check that
        // finished splits before recovery can be sent with the new finished splits
        pollUntil(
                reader,
                readerOutput,
                status ->
                        readerOutput.getEmittedRecords().size() == numRecordsUntilFinished
                                && status == InputStatus.NOTHING_AVAILABLE,
                "Not enough records are pulled before time out.");

        reader.notifyNoMoreSplits();
        reader.pollNext(readerOutput);
        assertAndClearSourceReaderFinishedEvent(readerContext, 0, 2);

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
        assertAndClearSourceReaderFinishedEvent(readerContext, -1, 0);
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
        return (SourceReader) Whitebox.getInternalState(reader, "currentReader");
    }

    private static void assertAndClearSourceReaderFinishedEvent(
            TestingReaderContext context, int sourceIndex, int numFinishedSplits) {
        assertThat(context.getSentEvents()).hasSize(1);
        SourceReaderFinishedEvent event =
                (SourceReaderFinishedEvent) context.getSentEvents().get(0);
        assertThat(event.sourceIndex()).isEqualTo(sourceIndex);
        assertThat(event.getFinishedSplits().size()).isEqualTo(numFinishedSplits);
        event.getFinishedSplits().forEach(split -> assertThat(split.isFinished).isTrue());
        context.clearSentEvents();
    }

    private void addRecords(MockSourceSplit split, Integer numRecords, Integer startIndex) {
        for (int i = startIndex; i < startIndex + numRecords; i++) {
            split.addRecord(i);
        }
    }

    private static void pollUntil(
            HybridSourceReader<Integer> reader,
            ReaderOutput<Integer> output,
            Function<InputStatus, Boolean> condition,
            String errorMessage)
            throws InterruptedException, TimeoutException {
        CommonTestUtils.waitUtil(
                () -> {
                    InputStatus status;
                    try {
                        status = reader.pollNext(output);
                    } catch (Exception exception) {
                        throw new RuntimeException(
                                "Caught unexpected exception when polling from the reader",
                                exception);
                    }
                    return condition.apply(status);
                },
                Duration.ofSeconds(Integer.MAX_VALUE),
                errorMessage);
    }
}
