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
import org.apache.flink.connector.base.source.reader.mocks.MockBaseSource;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;
import org.apache.flink.connector.testutils.source.reader.TestingReaderOutput;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.mock.Whitebox;

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;

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

        Assert.assertThat(readerContext.getSentEvents(), Matchers.emptyIterable());
        reader.start();
        assertAndClearSourceReaderFinishedEvent(readerContext, -1);
        Assert.assertNull(currentReader(reader));
        Assert.assertEquals(InputStatus.NOTHING_AVAILABLE, reader.pollNext(readerOutput));

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
        Assert.assertThat(readerOutput.getEmittedRecords(), Matchers.contains(0));
        reader.pollNext(readerOutput);
        Assert.assertEquals(
                "before notifyNoMoreSplits",
                InputStatus.NOTHING_AVAILABLE,
                reader.pollNext(readerOutput));

        reader.notifyNoMoreSplits();
        reader.pollNext(readerOutput);
        assertAndClearSourceReaderFinishedEvent(readerContext, 0);

        Assert.assertEquals(
                "reader before switch source event", mockSplitReader1, currentReader(reader));

        Source source2 =
                new MockSource(null, 0) {
                    @Override
                    public SourceReader<Integer, MockSourceSplit> createReader(
                            SourceReaderContext readerContext) {
                        return mockSplitReader2;
                    }
                };
        reader.handleSourceEvents(new SwitchSourceEvent(1, source2, true));
        Assert.assertEquals(
                "reader after switch source event", mockSplitReader2, currentReader(reader));

        reader.notifyNoMoreSplits();
        Assert.assertEquals(
                "reader 1 after notifyNoMoreSplits",
                InputStatus.END_OF_INPUT,
                reader.pollNext(readerOutput));

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
        Assert.assertThat(snapshot, Matchers.contains(hybridSplit));

        // reader recovery
        readerContext.clearSentEvents();
        reader = new HybridSourceReader<>(readerContext);

        reader.addSplits(snapshot);
        Assert.assertNull(currentReader(reader));

        reader.start();
        Assert.assertNull(currentReader(reader));

        assertAndClearSourceReaderFinishedEvent(readerContext, -1);
        reader.handleSourceEvents(new SwitchSourceEvent(0, source, false));
        Assert.assertNotNull(currentReader(reader));
        Assert.assertThat(reader.snapshotState(1), Matchers.contains(hybridSplit));

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
        return (SourceReader) Whitebox.getInternalState(reader, "currentReader");
    }

    private static void assertAndClearSourceReaderFinishedEvent(
            TestingReaderContext context, int sourceIndex) {
        Assert.assertThat(context.getSentEvents(), Matchers.iterableWithSize(1));
        Assert.assertEquals(
                sourceIndex,
                ((SourceReaderFinishedEvent) context.getSentEvents().get(0)).sourceIndex());
        context.clearSentEvents();
    }
}
