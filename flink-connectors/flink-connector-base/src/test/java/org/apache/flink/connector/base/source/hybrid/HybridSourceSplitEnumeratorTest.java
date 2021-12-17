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
import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.apache.flink.connector.base.source.reader.mocks.MockBaseSource;
import org.apache.flink.connector.base.source.reader.mocks.MockSplitEnumerator;
import org.apache.flink.mock.Whitebox;

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/** Tests for {@link HybridSourceSplitEnumerator}. */
public class HybridSourceSplitEnumeratorTest {

    private static final int SUBTASK0 = 0;
    private static final int SUBTASK1 = 1;
    private static final MockBaseSource MOCK_SOURCE = new MockBaseSource(1, 1, Boundedness.BOUNDED);

    private HybridSource<Integer> source;
    private MockSplitEnumeratorContext<HybridSourceSplit> context;
    private HybridSourceSplitEnumerator enumerator;
    private HybridSourceSplit splitFromSource0;
    private HybridSourceSplit splitFromSource1;

    private void setupEnumeratorAndTriggerSourceSwitch() {
        context = new MockSplitEnumeratorContext<>(2);
        source = HybridSource.builder(MOCK_SOURCE).addSource(MOCK_SOURCE).build();

        enumerator = (HybridSourceSplitEnumerator) source.createEnumerator(context);
        enumerator.start();
        // mock enumerator assigns splits once all readers are registered
        registerReader(context, enumerator, SUBTASK0);
        assertThat(context.getSplitsAssignmentSequence(), Matchers.emptyIterable());
        registerReader(context, enumerator, SUBTASK1);
        assertThat(context.getSplitsAssignmentSequence(), Matchers.emptyIterable());
        enumerator.handleSourceEvent(SUBTASK0, new SourceReaderFinishedEvent(-1));
        assertThat(context.getSplitsAssignmentSequence(), Matchers.iterableWithSize(0));
        enumerator.handleSourceEvent(SUBTASK1, new SourceReaderFinishedEvent(-1));
        assertThat(context.getSplitsAssignmentSequence(), Matchers.iterableWithSize(1));
        splitFromSource0 =
                context.getSplitsAssignmentSequence().get(0).assignment().get(SUBTASK0).get(0);
        assertEquals(0, splitFromSource0.sourceIndex());
        assertEquals(0, getCurrentSourceIndex(enumerator));

        // trigger source switch
        enumerator.handleSourceEvent(SUBTASK0, new SourceReaderFinishedEvent(0));
        assertEquals("one reader finished", 0, getCurrentSourceIndex(enumerator));
        enumerator.handleSourceEvent(SUBTASK1, new SourceReaderFinishedEvent(0));
        assertEquals("both readers finished", 1, getCurrentSourceIndex(enumerator));
        assertThat(
                "switch triggers split assignment",
                context.getSplitsAssignmentSequence(),
                Matchers.iterableWithSize(2));
        splitFromSource1 =
                context.getSplitsAssignmentSequence().get(1).assignment().get(SUBTASK0).get(0);
        assertEquals(1, splitFromSource1.sourceIndex());
        enumerator.handleSourceEvent(SUBTASK1, new SourceReaderFinishedEvent(SUBTASK1));
        assertEquals("reader without assignment", 1, getCurrentSourceIndex(enumerator));
    }

    @Test
    public void testRegisterReaderAfterSwitchAndReaderReset() {
        setupEnumeratorAndTriggerSourceSwitch();

        // add split of previous source back (simulates reader reset during recovery)
        context.getSplitsAssignmentSequence().clear();
        enumerator.addReader(SUBTASK0);
        enumerator.addSplitsBack(Collections.singletonList(splitFromSource0), SUBTASK0);
        assertThat(context.getSplitsAssignmentSequence(), Matchers.iterableWithSize(0));
        enumerator.handleSourceEvent(SUBTASK0, new SourceReaderFinishedEvent(-1));
        assertSplitAssignment(
                "addSplitsBack triggers assignment when reader registered",
                context,
                1,
                splitFromSource0,
                SUBTASK0);

        // remove reader from context
        context.getSplitsAssignmentSequence().clear();
        context.unregisterReader(SUBTASK0);
        enumerator.addSplitsBack(Collections.singletonList(splitFromSource0), SUBTASK0);
        assertThat(
                "addSplitsBack doesn't trigger assignment when reader not registered",
                context.getSplitsAssignmentSequence(),
                Matchers.emptyIterable());
        registerReader(context, enumerator, SUBTASK0);
        assertThat(context.getSplitsAssignmentSequence(), Matchers.iterableWithSize(0));
        enumerator.handleSourceEvent(SUBTASK0, new SourceReaderFinishedEvent(-1));
        assertSplitAssignment(
                "registerReader triggers assignment", context, 1, splitFromSource0, SUBTASK0);
    }

    @Test
    public void testHandleSplitRequestAfterSwitchAndReaderReset() {
        setupEnumeratorAndTriggerSourceSwitch();

        UnderlyingEnumeratorWrapper underlyingEnumeratorWrapper =
                new UnderlyingEnumeratorWrapper(getCurrentEnumerator(enumerator));
        Whitebox.setInternalState(enumerator, "currentEnumerator", underlyingEnumeratorWrapper);

        List<MockSourceSplit> mockSourceSplits =
                (List<MockSourceSplit>)
                        Whitebox.getInternalState(underlyingEnumeratorWrapper.enumerator, "splits");
        assertThat(mockSourceSplits, Matchers.emptyIterable());

        // simulate reader reset to before switch by adding split of previous source back
        context.getSplitsAssignmentSequence().clear();
        assertEquals("current enumerator", 1, getCurrentSourceIndex(enumerator));

        assertThat(underlyingEnumeratorWrapper.handleSplitRequests, Matchers.emptyIterable());
        enumerator.handleSplitRequest(SUBTASK0, "fakehostname");

        SwitchedSources switchedSources = new SwitchedSources();
        switchedSources.put(1, MOCK_SOURCE);

        assertSplitAssignment(
                "handleSplitRequest triggers assignment of split by underlying enumerator",
                context,
                1,
                HybridSourceSplit.wrapSplit(
                        UnderlyingEnumeratorWrapper.SPLIT_1, 1, switchedSources),
                SUBTASK0);

        // handleSplitRequest invalid during reset
        enumerator.addSplitsBack(Collections.singletonList(splitFromSource0), SUBTASK0);
        try {
            enumerator.handleSplitRequest(SUBTASK0, "fakehostname");
            Assert.fail("expected exception");
        } catch (IllegalStateException ex) {
        }
    }

    @Test
    public void testRestoreEnumerator() throws Exception {
        setupEnumeratorAndTriggerSourceSwitch();
        enumerator = (HybridSourceSplitEnumerator) source.createEnumerator(context);
        enumerator.start();
        HybridSourceEnumeratorState enumeratorState = enumerator.snapshotState(0);
        MockSplitEnumerator underlyingEnumerator = getCurrentEnumerator(enumerator);
        Assert.assertThat(
                (List<MockSourceSplit>) Whitebox.getInternalState(underlyingEnumerator, "splits"),
                Matchers.iterableWithSize(1));
        enumerator =
                (HybridSourceSplitEnumerator) source.restoreEnumerator(context, enumeratorState);
        enumerator.start();
        underlyingEnumerator = getCurrentEnumerator(enumerator);
        Assert.assertThat(
                (List<MockSourceSplit>) Whitebox.getInternalState(underlyingEnumerator, "splits"),
                Matchers.iterableWithSize(1));
    }

    @Test
    public void testDefaultMethodDelegation() throws Exception {
        setupEnumeratorAndTriggerSourceSwitch();
        SplitEnumerator<MockSourceSplit, Object> underlyingEnumeratorSpy =
                Mockito.spy((SplitEnumerator) getCurrentEnumerator(enumerator));
        Whitebox.setInternalState(enumerator, "currentEnumerator", underlyingEnumeratorSpy);

        enumerator.notifyCheckpointComplete(1);
        Mockito.verify(underlyingEnumeratorSpy).notifyCheckpointComplete(1);

        enumerator.notifyCheckpointAborted(2);
        Mockito.verify(underlyingEnumeratorSpy).notifyCheckpointAborted(2);

        SwitchSourceEvent se = new SwitchSourceEvent(0, null, false);
        enumerator.handleSourceEvent(0, se);
        Mockito.verify(underlyingEnumeratorSpy).handleSourceEvent(0, se);
    }

    private static class UnderlyingEnumeratorWrapper
            implements SplitEnumerator<MockSourceSplit, Object> {
        private static final MockSourceSplit SPLIT_1 = new MockSourceSplit(0, 0, 1);
        private final List<Integer> handleSplitRequests = new ArrayList<>();
        private final MockSplitEnumerator enumerator;
        private final SplitEnumeratorContext context;

        private UnderlyingEnumeratorWrapper(MockSplitEnumerator enumerator) {
            this.enumerator = enumerator;
            this.context =
                    (SplitEnumeratorContext) Whitebox.getInternalState(enumerator, "context");
        }

        @Override
        public void handleSplitRequest(int subtaskId, String requesterHostname) {
            handleSplitRequests.add(subtaskId);
            context.assignSplits(new SplitsAssignment(SPLIT_1, subtaskId));
        }

        @Override
        public void start() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addSplitsBack(List splits, int subtaskId) {
            enumerator.addSplitsBack(splits, subtaskId);
        }

        @Override
        public void addReader(int subtaskId) {
            enumerator.addReader(subtaskId);
        }

        @Override
        public Object snapshotState(long checkpointId) throws Exception {
            return enumerator.snapshotState(checkpointId);
        }

        @Override
        public void close() throws IOException {
            enumerator.close();
        }
    }

    private static void assertSplitAssignment(
            String reason,
            MockSplitEnumeratorContext<HybridSourceSplit> context,
            int size,
            HybridSourceSplit split,
            int subtask) {
        assertThat(reason, context.getSplitsAssignmentSequence(), Matchers.iterableWithSize(size));
        assertEquals(
                reason,
                split,
                context.getSplitsAssignmentSequence()
                        .get(size - 1)
                        .assignment()
                        .get(subtask)
                        .get(0));
    }

    private static void registerReader(
            MockSplitEnumeratorContext<HybridSourceSplit> context,
            HybridSourceSplitEnumerator enumerator,
            int reader) {
        context.registerReader(new ReaderInfo(reader, "location 0"));
        enumerator.addReader(reader);
    }

    private static int getCurrentSourceIndex(HybridSourceSplitEnumerator enumerator) {
        return (int) Whitebox.getInternalState(enumerator, "currentSourceIndex");
    }

    private static MockSplitEnumerator getCurrentEnumerator(
            HybridSourceSplitEnumerator enumerator) {
        return (MockSplitEnumerator) Whitebox.getInternalState(enumerator, "currentEnumerator");
    }
}
