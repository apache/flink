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

package org.apache.flink.runtime.source.coordinator;

import org.apache.flink.api.common.eventtime.WatermarkAlignmentParams;
import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SupportsHandleExecutionAttemptSourceEvent;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.api.connector.source.mocks.MockSourceSplitSerializer;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorCheckpointSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.source.event.NoMoreSplitsEvent;
import org.apache.flink.runtime.source.event.RequestSplitEvent;
import org.apache.flink.runtime.source.event.SourceEventWrapper;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link SourceCoordinator} when it is enabled to handle concurrent execution
 * attempts.
 */
class SourceCoordinatorConcurrentAttemptsTest extends SourceCoordinatorTestBase {

    private boolean enumeratorSupportsHandleExecutionAttemptSourceEvent;

    @Override
    @BeforeEach
    void setup() throws Exception {
        supportsConcurrentExecutionAttempts = true;
        enumeratorSupportsHandleExecutionAttemptSourceEvent = true;
        super.setup();
    }

    @Test
    void testCoordinatorThrowExceptionIfWatermarkAlignmentIsEnabled() {
        enumeratorSupportsHandleExecutionAttemptSourceEvent = false;

        assertThatThrownBy(
                        () ->
                                getNewSourceCoordinator(
                                        new WatermarkAlignmentParams(
                                                1000L, "group1", Long.MAX_VALUE)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testCoordinatorFailJobOnSourceEventToNonsupportingEnumerator() throws Exception {
        enumeratorSupportsHandleExecutionAttemptSourceEvent = false;
        sourceCoordinator = getNewSourceCoordinator();
        context = sourceCoordinator.getContext();

        sourceCoordinator.start();
        sourceCoordinator.handleEventFromOperator(
                0, 0, new SourceEventWrapper(new TestSourceEvent()));
        waitForCoordinatorToProcessActions();
        assertThat(operatorCoordinatorContext.isJobFailed()).isTrue();
    }

    @Test
    void testContextThrowExceptionOnSourceEventToNonsupportingMethod() throws Exception {
        enumeratorSupportsHandleExecutionAttemptSourceEvent = false;
        sourceCoordinator = getNewSourceCoordinator();
        context = sourceCoordinator.getContext();

        sourceCoordinator.start();
        sourceReady();
        assertThatThrownBy(() -> context.sendEventToSourceReader(0, new TestSourceEvent()))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testConcurrentAttemptsRequestSplits() throws Exception {
        sourceCoordinator.start();

        final List<MockSourceSplit> splits = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            splits.add(new MockSourceSplit(i));
        }
        getEnumerator().addNewSplits(splits);

        int attempt0 = 0;
        setReaderTaskReady(sourceCoordinator, 0, attempt0);
        registerReader(0, attempt0);
        sourceCoordinator.handleEventFromOperator(0, attempt0, new RequestSplitEvent());
        waitForSentEvents(1);

        int attempt1 = 3;
        setReaderTaskReady(sourceCoordinator, 0, attempt1);
        registerReader(0, attempt1);
        waitForSentEvents(2);

        sourceCoordinator.handleEventFromOperator(0, attempt1, new RequestSplitEvent());
        waitForSentEvents(4);

        sourceCoordinator.handleEventFromOperator(0, attempt0, new RequestSplitEvent());
        waitForSentEvents(6);

        int attempt2 = 5;
        setReaderTaskReady(sourceCoordinator, 0, attempt2);
        registerReader(0, attempt2);
        waitForSentEvents(8);

        final List<OperatorEvent> events = receivingTasks.getSentEventsForSubtask(0);
        assertAddSplitEvent(events.get(0), Collections.singletonList(splits.get(0)));
        assertAddSplitEvent(events.get(1), Collections.singletonList(splits.get(0)));
        assertAddSplitEvent(events.get(2), Collections.singletonList(splits.get(1)));
        assertAddSplitEvent(events.get(3), Collections.singletonList(splits.get(1)));
        assertAddSplitEvent(events.get(6), splits);

        assertThat(events.get(4)).isInstanceOf(NoMoreSplitsEvent.class);
        assertThat(events.get(5)).isInstanceOf(NoMoreSplitsEvent.class);
        assertThat(events.get(7)).isInstanceOf(NoMoreSplitsEvent.class);
    }

    @Test
    public void testReaderInfoOfConcurrentAttempts() throws Exception {
        sourceCoordinator.start();
        registerReader(0, 3);
        registerReader(0, 5);
        waitForCoordinatorToProcessActions();

        assertThat(context.registeredReadersOfAttempts()).hasSize(1);

        Map<Integer, ReaderInfo> attemptReaders = context.registeredReadersOfAttempts().get(0);
        assertThat(attemptReaders).containsOnlyKeys(3, 5);
        assertThat(attemptReaders.get(3)).isNotNull();
        assertThat(attemptReaders.get(3).getLocation()).isEqualTo(createLocationFor(0, 3));
        assertThat(attemptReaders.get(5)).isNotNull();
        assertThat(attemptReaders.get(5).getLocation()).isEqualTo(createLocationFor(0, 5));

        sourceCoordinator.executionAttemptFailed(0, 5, new Exception());
        waitForCoordinatorToProcessActions();

        attemptReaders = context.registeredReadersOfAttempts().get(0);
        assertThat(attemptReaders).containsOnlyKeys(3);

        sourceCoordinator.subtaskReset(0, -1);
        waitForCoordinatorToProcessActions();

        assertThat(context.registeredReadersOfAttempts()).isEmpty();
    }

    @Test
    public void testSubtaskReaderInfoOfConcurrentAttempts() throws Exception {
        sourceCoordinator.start();
        registerReader(0, 3);
        registerReader(0, 5);
        waitForCoordinatorToProcessActions();

        assertThat(context.registeredReaders()).hasSize(1);
        assertThat(context.registeredReaders().get(0).getLocation())
                .isEqualTo(createLocationFor(0, 3));

        registerReader(0, 1);
        waitForCoordinatorToProcessActions();

        // the subtask reader info is the reader info of the attempt with the smallest attempt
        // number
        assertThat(context.registeredReaders().get(0).getLocation())
                .isEqualTo(createLocationFor(0, 1));
    }

    @Test
    public void testForwardAttemptSourceEvents() throws Exception {
        sourceCoordinator.start();

        final SourceEvent event1 = new TestSourceEvent();
        final SourceEvent event2 = new TestSourceEvent();
        sourceCoordinator.handleEventFromOperator(0, 3, new SourceEventWrapper(event1));
        sourceCoordinator.handleEventFromOperator(0, 5, new SourceEventWrapper(event2));

        waitForCoordinatorToProcessActions();

        assertThat(getTestEnumerator().getEvent(0, 3)).isSameAs(event1);
        assertThat(getTestEnumerator().getEvent(0, 5)).isSameAs(event2);
    }

    @Override
    Source<Integer, MockSourceSplit, Set<MockSourceSplit>> createMockSource() {
        if (enumeratorSupportsHandleExecutionAttemptSourceEvent) {
            return new TestSource(
                    new MockSourceSplitSerializer(), new MockSplitEnumeratorCheckpointSerializer());
        } else {
            return TestingSplitEnumerator.factorySource(
                    new MockSourceSplitSerializer(), new MockSplitEnumeratorCheckpointSerializer());
        }
    }

    private TestEnumerator<MockSourceSplit> getTestEnumerator() {
        return (TestEnumerator) super.getEnumerator();
    }

    private static final class TestSource<T, SplitT extends SourceSplit>
            extends TestingSplitEnumerator.FactorySource<T, SplitT> {

        public TestSource(
                SimpleVersionedSerializer<SplitT> splitSerializer,
                SimpleVersionedSerializer<Set<SplitT>> checkpointSerializer) {
            super(splitSerializer, checkpointSerializer);
        }

        @Override
        public TestingSplitEnumerator<SplitT> createEnumerator(
                SplitEnumeratorContext<SplitT> enumContext) {
            return new TestEnumerator<>(enumContext);
        }

        @Override
        public SplitEnumerator<SplitT, Set<SplitT>> restoreEnumerator(
                SplitEnumeratorContext<SplitT> enumContext, Set<SplitT> checkpoint) {
            return new TestEnumerator<>(enumContext, checkpoint);
        }
    }

    private static class TestEnumerator<SplitT extends SourceSplit>
            extends TestingSplitEnumerator<SplitT>
            implements SupportsHandleExecutionAttemptSourceEvent {

        private final Map<Integer, Map<Integer, SourceEvent>> sourceEvents;

        private TestEnumerator(SplitEnumeratorContext<SplitT> context) {
            super(context);
            this.sourceEvents = new HashMap<>();
        }

        private TestEnumerator(
                SplitEnumeratorContext<SplitT> context, Collection<SplitT> restoredSplits) {
            super(context, restoredSplits);
            this.sourceEvents = new HashMap<>();
        }

        @Override
        public void handleSourceEvent(int subtaskId, int attemptNumber, SourceEvent sourceEvent) {
            sourceEvents
                    .computeIfAbsent(subtaskId, k -> new HashMap<>())
                    .put(attemptNumber, sourceEvent);
            handleSourceEvent(subtaskId, sourceEvent);
        }

        private SourceEvent getEvent(int subtaskId, int attemptNumber) {
            return sourceEvents.getOrDefault(subtaskId, new HashMap<>()).get(attemptNumber);
        }
    }

    private static class TestSourceEvent implements SourceEvent, Serializable {}
}
