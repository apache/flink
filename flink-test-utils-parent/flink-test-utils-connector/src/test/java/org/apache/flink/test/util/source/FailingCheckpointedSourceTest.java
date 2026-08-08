/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.util.source;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.util.FlinkRuntimeException;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the failure protocol of {@link FailingCheckpointedSource}: arm on a snapshot past the
 * threshold, pause at the failure position, fail once the armed checkpoint completes, and resume
 * from the restored position without failing again.
 */
class FailingCheckpointedSourceTest {

    private static final long EMIT_CALLS = 20;
    private static final long FAILURE_POSITION = 10;

    private static FailingCheckpointedSource<Long> failingSource() {
        return FailingCheckpointedSource.of(
                (subtaskIndex, sequenceNo, output) -> output.collect(sequenceNo),
                EMIT_CALLS,
                FailingCheckpointedSource.FailurePolicy.failAfterEmitCalls(FAILURE_POSITION),
                Types.LONG);
    }

    private static SourceReader<Long, SequenceSplit> readerWithSplit(
            FailingCheckpointedSource<Long> source, SequenceSplit split) {
        final SourceReader<Long, SequenceSplit> reader = source.createReader(null);
        reader.addSplits(Collections.singletonList(split));
        return reader;
    }

    private static List<Long> range(long startInclusive, long endExclusive) {
        return LongStream.range(startInclusive, endExclusive).boxed().collect(Collectors.toList());
    }

    @Test
    void testFailsOnceAfterArmedCheckpointCompletes() throws Exception {
        final SourceReader<Long, SequenceSplit> reader =
                readerWithSplit(failingSource(), new SequenceSplit(0, 0));
        final TestReaderOutput<Long> output = new TestReaderOutput<>();

        // Emission holds just past the arming threshold until a snapshot arms the failure.
        final long armHoldPosition = FAILURE_POSITION / 2 + 1;
        for (long i = 0; i < armHoldPosition; i++) {
            assertThat(reader.pollNext(output)).isEqualTo(InputStatus.MORE_AVAILABLE);
        }
        assertThat(reader.pollNext(output)).isEqualTo(InputStatus.NOTHING_AVAILABLE);
        assertThat(reader.isAvailable()).isNotDone();
        assertThat(output.getCollected()).isEqualTo(range(0, armHoldPosition));

        // The arming snapshot captures a position strictly before the failure position, so
        // recovery is guaranteed to replay the records in between.
        final List<SequenceSplit> snapshot = reader.snapshotState(1);
        assertThat(snapshot).hasSize(1);
        assertThat(snapshot.get(0).getPosition())
                .isEqualTo(armHoldPosition)
                .isLessThan(FAILURE_POSITION);
        assertThat(reader.isAvailable()).isDone();

        // Emission resumes to the failure position and holds for the armed checkpoint.
        for (long i = armHoldPosition; i < FAILURE_POSITION; i++) {
            assertThat(reader.pollNext(output)).isEqualTo(InputStatus.MORE_AVAILABLE);
        }
        assertThat(reader.pollNext(output)).isEqualTo(InputStatus.NOTHING_AVAILABLE);
        assertThat(reader.isAvailable()).isNotDone();

        reader.notifyCheckpointComplete(1);
        assertThat(reader.isAvailable()).isDone();
        assertThatThrownBy(() -> reader.pollNext(output))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessage("Artificial Failure");
        assertThat(output.getCollected()).isEqualTo(range(0, FAILURE_POSITION));
    }

    @Test
    void testDoesNotArmOnCheckpointBeforeThreshold() throws Exception {
        final SourceReader<Long, SequenceSplit> reader =
                readerWithSplit(failingSource(), new SequenceSplit(0, 0));
        final TestReaderOutput<Long> output = new TestReaderOutput<>();

        // Only 3 emit calls: below the arming threshold of FAILURE_POSITION / 2.
        for (long i = 0; i < 3; i++) {
            reader.pollNext(output);
        }
        reader.snapshotState(1);
        reader.notifyCheckpointComplete(1);

        // The early checkpoint must not trigger the failure; emission continues to the arm hold.
        final long armHoldPosition = FAILURE_POSITION / 2 + 1;
        for (long i = 3; i < armHoldPosition; i++) {
            assertThat(reader.pollNext(output)).isEqualTo(InputStatus.MORE_AVAILABLE);
        }
        assertThat(reader.pollNext(output)).isEqualTo(InputStatus.NOTHING_AVAILABLE);

        reader.snapshotState(2);
        for (long i = armHoldPosition; i < FAILURE_POSITION; i++) {
            assertThat(reader.pollNext(output)).isEqualTo(InputStatus.MORE_AVAILABLE);
        }
        assertThat(reader.pollNext(output)).isEqualTo(InputStatus.NOTHING_AVAILABLE);
        reader.notifyCheckpointComplete(2);
        assertThatThrownBy(() -> reader.pollNext(output))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessage("Artificial Failure");
    }

    @Test
    void testRestoredReaderResumesWithoutFailingAgain() throws Exception {
        final SourceReader<Long, SequenceSplit> reader =
                readerWithSplit(failingSource(), new SequenceSplit(0, FAILURE_POSITION));
        final TestReaderOutput<Long> output = new TestReaderOutput<>();

        InputStatus status = InputStatus.MORE_AVAILABLE;
        while (status == InputStatus.MORE_AVAILABLE) {
            status = reader.pollNext(output);
        }
        assertThat(status).isEqualTo(InputStatus.END_OF_INPUT);
        assertThat(output.getCollected()).isEqualTo(range(FAILURE_POSITION, EMIT_CALLS));
    }

    @Test
    void testNonZeroSubtaskNeverFails() throws Exception {
        final SourceReader<Long, SequenceSplit> reader =
                readerWithSplit(failingSource(), new SequenceSplit(1, 0));
        final TestReaderOutput<Long> output = new TestReaderOutput<>();

        InputStatus status = InputStatus.MORE_AVAILABLE;
        while (status == InputStatus.MORE_AVAILABLE) {
            reader.snapshotState(output.getCollectedCount());
            reader.notifyCheckpointComplete(output.getCollectedCount());
            status = reader.pollNext(output);
        }
        assertThat(status).isEqualTo(InputStatus.END_OF_INPUT);
        assertThat(output.getCollected()).isEqualTo(range(0, EMIT_CALLS));
    }

    @Test
    void testNeverFailPolicyEmitsAllAndFinishes() throws Exception {
        final FailingCheckpointedSource<Long> source =
                FailingCheckpointedSource.of(
                        (subtaskIndex, sequenceNo, output) -> output.collect(sequenceNo),
                        EMIT_CALLS,
                        FailingCheckpointedSource.FailurePolicy.neverFail(),
                        Types.LONG);
        final SourceReader<Long, SequenceSplit> reader =
                readerWithSplit(source, new SequenceSplit(0, 0));
        final TestReaderOutput<Long> output = new TestReaderOutput<>();

        InputStatus status = InputStatus.MORE_AVAILABLE;
        while (status == InputStatus.MORE_AVAILABLE) {
            status = reader.pollNext(output);
        }
        assertThat(status).isEqualTo(InputStatus.END_OF_INPUT);
        assertThat(output.getCollected()).isEqualTo(range(0, EMIT_CALLS));
    }

    @Test
    void testIdleAfterEmissionKeepsReaderAlive() throws Exception {
        final SourceReader<Long, SequenceSplit> reader =
                readerWithSplit(
                        failingSource().withIdleAfterEmission(),
                        new SequenceSplit(0, FAILURE_POSITION));
        final TestReaderOutput<Long> output = new TestReaderOutput<>();

        InputStatus status = InputStatus.MORE_AVAILABLE;
        while (status == InputStatus.MORE_AVAILABLE) {
            status = reader.pollNext(output);
        }
        assertThat(status).isEqualTo(InputStatus.NOTHING_AVAILABLE);
        assertThat(reader.isAvailable()).isNotDone();
        assertThat(output.getCollected()).isEqualTo(range(FAILURE_POSITION, EMIT_CALLS));
    }

    @Test
    void testSimulatedStateLossReemitsFromZero() throws Exception {
        // Red-check knob: a restored reader discards its position, producing duplicates that an
        // exactly-once test must detect.
        final SourceReader<Long, SequenceSplit> reader =
                readerWithSplit(
                        failingSource().withSimulatedStateLossOnRecovery(),
                        new SequenceSplit(0, FAILURE_POSITION));
        final TestReaderOutput<Long> output = new TestReaderOutput<>();

        InputStatus status = InputStatus.MORE_AVAILABLE;
        while (status == InputStatus.MORE_AVAILABLE) {
            status = reader.pollNext(output);
        }
        assertThat(status).isEqualTo(InputStatus.END_OF_INPUT);
        assertThat(output.getCollected()).isEqualTo(range(0, EMIT_CALLS));
    }

    @Test
    void testGeneratorEmitsMultipleRecordsWithTimestampsAtomically() throws Exception {
        final FailingCheckpointedSource<Long> source =
                FailingCheckpointedSource.of(
                        (subtaskIndex, sequenceNo, output) -> {
                            output.collect(sequenceNo, sequenceNo);
                            output.collect(-sequenceNo, sequenceNo);
                            output.emitWatermark(sequenceNo);
                        },
                        3,
                        FailingCheckpointedSource.FailurePolicy.neverFail(),
                        Types.LONG);
        final SourceReader<Long, SequenceSplit> reader =
                readerWithSplit(source, new SequenceSplit(0, 0));
        final TestReaderOutput<Long> output = new TestReaderOutput<>();

        assertThat(reader.pollNext(output)).isEqualTo(InputStatus.MORE_AVAILABLE);
        assertThat(output.getCollected()).containsExactly(0L, 0L);

        assertThat(reader.pollNext(output)).isEqualTo(InputStatus.MORE_AVAILABLE);
        assertThat(output.getCollected()).containsExactly(0L, 0L, 1L, -1L);
    }

    @Test
    void testAbortedArmedCheckpointRearmsOnNextSnapshot() throws Exception {
        final SourceReader<Long, SequenceSplit> reader =
                readerWithSplit(failingSource(), new SequenceSplit(0, 0));
        final TestReaderOutput<Long> output = new TestReaderOutput<>();

        final long armHoldPosition = FAILURE_POSITION / 2 + 1;
        for (long i = 0; i < armHoldPosition; i++) {
            reader.pollNext(output);
        }
        assertThat(reader.pollNext(output)).isEqualTo(InputStatus.NOTHING_AVAILABLE);

        reader.snapshotState(1);
        reader.notifyCheckpointAborted(1);
        // the aborted checkpoint must not trigger the failure and must not stay armed; the
        // reader holds at the arming position again
        reader.notifyCheckpointComplete(1);
        assertThat(reader.pollNext(output)).isEqualTo(InputStatus.NOTHING_AVAILABLE);

        reader.snapshotState(2);
        for (long i = armHoldPosition; i < FAILURE_POSITION; i++) {
            assertThat(reader.pollNext(output)).isEqualTo(InputStatus.MORE_AVAILABLE);
        }
        assertThat(reader.pollNext(output)).isEqualTo(InputStatus.NOTHING_AVAILABLE);
        reader.notifyCheckpointComplete(2);
        assertThatThrownBy(() -> reader.pollNext(output))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessage("Artificial Failure");
    }

    @Test
    void testEnumeratorAssignsOneSplitPerSubtaskOnFreshStart() throws Exception {
        final MockSplitEnumeratorContext<SequenceSplit> context =
                new MockSplitEnumeratorContext<>(2);
        final SplitEnumerator<SequenceSplit, Void> enumerator =
                failingSource().createEnumerator(context);

        enumerator.addReader(0);
        enumerator.addReader(1);

        assertThat(assignedSplits(context, 0)).containsExactly(new SequenceSplit(0, 0));
        assertThat(assignedSplits(context, 1)).containsExactly(new SequenceSplit(1, 0));
    }

    @Test
    void testEnumeratorDoesNotReassignOnReRegistration() throws Exception {
        // Regression test: on a partial failover the enumerator instance survives and the
        // restarted reader re-registers; it recovers its split from its own checkpointed state,
        // so the enumerator must not assign a second, fresh split.
        final MockSplitEnumeratorContext<SequenceSplit> context =
                new MockSplitEnumeratorContext<>(1);
        final SplitEnumerator<SequenceSplit, Void> enumerator =
                failingSource().createEnumerator(context);

        enumerator.addReader(0);
        enumerator.addReader(0);

        assertThat(assignedSplits(context, 0)).containsExactly(new SequenceSplit(0, 0));
    }

    @Test
    void testEnumeratorReassignsSplitsReturnedAfterFailure() throws Exception {
        final MockSplitEnumeratorContext<SequenceSplit> context =
                new MockSplitEnumeratorContext<>(1);
        final SplitEnumerator<SequenceSplit, Void> enumerator =
                failingSource().createEnumerator(context);

        enumerator.addReader(0);
        enumerator.addSplitsBack(
                Collections.singletonList(new SequenceSplit(0, FAILURE_POSITION)), 0);
        enumerator.addReader(0);

        assertThat(assignedSplits(context, 0))
                .containsExactly(new SequenceSplit(0, 0), new SequenceSplit(0, FAILURE_POSITION));
    }

    @Test
    void testRestoredEnumeratorAssignsNothingOnRegistration() throws Exception {
        final MockSplitEnumeratorContext<SequenceSplit> context =
                new MockSplitEnumeratorContext<>(1);
        final SplitEnumerator<SequenceSplit, Void> enumerator =
                failingSource().restoreEnumerator(context, null);

        enumerator.addReader(0);

        assertThat(context.getSplitsAssignmentSequence()).isEmpty();
    }

    private static List<SequenceSplit> assignedSplits(
            MockSplitEnumeratorContext<SequenceSplit> context, int subtask) {
        return context.getSplitsAssignmentSequence().stream()
                .flatMap(
                        assignment ->
                                assignment
                                        .assignment()
                                        .getOrDefault(subtask, Collections.emptyList())
                                        .stream())
                .collect(Collectors.toList());
    }

    @Test
    void testSplitSerializerRoundTrip() throws Exception {
        final SequenceSplit split = new SequenceSplit(3, 42);
        final byte[] bytes = SequenceSplit.SERIALIZER.serialize(split);
        final SequenceSplit restored =
                SequenceSplit.SERIALIZER.deserialize(SequenceSplit.SERIALIZER.getVersion(), bytes);
        assertThat(restored.getSubtaskIndex()).isEqualTo(3);
        assertThat(restored.getPosition()).isEqualTo(42);
        assertThat(restored.splitId()).isEqualTo(split.splitId());
    }
}
