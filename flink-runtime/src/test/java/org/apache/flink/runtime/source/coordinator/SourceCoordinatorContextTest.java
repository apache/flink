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

package org.apache.flink.runtime.source.coordinator;

import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.api.connector.source.mocks.MockSourceSplitSerializer;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.source.event.AddSplitEvent;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.runtime.source.coordinator.CoordinatorTestUtils.getSplitsAssignment;
import static org.apache.flink.runtime.source.coordinator.CoordinatorTestUtils.verifyAssignment;
import static org.apache.flink.runtime.source.coordinator.CoordinatorTestUtils.verifyException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Unit test for {@link SourceCoordinatorContext}. */
public class SourceCoordinatorContextTest extends SourceCoordinatorTestBase {

    @Test
    public void testRegisterReader() {
        List<ReaderInfo> readerInfo = registerReaders();

        assertTrue(context.registeredReaders().containsKey(0));
        assertTrue(context.registeredReaders().containsKey(1));
        assertEquals(readerInfo.get(0), context.registeredReaders().get(0));
        assertEquals(readerInfo.get(1), context.registeredReaders().get(1));
    }

    @Test
    public void testUnregisterReader() {
        List<ReaderInfo> readerInfo = registerReaders();
        assertEquals(readerInfo.get(0), context.registeredReaders().get(0));

        context.unregisterSourceReader(0);
        assertEquals("Only reader 2 should be registered.", 2, context.registeredReaders().size());
        assertNull(context.registeredReaders().get(0));
        assertEquals(readerInfo.get(1), context.registeredReaders().get(1));
        assertEquals(readerInfo.get(2), context.registeredReaders().get(2));
    }

    @Test
    public void testUnregisterUnregisteredReader() {
        context.unregisterSourceReader(0);
    }

    @Test
    public void testAssignSplitsFromCoordinatorExecutor() throws Exception {
        testAssignSplits(true);
    }

    @Test
    public void testAssignSplitsFromOtherThread() throws Exception {
        testAssignSplits(false);
    }

    private void testAssignSplits(boolean fromCoordinatorExecutor) throws Exception {
        // Register the readers.
        registerReaders();

        // Assign splits to the readers.
        SplitsAssignment<MockSourceSplit> splitsAssignment = getSplitsAssignment(2, 0);
        if (fromCoordinatorExecutor) {
            coordinatorExecutor.submit(() -> context.assignSplits(splitsAssignment)).get();
        } else {
            context.assignSplits(splitsAssignment);
        }

        // The tracker should have recorded the assignments.
        verifyAssignment(
                Arrays.asList("0"), splitSplitAssignmentTracker.uncheckpointedAssignments().get(0));
        verifyAssignment(
                Arrays.asList("1", "2"),
                splitSplitAssignmentTracker.uncheckpointedAssignments().get(1));
        // The OperatorCoordinatorContext should have received the event sending call.
        assertEquals(
                "There should be two events sent to the subtasks.",
                2,
                operatorCoordinatorContext.getEventsToOperator().size());

        // Assert the events to subtask0.
        List<OperatorEvent> eventsToSubtask0 =
                operatorCoordinatorContext.getEventsToOperatorBySubtaskId(0);
        assertEquals(1, eventsToSubtask0.size());
        OperatorEvent event = eventsToSubtask0.get(0);
        assertTrue(event instanceof AddSplitEvent);
        verifyAssignment(
                Arrays.asList("0"),
                ((AddSplitEvent) event).splits(new MockSourceSplitSerializer()));
    }

    @Test
    public void testAssignSplitToUnregisteredReaderFromCoordinatorExecutor() {
        testAssignSplitToUnregisterdReader(true);
    }

    @Test
    public void testAssignSplitToUnregisteredReaderFromOtherThread() {
        testAssignSplitToUnregisterdReader(false);
    }

    private void testAssignSplitToUnregisterdReader(boolean fromCoordinatorExecutor) {
        // Assign splits to the readers.
        SplitsAssignment<MockSourceSplit> splitsAssignment = getSplitsAssignment(2, 0);
        verifyException(
                () -> {
                    if (fromCoordinatorExecutor) {
                        coordinatorExecutor
                                .submit(() -> context.assignSplits(splitsAssignment))
                                .get();
                    } else {
                        context.assignSplits(splitsAssignment);
                    }
                },
                "assignSplits() should fail to assign the splits to a reader that is not registered.",
                "Cannot assign splits");
    }

    @Test
    public void testSnapshotAndRestore() throws Exception {
        registerReaders();

        // Assign splits to the readers.
        SplitsAssignment<MockSourceSplit> splitsAssignment = getSplitsAssignment(2, 0);
        coordinatorExecutor.submit(() -> context.assignSplits(splitsAssignment)).get();
        // Take the first snapshot;
        byte[] bytes = takeSnapshot(context, 100L);

        SourceCoordinatorContext<MockSourceSplit> restoredContext;
        SplitAssignmentTracker<MockSourceSplit> restoredTracker = new SplitAssignmentTracker<>();
        SourceCoordinatorProvider.CoordinatorExecutorThreadFactory coordinatorThreadFactory =
                new SourceCoordinatorProvider.CoordinatorExecutorThreadFactory(
                        TEST_OPERATOR_ID.toHexString(), getClass().getClassLoader());

        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
                DataInputStream in = new DataInputStream(bais)) {
            restoredContext =
                    new SourceCoordinatorContext<>(
                            coordinatorExecutor,
                            coordinatorThreadFactory,
                            1,
                            operatorCoordinatorContext,
                            new MockSourceSplitSerializer(),
                            restoredTracker);
            restoredContext.restoreState(new MockSourceSplitSerializer(), in);
        }
        // FLINK-21452: do not (re)store registered readers
        assertEquals(0, restoredContext.registeredReaders().size());
        assertEquals(
                splitSplitAssignmentTracker.uncheckpointedAssignments(),
                restoredTracker.uncheckpointedAssignments());
        assertEquals(
                splitSplitAssignmentTracker.assignmentsByCheckpointId(),
                restoredTracker.assignmentsByCheckpointId());
    }

    // ------------------------

    private List<ReaderInfo> registerReaders() {
        // Register the readers.
        ReaderInfo readerInfo0 = new ReaderInfo(0, "subtask_0_location");
        ReaderInfo readerInfo1 = new ReaderInfo(1, "subtask_1_location");
        ReaderInfo readerInfo2 = new ReaderInfo(2, "subtask_1_location");
        context.registerSourceReader(readerInfo0);
        context.registerSourceReader(readerInfo1);
        context.registerSourceReader(readerInfo2);
        return Arrays.asList(readerInfo0, readerInfo1, readerInfo2);
    }

    private byte[] takeSnapshot(
            SourceCoordinatorContext<MockSourceSplit> context, long checkpointId) throws Exception {
        byte[] bytes;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputViewStreamWrapper(baos)) {
            context.snapshotState(checkpointId, new MockSourceSplitSerializer(), out);
            out.flush();
            bytes = baos.toByteArray();
        }
        return bytes;
    }
}
