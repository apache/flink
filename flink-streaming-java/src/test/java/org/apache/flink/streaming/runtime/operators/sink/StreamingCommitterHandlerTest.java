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

package org.apache.flink.streaming.runtime.operators.sink;

import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.TestLogger;

import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.runtime.operators.sink.SinkTestUtil.fromRecords;
import static org.apache.flink.streaming.util.TestHarnessUtil.buildSubtaskState;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertTrue;

/** Test the {@link StreamingCommitterHandler}. */
public class StreamingCommitterHandlerTest extends TestLogger {

    @Test(expected = IllegalStateException.class)
    public void throwExceptionWithoutSerializer() throws Exception {
        final OneInputStreamOperatorTestHarness<String, byte[]> testHarness =
                createTestHarness(new TestSink.DefaultCommitter(), null);
        testHarness.initializeEmptyState();
        testHarness.open();
    }

    @Test
    public void supportRetry() throws Exception {
        final List<String> input = Arrays.asList("lazy", "leaf");
        final TestSink.RetryOnceCommitter committer = new TestSink.RetryOnceCommitter();
        final OneInputStreamOperatorTestHarness<String, byte[]> testHarness =
                createTestHarness(committer);

        testHarness.initializeEmptyState();
        testHarness.open();
        testHarness.processElements(
                input.stream().map(StreamRecord::new).collect(Collectors.toList()));
        testHarness.prepareSnapshotPreBarrier(1);
        testHarness.snapshot(1L, 1L);
        testHarness.notifyOfCompletedCheckpoint(1L);

        assertTrue(testHarness.getOutput().isEmpty());

        testHarness.snapshot(2L, 2L);
        testHarness.notifyOfCompletedCheckpoint(2L);

        assertThat(testHarness.getOutput(), hasSize(2));

        testHarness.close();

        // committedData has the format (<input>, null, <number>)
        final List<String> committedInputs =
                committer.getCommittedData().stream()
                        .map(s -> s.substring(1, s.length() - 1).split(",")[0].trim())
                        .collect(Collectors.toList());
        assertThat(committedInputs, Matchers.contains("lazy", "leaf"));
    }

    @Test
    public void closeCommitter() throws Exception {
        final TestSink.DefaultCommitter committer = new TestSink.DefaultCommitter();
        final OneInputStreamOperatorTestHarness<String, byte[]> testHarness =
                createTestHarness(committer);
        testHarness.initializeEmptyState();
        testHarness.open();
        testHarness.close();
        assertThat(committer.isClosed(), Matchers.equalTo(true));
    }

    @Test
    public void restoredFromMergedState() throws Exception {

        final List<String> input1 = Arrays.asList("today", "whom");
        final OperatorSubtaskState operatorSubtaskState1 =
                buildSubtaskState(createTestHarness(), input1);

        final List<String> input2 = Arrays.asList("future", "evil", "how");
        final OperatorSubtaskState operatorSubtaskState2 =
                buildSubtaskState(createTestHarness(), input2);

        final TestSink.DefaultCommitter committer = new TestSink.DefaultCommitter();
        final OneInputStreamOperatorTestHarness<String, byte[]> testHarness =
                createTestHarness(committer);

        final OperatorSubtaskState mergedOperatorSubtaskState =
                OneInputStreamOperatorTestHarness.repackageState(
                        operatorSubtaskState1, operatorSubtaskState2);

        testHarness.initializeState(
                OneInputStreamOperatorTestHarness.repartitionOperatorState(
                        mergedOperatorSubtaskState, 2, 2, 1, 0));
        testHarness.open();

        final List<String> expectedOutput = new ArrayList<>();
        expectedOutput.addAll(input1);
        expectedOutput.addAll(input2);

        testHarness.prepareSnapshotPreBarrier(1L);
        testHarness.snapshot(1L, 1L);
        testHarness.notifyOfCompletedCheckpoint(1);

        testHarness.close();

        String[] expectedList =
                expectedOutput.stream()
                        .map(output -> Tuple3.of(output, null, Long.MIN_VALUE).toString())
                        .toArray(String[]::new);
        assertThat(fromRecords(testHarness.getRecordOutput()), containsInAnyOrder(expectedList));

        assertThat(committer.getCommittedData(), containsInAnyOrder(expectedList));
    }

    @Test
    public void commitMultipleStagesTogether() throws Exception {

        final TestSink.DefaultCommitter committer = new TestSink.DefaultCommitter();

        final List<String> input1 = Arrays.asList("cautious", "nature");
        final List<String> input2 = Arrays.asList("count", "over");
        final List<String> input3 = Arrays.asList("lawyer", "grammar");

        final List<String> expectedOutput = new ArrayList<>();

        expectedOutput.addAll(input1);
        expectedOutput.addAll(input2);
        expectedOutput.addAll(input3);

        final OneInputStreamOperatorTestHarness<String, byte[]> testHarness =
                createTestHarness(committer);
        testHarness.initializeEmptyState();
        testHarness.open();

        testHarness.processElements(
                input1.stream().map(StreamRecord::new).collect(Collectors.toList()));
        testHarness.prepareSnapshotPreBarrier(1L);
        testHarness.snapshot(1L, 1L);
        testHarness.processElements(
                input2.stream().map(StreamRecord::new).collect(Collectors.toList()));
        testHarness.prepareSnapshotPreBarrier(2L);
        testHarness.snapshot(2L, 2L);
        testHarness.processElements(
                input3.stream().map(StreamRecord::new).collect(Collectors.toList()));
        testHarness.prepareSnapshotPreBarrier(3L);
        testHarness.snapshot(3L, 3L);

        testHarness.notifyOfCompletedCheckpoint(1);
        testHarness.notifyOfCompletedCheckpoint(3);

        testHarness.close();

        List<String> expectedList =
                expectedOutput.stream()
                        .map(output -> Tuple3.of(output, null, Long.MIN_VALUE).toString())
                        .collect(Collectors.toList());
        assertThat(fromRecords(testHarness.getRecordOutput()), equalTo(expectedList));

        assertThat(committer.getCommittedData(), equalTo(expectedList));
    }

    private OneInputStreamOperatorTestHarness<String, byte[]> createTestHarness() throws Exception {
        return createTestHarness(
                new TestSink.DefaultCommitter(), TestSink.StringCommittableSerializer.INSTANCE);
    }

    private OneInputStreamOperatorTestHarness<String, byte[]> createTestHarness(
            Committer<String> committer) throws Exception {
        return createTestHarness(committer, TestSink.StringCommittableSerializer.INSTANCE);
    }

    private OneInputStreamOperatorTestHarness<String, byte[]> createTestHarness(
            Committer<String> committer, SimpleVersionedSerializer<String> serializer)
            throws Exception {
        return new OneInputStreamOperatorTestHarness<>(
                new SinkOperatorFactory<>(
                        TestSink.newBuilder()
                                .setWriter(new TestSink.DefaultSinkWriter<String>())
                                .withWriterState()
                                .setCommittableSerializer(
                                        TestSink.StringCommittableSerializer.INSTANCE)
                                .setCommitter(committer)
                                .setCommittableSerializer(serializer)
                                .build(),
                        false,
                        true));
    }
}
