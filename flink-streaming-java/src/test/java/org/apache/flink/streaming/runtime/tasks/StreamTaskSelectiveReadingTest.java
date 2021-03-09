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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.InputSelectable;
import org.apache.flink.streaming.api.operators.InputSelection;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxDefaultAction;
import org.apache.flink.streaming.util.TestAnyModeReadingStreamOperator;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.streaming.util.TestSequentialReadingStreamOperator;
import org.apache.flink.util.ExceptionUtils;

import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;

/** Test selective reading. */
public class StreamTaskSelectiveReadingTest {

    @Test
    public void testAnyOrderedReading() throws Exception {
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        expectedOutput.add(new StreamRecord<>("[Operator0-1]: Hello-1"));
        expectedOutput.add(new StreamRecord<>("[Operator0-2]: 1"));
        expectedOutput.add(new StreamRecord<>("[Operator0-1]: Hello-2"));
        expectedOutput.add(new StreamRecord<>("[Operator0-2]: 2"));
        expectedOutput.add(new StreamRecord<>("[Operator0-1]: Hello-3"));
        expectedOutput.add(new StreamRecord<>("[Operator0-2]: 3"));
        expectedOutput.add(new StreamRecord<>("[Operator0-2]: 4"));

        testBase(new TestAnyModeReadingStreamOperator("Operator0"), true, expectedOutput, true);
    }

    @Test
    public void testAnyUnorderedReading() throws Exception {
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        expectedOutput.add(new StreamRecord<>("[Operator0-1]: Hello-1"));
        expectedOutput.add(new StreamRecord<>("[Operator0-2]: 1"));
        expectedOutput.add(new StreamRecord<>("[Operator0-1]: Hello-2"));
        expectedOutput.add(new StreamRecord<>("[Operator0-2]: 2"));
        expectedOutput.add(new StreamRecord<>("[Operator0-1]: Hello-3"));
        expectedOutput.add(new StreamRecord<>("[Operator0-2]: 3"));
        expectedOutput.add(new StreamRecord<>("[Operator0-2]: 4"));

        testBase(new TestAnyModeReadingStreamOperator("Operator0"), false, expectedOutput, false);
    }

    @Test
    public void testSequentialReading() throws Exception {
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        expectedOutput.add(new StreamRecord<>("[Operator0-1]: Hello-1"));
        expectedOutput.add(new StreamRecord<>("[Operator0-1]: Hello-2"));
        expectedOutput.add(new StreamRecord<>("[Operator0-1]: Hello-3"));
        expectedOutput.add(new StreamRecord<>("[Operator0-2]: 1"));
        expectedOutput.add(new StreamRecord<>("[Operator0-2]: 2"));
        expectedOutput.add(new StreamRecord<>("[Operator0-2]: 3"));
        expectedOutput.add(new StreamRecord<>("[Operator0-2]: 4"));

        testBase(new TestSequentialReadingStreamOperator("Operator0"), false, expectedOutput, true);
    }

    @Test
    public void testSpecialRuleReading() throws Exception {
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        expectedOutput.add(new StreamRecord<>("[Operator0-1]: Hello-1"));
        expectedOutput.add(new StreamRecord<>("[Operator0-1]: Hello-2"));
        expectedOutput.add(new StreamRecord<>("[Operator0-2]: 1"));
        expectedOutput.add(new StreamRecord<>("[Operator0-2]: 2"));
        expectedOutput.add(new StreamRecord<>("[Operator0-1]: Hello-3"));
        expectedOutput.add(new StreamRecord<>("[Operator0-2]: 3"));
        expectedOutput.add(new StreamRecord<>("[Operator0-2]: 4"));

        testBase(
                new SpecialRuleReadingStreamOperator("Operator0", 3, 4, 2),
                false,
                expectedOutput,
                true);
    }

    @Test
    public void testReadFinishedInput() throws Exception {
        try {
            testBase(
                    new TestReadFinishedInputStreamOperator(),
                    false,
                    new ConcurrentLinkedQueue<>(),
                    true);
            fail("should throw an IOException");
        } catch (Exception t) {
            if (!ExceptionUtils.findThrowableWithMessage(
                            t, "only first input is selected but it is already finished")
                    .isPresent()) {
                throw t;
            }
        }
    }

    private void testBase(
            TwoInputStreamOperator<String, Integer, String> streamOperator,
            boolean prepareDataBeforeProcessing,
            ConcurrentLinkedQueue<Object> expectedOutput,
            boolean orderedCheck)
            throws Exception {

        final TwoInputStreamTaskTestHarness<String, Integer, String> testHarness =
                new TwoInputStreamTaskTestHarness<>(
                        TestSelectiveReadingTask::new,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.INT_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO);

        testHarness.setupOutputForSingletonOperatorChain();
        StreamConfig streamConfig = testHarness.getStreamConfig();
        streamConfig.setStreamOperator(streamOperator);
        streamConfig.setOperatorID(new OperatorID());

        testHarness.invoke();
        testHarness.waitForTaskRunning();

        boolean isProcessing = false;
        if (!prepareDataBeforeProcessing) {
            ((TestSelectiveReadingTask) testHarness.getTask()).startProcessing();
            isProcessing = true;
        }

        testHarness.processElement(new StreamRecord<>("Hello-1"), 0, 0);

        // wait until the input is processed to test the listening and blocking logic
        if (!prepareDataBeforeProcessing) {
            testHarness.waitForInputProcessing();
        }

        testHarness.processElement(new StreamRecord<>("Hello-2"), 0, 0);
        testHarness.processElement(new StreamRecord<>("Hello-3"), 0, 0);

        testHarness.processElement(new StreamRecord<>(1), 1, 0);
        testHarness.processElement(new StreamRecord<>(2), 1, 0);
        testHarness.processElement(new StreamRecord<>(3), 1, 0);
        testHarness.processElement(new StreamRecord<>(4), 1, 0);

        testHarness.endInput();

        if (!isProcessing) {
            ((TestSelectiveReadingTask) testHarness.getTask()).startProcessing();
        }
        testHarness.waitForTaskCompletion(10_000L);

        LinkedBlockingQueue<Object> output = testHarness.getOutput();
        if (orderedCheck) {
            TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, output);
        } else {
            String[] expectedResult =
                    expectedOutput.stream()
                            .map(record -> ((StreamRecord) record).getValue().toString())
                            .toArray(String[]::new);
            Arrays.sort(expectedResult);

            String[] result =
                    output.stream()
                            .map(record -> ((StreamRecord) record).getValue().toString())
                            .toArray(String[]::new);
            Arrays.sort(result);

            assertArrayEquals("Output was not correct.", expectedResult, result);
        }
    }

    // ------------------------------------------------------------------------
    // Utilities
    // ------------------------------------------------------------------------

    private static class TestSelectiveReadingTask<IN1, IN2, OUT>
            extends TwoInputStreamTask<IN1, IN2, OUT> {

        private volatile boolean started;

        TestSelectiveReadingTask(Environment env) throws Exception {
            super(env);
            started = false;
        }

        @Override
        protected void processInput(MailboxDefaultAction.Controller controller) throws Exception {
            if (!started) {
                synchronized (this) {
                    this.wait();
                }
            }

            super.processInput(controller);
        }

        public void startProcessing() {
            started = true;
            synchronized (this) {
                this.notifyAll();
            }
        }
    }

    private static class SpecialRuleReadingStreamOperator extends AbstractStreamOperator<String>
            implements TwoInputStreamOperator<String, Integer, String>,
                    InputSelectable,
                    BoundedMultiInput {

        private final String name;

        private final int input1Records;
        private final int input2Records;

        private final int maxContinuousReadingRecords;

        private int input1ReadingRecords;
        private int input2ReadingRecords;

        private int continuousReadingRecords;
        private InputSelection inputSelection;

        SpecialRuleReadingStreamOperator(
                String name,
                int input1Records,
                int input2Records,
                int maxContinuousReadingRecords) {
            super();

            this.name = name;
            this.input1Records = input1Records;
            this.input2Records = input2Records;
            this.maxContinuousReadingRecords = maxContinuousReadingRecords;

            this.input1ReadingRecords = 0;
            this.input2ReadingRecords = 0;
            this.continuousReadingRecords = 0;
            this.inputSelection = InputSelection.FIRST;
        }

        @Override
        public InputSelection nextSelection() {
            return inputSelection;
        }

        @Override
        public void processElement1(StreamRecord<String> element) {
            output.collect(element.replace("[" + name + "-1]: " + element.getValue()));

            input1ReadingRecords++;
            continuousReadingRecords++;
            if (continuousReadingRecords == maxContinuousReadingRecords) {
                continuousReadingRecords = 0;
                if (input2ReadingRecords < input2Records) {
                    inputSelection = InputSelection.SECOND;
                    return;
                }
            }

            inputSelection = InputSelection.FIRST;
        }

        @Override
        public void processElement2(StreamRecord<Integer> element) {
            output.collect(element.replace("[" + name + "-2]: " + element.getValue()));

            input2ReadingRecords++;
            continuousReadingRecords++;
            if (continuousReadingRecords == maxContinuousReadingRecords) {
                continuousReadingRecords = 0;
                if (input1ReadingRecords < input1Records) {
                    inputSelection = InputSelection.FIRST;
                    return;
                }
            }

            inputSelection = InputSelection.SECOND;
        }

        @Override
        public void endInput(int inputId) {
            inputSelection = (inputId == 1) ? InputSelection.SECOND : InputSelection.FIRST;
        }
    }

    private static class TestReadFinishedInputStreamOperator extends AbstractStreamOperator<String>
            implements TwoInputStreamOperator<String, Integer, String>, InputSelectable {

        private InputSelection inputSelection;

        TestReadFinishedInputStreamOperator() {
            super();

            this.inputSelection = InputSelection.FIRST;
        }

        @Override
        public InputSelection nextSelection() {
            return inputSelection;
        }

        @Override
        public void processElement1(StreamRecord<String> element) {}

        @Override
        public void processElement2(StreamRecord<Integer> element) {}
    }
}
