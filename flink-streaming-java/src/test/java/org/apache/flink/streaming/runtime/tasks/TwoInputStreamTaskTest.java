/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Metric;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.io.AvailabilityProvider;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.EndOfData;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.runtime.io.network.api.writer.RecordOrEventCollectingResultPartitionWriter;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriterWithAvailabilityHelper;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.NoOpMetricRegistry;
import org.apache.flink.runtime.metrics.groups.InternalOperatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.metrics.util.InterceptingOperatorMetricGroup;
import org.apache.flink.runtime.metrics.util.InterceptingTaskMetricGroup;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.RichCoMapFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.InputSelectable;
import org.apache.flink.streaming.api.operators.InputSelection;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.operators.co.CoStreamMap;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.util.OutputTag;

import org.hamcrest.collection.IsMapContaining;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createExecutionAttemptId;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Tests for {@link TwoInputStreamTask}.
 *
 * <p>Note:<br>
 * We only use a {@link CoStreamMap} operator here. We also test the individual operators but Map is
 * used as a representative to test {@link TwoInputStreamTask}, since {@link TwoInputStreamTask} is
 * used for all {@link TwoInputStreamOperator}s.
 */
public class TwoInputStreamTaskTest {

    /**
     * This test verifies that open() and close() are correctly called. This test also verifies that
     * timestamps of emitted elements are correct. {@link CoStreamMap} assigns the input timestamp
     * to emitted elements.
     */
    @Test
    public void testOpenCloseAndTimestamps() throws Exception {
        final TwoInputStreamTaskTestHarness<String, Integer, String> testHarness =
                new TwoInputStreamTaskTestHarness<>(
                        TwoInputStreamTask::new,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.INT_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO);
        testHarness.setupOutputForSingletonOperatorChain();

        StreamConfig streamConfig = testHarness.getStreamConfig();
        CoStreamMap<String, Integer, String> coMapOperator =
                new CoStreamMap<>(new TestOpenCloseMapFunction());
        streamConfig.setStreamOperator(coMapOperator);
        streamConfig.setOperatorID(new OperatorID());

        long initialTime = 0L;
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        testHarness.invoke();
        testHarness.waitForTaskRunning();

        testHarness.processElement(new StreamRecord<>("Hello", initialTime + 1), 0, 0);
        expectedOutput.add(new StreamRecord<>("Hello", initialTime + 1));

        // wait until the input is processed to ensure ordering of the output
        testHarness.waitForInputProcessing();

        testHarness.processElement(new StreamRecord<>(1337, initialTime + 2), 1, 0);

        expectedOutput.add(new StreamRecord<>("1337", initialTime + 2));

        testHarness.waitForInputProcessing();

        testHarness.endInput();

        testHarness.waitForTaskCompletion();

        Assert.assertTrue(
                "RichFunction methods were not called.", TestOpenCloseMapFunction.closeCalled);

        TestHarnessUtil.assertOutputEquals(
                "Output was not correct.", expectedOutput, testHarness.getOutput());
    }

    /**
     * This test verifies that watermarks and watermark statuses are correctly forwarded. This also
     * checks whether watermarks are forwarded only when we have received watermarks from all
     * inputs. The forwarded watermark must be the minimum of the watermarks of all active inputs.
     */
    @Test
    public void testWatermarkAndWatermarkStatusForwarding() throws Exception {

        final TwoInputStreamTaskTestHarness<String, Integer, String> testHarness =
                new TwoInputStreamTaskTestHarness<>(
                        TwoInputStreamTask::new,
                        2,
                        2,
                        new int[] {1, 2},
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.INT_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO);
        testHarness.setupOutputForSingletonOperatorChain();

        StreamConfig streamConfig = testHarness.getStreamConfig();
        CoStreamMap<String, Integer, String> coMapOperator = new CoStreamMap<>(new IdentityMap());
        streamConfig.setStreamOperator(coMapOperator);
        streamConfig.setOperatorID(new OperatorID());

        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        long initialTime = 0L;

        testHarness.invoke();
        testHarness.waitForTaskRunning();

        testHarness.processElement(new Watermark(initialTime), 0, 0);
        testHarness.processElement(new Watermark(initialTime), 0, 1);

        testHarness.processElement(new Watermark(initialTime), 1, 0);

        // now the output should still be empty
        testHarness.waitForInputProcessing();
        TestHarnessUtil.assertOutputEquals(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.processElement(new Watermark(initialTime), 1, 1);

        // now the watermark should have propagated, Map simply forward Watermarks
        testHarness.waitForInputProcessing();
        expectedOutput.add(new Watermark(initialTime));
        TestHarnessUtil.assertOutputEquals(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // contrary to checkpoint barriers these elements are not blocked by watermarks
        testHarness.processElement(new StreamRecord<>("Hello", initialTime), 0, 0);
        testHarness.processElement(new StreamRecord<>(42, initialTime), 1, 1);
        expectedOutput.add(new StreamRecord<>("Hello", initialTime));
        expectedOutput.add(new StreamRecord<>("42", initialTime));

        testHarness.waitForInputProcessing();
        TestHarnessUtil.assertOutputEquals(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.processElement(new Watermark(initialTime + 4), 0, 0);
        testHarness.processElement(new Watermark(initialTime + 3), 0, 1);
        testHarness.processElement(new Watermark(initialTime + 3), 1, 0);
        testHarness.processElement(new Watermark(initialTime + 2), 1, 1);

        // check whether we get the minimum of all the watermarks, this must also only occur in
        // the output after the two StreamRecords
        expectedOutput.add(new Watermark(initialTime + 2));
        testHarness.waitForInputProcessing();
        TestHarnessUtil.assertOutputEquals(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // advance watermark from one of the inputs, now we should get a new one since the
        // minimum increases
        testHarness.processElement(new Watermark(initialTime + 4), 1, 1);
        testHarness.waitForInputProcessing();
        expectedOutput.add(new Watermark(initialTime + 3));
        TestHarnessUtil.assertOutputEquals(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // advance the other two inputs, now we should get a new one since the
        // minimum increases again
        testHarness.processElement(new Watermark(initialTime + 4), 0, 1);
        testHarness.processElement(new Watermark(initialTime + 4), 1, 0);
        testHarness.waitForInputProcessing();
        expectedOutput.add(new Watermark(initialTime + 4));
        TestHarnessUtil.assertOutputEquals(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // test whether idle input channels are acknowledged correctly when forwarding watermarks
        testHarness.processElement(WatermarkStatus.IDLE, 0, 1);
        testHarness.processElement(WatermarkStatus.IDLE, 1, 0);
        testHarness.processElement(new Watermark(initialTime + 6), 0, 0);
        testHarness.processElement(
                new Watermark(initialTime + 5), 1, 1); // this watermark should be advanced first
        testHarness.processElement(WatermarkStatus.IDLE, 1, 1); // once this is acknowledged,

        testHarness.waitForInputProcessing();
        expectedOutput.add(new Watermark(initialTime + 5));
        expectedOutput.add(new Watermark(initialTime + 6));
        TestHarnessUtil.assertOutputEquals(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // make all input channels idle and check that the operator's idle status is forwarded
        testHarness.processElement(WatermarkStatus.IDLE, 0, 0);
        testHarness.waitForInputProcessing();
        expectedOutput.add(WatermarkStatus.IDLE);
        TestHarnessUtil.assertOutputEquals(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // make some input channels active again and check that the operator's active status is
        // forwarded only once
        testHarness.processElement(WatermarkStatus.ACTIVE, 1, 0);
        testHarness.processElement(WatermarkStatus.ACTIVE, 0, 1);
        testHarness.waitForInputProcessing();
        expectedOutput.add(WatermarkStatus.ACTIVE);
        TestHarnessUtil.assertOutputEquals(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.endInput();

        testHarness.waitForTaskCompletion();

        List<String> resultElements =
                TestHarnessUtil.getRawElementsFromOutput(testHarness.getOutput());
        Assert.assertEquals(2, resultElements.size());
    }

    /** This test verifies that checkpoint barriers are correctly forwarded. */
    @Test
    public void testCheckpointBarriers() throws Exception {

        final TwoInputStreamTaskTestHarness<String, Integer, String> testHarness =
                new TwoInputStreamTaskTestHarness<>(
                        TwoInputStreamTask::new,
                        2,
                        2,
                        new int[] {1, 2},
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.INT_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO);
        testHarness.setupOutputForSingletonOperatorChain();

        StreamConfig streamConfig = testHarness.getStreamConfig();
        CoStreamMap<String, Integer, String> coMapOperator = new CoStreamMap<>(new IdentityMap());
        streamConfig.setStreamOperator(coMapOperator);
        streamConfig.setOperatorID(new OperatorID());

        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        long initialTime = 0L;

        testHarness.invoke();
        testHarness.waitForTaskRunning();

        testHarness.processEvent(
                new CheckpointBarrier(0, 0, CheckpointOptions.forCheckpointWithDefaultLocation()),
                0,
                0);

        // This one should go through
        testHarness.processElement(new StreamRecord<>("Ciao-0-0", initialTime), 0, 1);
        expectedOutput.add(new StreamRecord<>("Ciao-0-0", initialTime));

        testHarness.waitForInputProcessing();

        // These elements should be forwarded, since we did not yet receive a checkpoint barrier
        // on that input, only add to same input, otherwise we would not know the ordering
        // of the output since the Task might read the inputs in any order
        testHarness.processElement(new StreamRecord<>(11, initialTime), 1, 1);
        testHarness.processElement(new StreamRecord<>(111, initialTime), 1, 1);
        expectedOutput.add(new StreamRecord<>("11", initialTime));
        expectedOutput.add(new StreamRecord<>("111", initialTime));

        testHarness.waitForInputProcessing();

        // Wait to allow input to end up in the output.
        // TODO Use count down latches instead as a cleaner solution
        for (int i = 0; i < 20; ++i) {
            if (testHarness.getOutput().size() >= expectedOutput.size()) {
                break;
            } else {
                Thread.sleep(100);
            }
        }

        // we should not yet see the barrier, only the two elements from non-blocked input
        TestHarnessUtil.assertOutputEquals(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.processEvent(
                new CheckpointBarrier(0, 0, CheckpointOptions.forCheckpointWithDefaultLocation()),
                0,
                1);
        testHarness.processEvent(
                new CheckpointBarrier(0, 0, CheckpointOptions.forCheckpointWithDefaultLocation()),
                1,
                0);
        testHarness.processEvent(
                new CheckpointBarrier(0, 0, CheckpointOptions.forCheckpointWithDefaultLocation()),
                1,
                1);

        testHarness.waitForInputProcessing();
        testHarness.endInput();
        testHarness.waitForTaskCompletion();

        // now we should see the barrier
        expectedOutput.add(
                new CheckpointBarrier(0, 0, CheckpointOptions.forCheckpointWithDefaultLocation()));

        TestHarnessUtil.assertOutputEquals(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        List<String> resultElements =
                TestHarnessUtil.getRawElementsFromOutput(testHarness.getOutput());
        Assert.assertEquals(3, resultElements.size());
    }

    /**
     * This test verifies that checkpoint barriers and barrier buffers work correctly with
     * concurrent checkpoint barriers where one checkpoint is "overtaking" another checkpoint, i.e.
     * some inputs receive barriers from an earlier checkpoint, thereby blocking, then all inputs
     * receive barriers from a later checkpoint.
     */
    @Test
    public void testOvertakingCheckpointBarriers() throws Exception {

        final TwoInputStreamTaskTestHarness<String, Integer, String> testHarness =
                new TwoInputStreamTaskTestHarness<>(
                        TwoInputStreamTask::new,
                        2,
                        2,
                        new int[] {1, 2},
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.INT_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO);

        testHarness.setupOutputForSingletonOperatorChain();

        StreamConfig streamConfig = testHarness.getStreamConfig();
        CoStreamMap<String, Integer, String> coMapOperator = new CoStreamMap<>(new IdentityMap());
        streamConfig.setStreamOperator(coMapOperator);
        streamConfig.setOperatorID(new OperatorID());

        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        long initialTime = 0L;

        testHarness.invoke();
        testHarness.waitForTaskRunning();

        testHarness.processEvent(
                new CheckpointBarrier(0, 0, CheckpointOptions.forCheckpointWithDefaultLocation()),
                0,
                0);

        // These elements should be forwarded, since we did not yet receive a checkpoint barrier
        // on that input, only add to same input, otherwise we would not know the ordering
        // of the output since the Task might read the inputs in any order
        testHarness.processElement(new StreamRecord<>(42, initialTime), 1, 1);
        testHarness.processElement(new StreamRecord<>(1337, initialTime), 1, 1);
        expectedOutput.add(new StreamRecord<>("42", initialTime));
        expectedOutput.add(new StreamRecord<>("1337", initialTime));

        testHarness.waitForInputProcessing();
        // we should not yet see the barrier, only the two elements from non-blocked input
        TestHarnessUtil.assertOutputEquals(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // Now give a later barrier to all inputs, this should unblock the first channel
        testHarness.processEvent(
                new CheckpointBarrier(1, 1, CheckpointOptions.forCheckpointWithDefaultLocation()),
                0,
                1);
        testHarness.processEvent(
                new CheckpointBarrier(1, 1, CheckpointOptions.forCheckpointWithDefaultLocation()),
                0,
                0);
        testHarness.processEvent(
                new CheckpointBarrier(1, 1, CheckpointOptions.forCheckpointWithDefaultLocation()),
                1,
                0);
        testHarness.processEvent(
                new CheckpointBarrier(1, 1, CheckpointOptions.forCheckpointWithDefaultLocation()),
                1,
                1);

        expectedOutput.add(new CancelCheckpointMarker(0));
        expectedOutput.add(
                new CheckpointBarrier(1, 1, CheckpointOptions.forCheckpointWithDefaultLocation()));

        testHarness.waitForInputProcessing();

        TestHarnessUtil.assertOutputEquals(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // Then give the earlier barrier, these should be ignored
        testHarness.processEvent(
                new CheckpointBarrier(0, 0, CheckpointOptions.forCheckpointWithDefaultLocation()),
                0,
                1);
        testHarness.processEvent(
                new CheckpointBarrier(0, 0, CheckpointOptions.forCheckpointWithDefaultLocation()),
                1,
                0);
        testHarness.processEvent(
                new CheckpointBarrier(0, 0, CheckpointOptions.forCheckpointWithDefaultLocation()),
                1,
                1);

        testHarness.waitForInputProcessing();

        testHarness.endInput();

        testHarness.waitForTaskCompletion();

        TestHarnessUtil.assertOutputEquals(
                "Output was not correct.", expectedOutput, testHarness.getOutput());
    }

    @Test
    public void testOperatorMetricReuse() throws Exception {
        final TwoInputStreamTaskTestHarness<String, String, String> testHarness =
                new TwoInputStreamTaskTestHarness<>(
                        TwoInputStreamTask::new,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO);

        testHarness
                .setupOperatorChain(new OperatorID(), new DuplicatingOperator())
                .chain(
                        new OperatorID(),
                        new OneInputStreamTaskTest.DuplicatingOperator(),
                        BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()))
                .chain(
                        new OperatorID(),
                        new OneInputStreamTaskTest.DuplicatingOperator(),
                        BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()))
                .finish();

        final TaskMetricGroup taskMetricGroup =
                TaskManagerMetricGroup.createTaskManagerMetricGroup(
                                NoOpMetricRegistry.INSTANCE, "host", ResourceID.generate())
                        .addJob(new JobID(), "jobname")
                        .addTask(createExecutionAttemptId(), "task");

        final StreamMockEnvironment env =
                new StreamMockEnvironment(
                        testHarness.jobConfig,
                        testHarness.taskConfig,
                        testHarness.memorySize,
                        new MockInputSplitProvider(),
                        testHarness.bufferSize,
                        new TestTaskStateManager()) {
                    @Override
                    public TaskMetricGroup getMetricGroup() {
                        return taskMetricGroup;
                    }
                };

        final Counter numRecordsInCounter =
                taskMetricGroup.getIOMetricGroup().getNumRecordsInCounter();
        final Counter numRecordsOutCounter =
                taskMetricGroup.getIOMetricGroup().getNumRecordsOutCounter();

        testHarness.invoke(env);
        testHarness.waitForTaskRunning();

        final int numRecords1 = 5;
        final int numRecords2 = 3;

        for (int x = 0; x < numRecords1; x++) {
            testHarness.processElement(new StreamRecord<>("hello"), 0, 0);
        }

        for (int x = 0; x < numRecords2; x++) {
            testHarness.processElement(new StreamRecord<>("hello"), 1, 0);
        }
        testHarness.waitForInputProcessing();

        assertEquals(numRecords1 + numRecords2, numRecordsInCounter.getCount());
        assertEquals((numRecords1 + numRecords2) * 2 * 2 * 2, numRecordsOutCounter.getCount());

        testHarness.endInput();
        testHarness.waitForTaskCompletion();
    }

    @Test
    public void testSkipExecutionsIfFinishedOnRestore() throws Exception {
        OperatorID nonSourceOperatorId = new OperatorID();

        try (StreamTaskMailboxTestHarness<String> testHarness =
                new StreamTaskMailboxTestHarnessBuilder<>(
                                TwoInputStreamTask::new, BasicTypeInfo.STRING_TYPE_INFO)
                        .setCollectNetworkEvents()
                        .modifyStreamConfig(config -> config.setCheckpointingEnabled(true))
                        .addInput(BasicTypeInfo.INT_TYPE_INFO)
                        .addInput(BasicTypeInfo.INT_TYPE_INFO)
                        .setTaskStateSnapshot(1, TaskStateSnapshot.FINISHED_ON_RESTORE)
                        .setupOperatorChain(
                                nonSourceOperatorId, new TestFinishedOnRestoreStreamOperator())
                        .finishForSingletonOperatorChain(StringSerializer.INSTANCE)
                        .build()) {

            testHarness.processElement(Watermark.MAX_WATERMARK, 0);
            testHarness.processElement(Watermark.MAX_WATERMARK, 1);
            testHarness.waitForTaskCompletion();
            assertThat(
                    testHarness.getOutput(),
                    contains(Watermark.MAX_WATERMARK, new EndOfData(StopMode.DRAIN)));
        }
    }

    static class DuplicatingOperator extends AbstractStreamOperator<String>
            implements TwoInputStreamOperator<String, String, String>, InputSelectable {

        @Override
        public void processElement1(StreamRecord<String> element) {
            output.collect(element);
            output.collect(element);
        }

        @Override
        public void processElement2(StreamRecord<String> element) {
            output.collect(element);
            output.collect(element);
        }

        @Override
        public InputSelection nextSelection() {
            return InputSelection.ALL;
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testWatermarkMetrics() throws Exception {
        final TwoInputStreamTaskTestHarness<String, Integer, String> testHarness =
                new TwoInputStreamTaskTestHarness<>(
                        TwoInputStreamTask::new,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.INT_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO);

        CoStreamMap<String, Integer, String> headOperator = new CoStreamMap<>(new IdentityMap());
        final OperatorID headOperatorId = new OperatorID();

        OneInputStreamTaskTest.WatermarkMetricOperator chainedOperator =
                new OneInputStreamTaskTest.WatermarkMetricOperator();
        OperatorID chainedOperatorId = new OperatorID();

        testHarness
                .setupOperatorChain(headOperatorId, headOperator)
                .chain(
                        chainedOperatorId,
                        chainedOperator,
                        BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()))
                .finish();

        InterceptingOperatorMetricGroup headOperatorMetricGroup =
                new InterceptingOperatorMetricGroup();
        InterceptingOperatorMetricGroup chainedOperatorMetricGroup =
                new InterceptingOperatorMetricGroup();
        InterceptingTaskMetricGroup taskMetricGroup =
                new InterceptingTaskMetricGroup() {
                    @Override
                    public InternalOperatorMetricGroup getOrAddOperator(
                            OperatorID id, String name) {
                        if (id.equals(headOperatorId)) {
                            return headOperatorMetricGroup;
                        } else if (id.equals(chainedOperatorId)) {
                            return chainedOperatorMetricGroup;
                        } else {
                            return super.getOrAddOperator(id, name);
                        }
                    }
                };

        StreamMockEnvironment env =
                new StreamMockEnvironment(
                        testHarness.jobConfig,
                        testHarness.taskConfig,
                        testHarness.memorySize,
                        new MockInputSplitProvider(),
                        testHarness.bufferSize,
                        new TestTaskStateManager()) {
                    @Override
                    public TaskMetricGroup getMetricGroup() {
                        return taskMetricGroup;
                    }
                };

        testHarness.invoke(env);
        testHarness.waitForTaskRunning();

        Gauge<Long> taskInputWatermarkGauge =
                (Gauge<Long>) taskMetricGroup.get(MetricNames.IO_CURRENT_INPUT_WATERMARK);
        Gauge<Long> headInput1WatermarkGauge =
                (Gauge<Long>) headOperatorMetricGroup.get(MetricNames.IO_CURRENT_INPUT_1_WATERMARK);
        Gauge<Long> headInput2WatermarkGauge =
                (Gauge<Long>) headOperatorMetricGroup.get(MetricNames.IO_CURRENT_INPUT_2_WATERMARK);
        Gauge<Long> headInputWatermarkGauge =
                (Gauge<Long>) headOperatorMetricGroup.get(MetricNames.IO_CURRENT_INPUT_WATERMARK);
        Gauge<Long> headOutputWatermarkGauge =
                (Gauge<Long>) headOperatorMetricGroup.get(MetricNames.IO_CURRENT_OUTPUT_WATERMARK);
        Gauge<Long> chainedInputWatermarkGauge =
                (Gauge<Long>)
                        chainedOperatorMetricGroup.get(MetricNames.IO_CURRENT_INPUT_WATERMARK);
        Gauge<Long> chainedOutputWatermarkGauge =
                (Gauge<Long>)
                        chainedOperatorMetricGroup.get(MetricNames.IO_CURRENT_OUTPUT_WATERMARK);

        Assert.assertEquals(
                "A metric was registered multiple times.",
                7,
                new HashSet<>(
                                Arrays.asList(
                                        taskInputWatermarkGauge,
                                        headInput1WatermarkGauge,
                                        headInput2WatermarkGauge,
                                        headInputWatermarkGauge,
                                        headOutputWatermarkGauge,
                                        chainedInputWatermarkGauge,
                                        chainedOutputWatermarkGauge))
                        .size());

        Assert.assertEquals(Long.MIN_VALUE, taskInputWatermarkGauge.getValue().longValue());
        Assert.assertEquals(Long.MIN_VALUE, headInputWatermarkGauge.getValue().longValue());
        Assert.assertEquals(Long.MIN_VALUE, headInput1WatermarkGauge.getValue().longValue());
        Assert.assertEquals(Long.MIN_VALUE, headInput2WatermarkGauge.getValue().longValue());
        Assert.assertEquals(Long.MIN_VALUE, headOutputWatermarkGauge.getValue().longValue());
        Assert.assertEquals(Long.MIN_VALUE, chainedInputWatermarkGauge.getValue().longValue());
        Assert.assertEquals(Long.MIN_VALUE, chainedOutputWatermarkGauge.getValue().longValue());

        testHarness.processElement(new Watermark(1L), 0, 0);
        testHarness.waitForInputProcessing();
        Assert.assertEquals(Long.MIN_VALUE, taskInputWatermarkGauge.getValue().longValue());
        Assert.assertEquals(Long.MIN_VALUE, headInputWatermarkGauge.getValue().longValue());
        Assert.assertEquals(1L, headInput1WatermarkGauge.getValue().longValue());
        Assert.assertEquals(Long.MIN_VALUE, headInput2WatermarkGauge.getValue().longValue());
        Assert.assertEquals(Long.MIN_VALUE, headOutputWatermarkGauge.getValue().longValue());
        Assert.assertEquals(Long.MIN_VALUE, chainedInputWatermarkGauge.getValue().longValue());
        Assert.assertEquals(Long.MIN_VALUE, chainedOutputWatermarkGauge.getValue().longValue());

        testHarness.processElement(new Watermark(2L), 1, 0);
        testHarness.waitForInputProcessing();
        Assert.assertEquals(1L, taskInputWatermarkGauge.getValue().longValue());
        Assert.assertEquals(1L, headInputWatermarkGauge.getValue().longValue());
        Assert.assertEquals(1L, headInput1WatermarkGauge.getValue().longValue());
        Assert.assertEquals(2L, headInput2WatermarkGauge.getValue().longValue());
        Assert.assertEquals(1L, headOutputWatermarkGauge.getValue().longValue());
        Assert.assertEquals(1L, chainedInputWatermarkGauge.getValue().longValue());
        Assert.assertEquals(2L, chainedOutputWatermarkGauge.getValue().longValue());

        testHarness.processElement(new Watermark(3L), 0, 0);
        testHarness.waitForInputProcessing();
        Assert.assertEquals(2L, taskInputWatermarkGauge.getValue().longValue());
        Assert.assertEquals(2L, headInputWatermarkGauge.getValue().longValue());
        Assert.assertEquals(3L, headInput1WatermarkGauge.getValue().longValue());
        Assert.assertEquals(2L, headInput2WatermarkGauge.getValue().longValue());
        Assert.assertEquals(2L, headOutputWatermarkGauge.getValue().longValue());
        Assert.assertEquals(2L, chainedInputWatermarkGauge.getValue().longValue());
        Assert.assertEquals(4L, chainedOutputWatermarkGauge.getValue().longValue());

        testHarness.endInput();
        testHarness.waitForTaskCompletion();
    }

    @Test
    public void testClosingAllOperatorsOnChainProperly() throws Exception {
        final TwoInputStreamTaskTestHarness<String, String, String> testHarness =
                new TwoInputStreamTaskTestHarness<>(
                        TwoInputStreamTask::new,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO);

        testHarness
                .setupOperatorChain(new OperatorID(), new TestBoundedTwoInputOperator("Operator0"))
                .chain(
                        new OperatorID(),
                        new TestBoundedOneInputStreamOperator("Operator1"),
                        BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()))
                .finish();

        testHarness.invoke();
        testHarness.waitForTaskRunning();

        testHarness.processElement(new StreamRecord<>("Hello-1"), 0, 0);
        testHarness.endInput(0, 0);

        testHarness.waitForInputProcessing();

        testHarness.processElement(new StreamRecord<>("Hello-2"), 1, 0);
        testHarness.endInput(1, 0);

        testHarness.waitForTaskCompletion();

        ArrayList<Object> expected = new ArrayList<>();
        Collections.addAll(
                expected,
                new StreamRecord<>("[Operator0-1]: Hello-1"),
                new StreamRecord<>("[Operator0-1]: End of input"),
                new StreamRecord<>("[Operator0-2]: Hello-2"),
                new StreamRecord<>("[Operator0-2]: End of input"),
                new StreamRecord<>("[Operator0]: Finish"),
                new StreamRecord<>("[Operator1]: End of input"),
                new StreamRecord<>("[Operator1]: Finish"));

        final Object[] output = testHarness.getOutput().toArray();
        assertArrayEquals("Output was not correct.", expected.toArray(), output);
    }

    /**
     * Tests the checkpoint related metrics are registered into {@link TaskIOMetricGroup} correctly
     * while generating the {@link TwoInputStreamTask}.
     */
    @Test
    public void testCheckpointBarrierMetrics() throws Exception {
        final TwoInputStreamTaskTestHarness<String, Integer, String> testHarness =
                new TwoInputStreamTaskTestHarness<>(
                        TwoInputStreamTask::new,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.INT_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO);
        final StreamConfig streamConfig = testHarness.getStreamConfig();
        final CoStreamMap<String, Integer, String> coMapOperator =
                new CoStreamMap<>(new IdentityMap());
        testHarness.setupOutputForSingletonOperatorChain();
        streamConfig.setStreamOperator(coMapOperator);

        final Map<String, Metric> metrics = new ConcurrentHashMap<>();
        final TaskMetricGroup taskMetricGroup =
                StreamTaskTestHarness.createTaskMetricGroup(metrics);
        final StreamMockEnvironment environment = testHarness.createEnvironment();
        environment.setTaskMetricGroup(taskMetricGroup);

        testHarness.invoke(environment);
        testHarness.waitForTaskRunning();

        assertThat(metrics, IsMapContaining.hasKey(MetricNames.CHECKPOINT_ALIGNMENT_TIME));
        assertThat(metrics, IsMapContaining.hasKey(MetricNames.CHECKPOINT_START_DELAY_TIME));

        testHarness.endInput();
        testHarness.waitForTaskCompletion();
    }

    /** The CanEmitBatchOfRecords should always be false for {@link TwoInputStreamTask}. */
    @Test
    public void testCanEmitBatchOfRecords() throws Exception {
        AvailabilityProvider.AvailabilityHelper availabilityHelper =
                new AvailabilityProvider.AvailabilityHelper();
        try (StreamTaskMailboxTestHarness<String> testHarness =
                new StreamTaskMailboxTestHarnessBuilder<>(
                                TwoInputStreamTask::new, BasicTypeInfo.STRING_TYPE_INFO)
                        .addInput(BasicTypeInfo.STRING_TYPE_INFO)
                        .addInput(BasicTypeInfo.STRING_TYPE_INFO)
                        .addAdditionalOutput(
                                new ResultPartitionWriterWithAvailabilityHelper(availabilityHelper))
                        .setupOperatorChain(new DuplicatingOperator())
                        .finishForSingletonOperatorChain(IntSerializer.INSTANCE)
                        .build()) {
            StreamTask.CanEmitBatchOfRecordsChecker canEmitBatchOfRecordsChecker =
                    testHarness.streamTask.getCanEmitBatchOfRecords();
            testHarness.processAll();

            availabilityHelper.resetAvailable();
            assertFalse(canEmitBatchOfRecordsChecker.check());

            // The canEmitBatchOfRecordsChecker should be the false after the record writer is
            // unavailable.
            availabilityHelper.resetUnavailable();
            assertFalse(canEmitBatchOfRecordsChecker.check());

            // Restore record writer to available
            availabilityHelper.resetAvailable();
            assertFalse(canEmitBatchOfRecordsChecker.check());

            // The canEmitBatchOfRecordsChecker should be the false after add the mail to mail box.
            testHarness.streamTask.mainMailboxExecutor.execute(() -> {}, "mail");
            assertFalse(canEmitBatchOfRecordsChecker.check());

            testHarness.processAll();
            assertFalse(canEmitBatchOfRecordsChecker.check());
        }
    }

    @Test
    public void testTaskSideOutputStatistics() throws Exception {
        TaskMetricGroup taskMetricGroup =
                UnregisteredMetricGroups.createUnregisteredTaskMetricGroup();

        ResultPartitionWriter[] partitionWriters = new ResultPartitionWriter[3];
        for (int i = 0; i < partitionWriters.length; ++i) {
            partitionWriters[i] =
                    new RecordOrEventCollectingResultPartitionWriter<>(
                            new ArrayDeque<>(),
                            new StreamElementSerializer<>(
                                    BasicTypeInfo.INT_TYPE_INFO.createSerializer(
                                            new ExecutionConfig())));
            partitionWriters[i].setup();
        }

        try (StreamTaskMailboxTestHarness<Integer> testHarness =
                new StreamTaskMailboxTestHarnessBuilder<>(
                                TwoInputStreamTask::new, BasicTypeInfo.INT_TYPE_INFO)
                        .addInput(BasicTypeInfo.INT_TYPE_INFO)
                        .addInput(BasicTypeInfo.INT_TYPE_INFO)
                        .addAdditionalOutput(partitionWriters)
                        .setupOperatorChain(new OperatorID(), new PassThroughOperator<>())
                        .chain(BasicTypeInfo.INT_TYPE_INFO.createSerializer(new ExecutionConfig()))
                        .setOperatorFactory(
                                SimpleOperatorFactory.of(
                                        new OneInputStreamTaskTest.OddEvenOperator()))
                        .addNonChainedOutputsCount(
                                new OutputTag<>("odd", BasicTypeInfo.INT_TYPE_INFO), 2)
                        .addNonChainedOutputsCount(1)
                        .build()
                        .chain(BasicTypeInfo.INT_TYPE_INFO.createSerializer(new ExecutionConfig()))
                        .setOperatorFactory(
                                SimpleOperatorFactory.of(
                                        new OneInputStreamTaskTest.DuplicatingOperator()))
                        .addNonChainedOutputsCount(1)
                        .build()
                        .finish()
                        .setTaskMetricGroup(taskMetricGroup)
                        .build()) {
            Counter numRecordsInCounter =
                    taskMetricGroup.getIOMetricGroup().getNumRecordsInCounter();
            Counter numRecordsOutCounter =
                    taskMetricGroup.getIOMetricGroup().getNumRecordsOutCounter();

            final int numEvenRecords = 5;
            final int numOddRecords = 3;

            for (int x = 0; x < numEvenRecords; x++) {
                testHarness.processElement(new StreamRecord<>(2 * x));
            }

            for (int x = 0; x < numOddRecords; x++) {
                testHarness.processElement(new StreamRecord<>(2 * x + 1));
            }

            final int oddEvenOperatorOutputsWithOddTag = numOddRecords;
            final int oddEvenOperatorOutputsWithoutTag = numOddRecords + numEvenRecords;
            final int duplicatingOperatorOutput = (numOddRecords + numEvenRecords) * 2;
            assertEquals(numOddRecords + numEvenRecords, numRecordsInCounter.getCount());
            assertEquals(
                    oddEvenOperatorOutputsWithOddTag
                            + oddEvenOperatorOutputsWithoutTag
                            + duplicatingOperatorOutput,
                    numRecordsOutCounter.getCount());
            testHarness.waitForTaskCompletion();
        } finally {
            for (ResultPartitionWriter partitionWriter : partitionWriters) {
                partitionWriter.close();
            }
        }
    }

    static class PassThroughOperator<T> extends AbstractStreamOperator<T>
            implements TwoInputStreamOperator<T, T, T> {

        @Override
        public void processElement1(StreamRecord<T> element) throws Exception {
            output.collect(element);
        }

        @Override
        public void processElement2(StreamRecord<T> element) throws Exception {
            output.collect(element);
        }
    }

    static class OddEvenOperator extends AbstractStreamOperator<Integer>
            implements TwoInputStreamOperator<Integer, Integer, Integer> {
        private final OutputTag<Integer> oddOutputTag =
                new OutputTag<>("odd", BasicTypeInfo.INT_TYPE_INFO);
        private final OutputTag<Integer> evenOutputTag =
                new OutputTag<>("even", BasicTypeInfo.INT_TYPE_INFO);

        @Override
        public void processElement1(StreamRecord<Integer> element) throws Exception {
            processElement(element);
        }

        @Override
        public void processElement2(StreamRecord<Integer> element) throws Exception {
            processElement(element);
        }

        private void processElement(StreamRecord<Integer> element) {
            if (element.getValue() % 2 == 0) {
                output.collect(evenOutputTag, element);
            } else {
                output.collect(oddOutputTag, element);
            }
            output.collect(element);
        }
    }

    // This must only be used in one test, otherwise the static fields will be changed
    // by several tests concurrently
    private static class TestOpenCloseMapFunction
            extends RichCoMapFunction<String, Integer, String> {
        private static final long serialVersionUID = 1L;

        public static boolean openCalled = false;
        public static boolean closeCalled = false;

        TestOpenCloseMapFunction() {
            openCalled = false;
            closeCalled = false;
        }

        @Override
        public void open(OpenContext openContext) throws Exception {
            super.open(openContext);
            if (closeCalled) {
                Assert.fail("Close called before open.");
            }
            openCalled = true;
        }

        @Override
        public void close() throws Exception {
            super.close();
            if (!openCalled) {
                Assert.fail("Open was not called before close.");
            }
            closeCalled = true;
        }

        @Override
        public String map1(String value) {
            if (!openCalled) {
                Assert.fail("Open was not called before run.");
            }
            return value;
        }

        @Override
        public String map2(Integer value) {
            if (!openCalled) {
                Assert.fail("Open was not called before run.");
            }
            return value.toString();
        }
    }

    private static class IdentityMap implements CoMapFunction<String, Integer, String> {
        private static final long serialVersionUID = 1L;

        @Override
        public String map1(String value) {
            return value;
        }

        @Override
        public String map2(Integer value) {

            return value.toString();
        }
    }
}
