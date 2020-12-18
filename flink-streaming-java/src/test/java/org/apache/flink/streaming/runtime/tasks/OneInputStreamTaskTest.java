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
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Metric;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.NoOpMetricRegistry;
import org.apache.flink.runtime.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.metrics.util.InterceptingOperatorMetricGroup;
import org.apache.flink.runtime.metrics.util.InterceptingTaskMetricGroup;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;

import org.hamcrest.collection.IsMapContaining;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link OneInputStreamTask}.
 *
 * <p>Note:<br>
 * We only use a {@link StreamMap} operator here. We also test the individual operators but Map is
 * used as a representative to test OneInputStreamTask, since OneInputStreamTask is used for all
 * OneInputStreamOperators.
 */
public class OneInputStreamTaskTest extends TestLogger {

    private static final ListStateDescriptor<Integer> TEST_DESCRIPTOR =
            new ListStateDescriptor<>("test", new IntSerializer());

    /**
     * This test verifies that open() and close() are correctly called. This test also verifies that
     * timestamps of emitted elements are correct. {@link StreamMap} assigns the input timestamp to
     * emitted elements.
     */
    @Test
    public void testOpenCloseAndTimestamps() throws Exception {
        final OneInputStreamTaskTestHarness<String, String> testHarness =
                new OneInputStreamTaskTestHarness<>(
                        OneInputStreamTask::new,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO);

        testHarness.setupOutputForSingletonOperatorChain();

        StreamConfig streamConfig = testHarness.getStreamConfig();
        StreamMap<String, String> mapOperator = new StreamMap<>(new TestOpenCloseMapFunction());
        streamConfig.setStreamOperator(mapOperator);
        streamConfig.setOperatorID(new OperatorID());

        long initialTime = 0L;
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        testHarness.invoke();
        testHarness.waitForTaskRunning();

        testHarness.processElement(new StreamRecord<>("Hello", initialTime + 1));
        testHarness.processElement(new StreamRecord<>("Ciao", initialTime + 2));
        expectedOutput.add(new StreamRecord<>("Hello", initialTime + 1));
        expectedOutput.add(new StreamRecord<>("Ciao", initialTime + 2));

        testHarness.waitForInputProcessing();

        testHarness.endInput();

        testHarness.waitForTaskCompletion();

        assertTrue("RichFunction methods where not called.", TestOpenCloseMapFunction.closeCalled);

        TestHarnessUtil.assertOutputEquals(
                "Output was not correct.", expectedOutput, testHarness.getOutput());
    }

    /**
     * This test verifies that watermarks and stream statuses are correctly forwarded. This also
     * checks whether watermarks are forwarded only when we have received watermarks from all
     * inputs. The forwarded watermark must be the minimum of the watermarks of all active inputs.
     */
    @Test
    public void testWatermarkAndStreamStatusForwarding() throws Exception {

        final OneInputStreamTaskTestHarness<String, String> testHarness =
                new OneInputStreamTaskTestHarness<>(
                        OneInputStreamTask::new,
                        2,
                        2,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO);
        testHarness.setupOutputForSingletonOperatorChain();

        StreamConfig streamConfig = testHarness.getStreamConfig();
        StreamMap<String, String> mapOperator = new StreamMap<>(new IdentityMap());
        streamConfig.setStreamOperator(mapOperator);
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
        testHarness.processElement(new StreamRecord<>("Hello", initialTime));
        testHarness.processElement(new StreamRecord<>("Ciao", initialTime));
        expectedOutput.add(new StreamRecord<>("Hello", initialTime));
        expectedOutput.add(new StreamRecord<>("Ciao", initialTime));

        testHarness.processElement(new Watermark(initialTime + 4), 0, 0);
        testHarness.processElement(new Watermark(initialTime + 3), 0, 1);
        testHarness.processElement(new Watermark(initialTime + 3), 1, 0);
        testHarness.processElement(new Watermark(initialTime + 2), 1, 1);

        // check whether we get the minimum of all the watermarks, this must also only occur in
        // the output after the two StreamRecords
        testHarness.waitForInputProcessing();
        expectedOutput.add(new Watermark(initialTime + 2));
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
        testHarness.processElement(StreamStatus.IDLE, 0, 1);
        testHarness.processElement(StreamStatus.IDLE, 1, 0);
        testHarness.processElement(new Watermark(initialTime + 6), 0, 0);
        testHarness.processElement(
                new Watermark(initialTime + 5), 1, 1); // this watermark should be advanced first
        testHarness.processElement(StreamStatus.IDLE, 1, 1); // once this is acknowledged,
        // watermark (initial + 6) should be forwarded
        testHarness.waitForInputProcessing();
        expectedOutput.add(new Watermark(initialTime + 5));
        expectedOutput.add(new Watermark(initialTime + 6));
        TestHarnessUtil.assertOutputEquals(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // make all input channels idle and check that the operator's idle status is forwarded
        testHarness.processElement(StreamStatus.IDLE, 0, 0);
        testHarness.waitForInputProcessing();
        expectedOutput.add(StreamStatus.IDLE);
        TestHarnessUtil.assertOutputEquals(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // make some input channels active again and check that the operator's active status is
        // forwarded only once
        testHarness.processElement(StreamStatus.ACTIVE, 1, 0);
        testHarness.processElement(StreamStatus.ACTIVE, 0, 1);
        testHarness.waitForInputProcessing();
        expectedOutput.add(StreamStatus.ACTIVE);
        TestHarnessUtil.assertOutputEquals(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.endInput();

        testHarness.waitForTaskCompletion();

        List<String> resultElements =
                TestHarnessUtil.getRawElementsFromOutput(testHarness.getOutput());
        assertEquals(2, resultElements.size());
    }

    /**
     * This test verifies that watermarks are not forwarded when the task is idle. It also verifies
     * that when task is idle, watermarks generated in the middle of chains are also blocked and
     * never forwarded.
     *
     * <p>The tested chain will be: (HEAD: normal operator) --> (watermark generating operator) -->
     * (normal operator). The operators will throw an exception and fail the test if either of them
     * were forwarded watermarks when the task is idle.
     */
    @Test
    public void testWatermarksNotForwardedWithinChainWhenIdle() throws Exception {

        final OneInputStreamTaskTestHarness<String, String> testHarness =
                new OneInputStreamTaskTestHarness<>(
                        OneInputStreamTask::new,
                        1,
                        1,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO);

        TriggerableFailOnWatermarkTestOperator headOperator =
                new TriggerableFailOnWatermarkTestOperator();
        WatermarkGeneratingTestOperator watermarkOperator = new WatermarkGeneratingTestOperator();
        TriggerableFailOnWatermarkTestOperator tailOperator =
                new TriggerableFailOnWatermarkTestOperator();

        testHarness
                .setupOperatorChain(new OperatorID(42L, 42L), headOperator)
                .chain(new OperatorID(4711L, 42L), watermarkOperator, StringSerializer.INSTANCE)
                .chain(new OperatorID(123L, 123L), tailOperator, StringSerializer.INSTANCE)
                .finish();

        // --------------------- begin test ---------------------

        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        testHarness.invoke();
        testHarness.waitForTaskRunning();

        // the task starts as active, so all generated watermarks should be forwarded
        testHarness.processElement(
                new StreamRecord<>(
                        TriggerableFailOnWatermarkTestOperator.EXPECT_FORWARDED_WATERMARKS_MARKER));

        testHarness.processElement(new StreamRecord<>("10"), 0, 0);

        // this watermark will be forwarded since the task is currently active,
        // but should not be in the final output because it should be blocked by the watermark
        // generator in the chain
        testHarness.processElement(new Watermark(15));

        testHarness.processElement(new StreamRecord<>("20"), 0, 0);
        testHarness.processElement(new StreamRecord<>("30"), 0, 0);

        testHarness.waitForInputProcessing();

        expectedOutput.add(
                new StreamRecord<>(
                        TriggerableFailOnWatermarkTestOperator.EXPECT_FORWARDED_WATERMARKS_MARKER));
        expectedOutput.add(new StreamRecord<>("10"));
        expectedOutput.add(new Watermark(10));
        expectedOutput.add(new StreamRecord<>("20"));
        expectedOutput.add(new Watermark(20));
        expectedOutput.add(new StreamRecord<>("30"));
        expectedOutput.add(new Watermark(30));
        TestHarnessUtil.assertOutputEquals(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // now, toggle the task to be idle, and let the watermark generator produce some watermarks
        testHarness.processElement(StreamStatus.IDLE);

        // after this, the operators will throw an exception if they are forwarded watermarks
        // anywhere in the chain
        testHarness.processElement(
                new StreamRecord<>(
                        TriggerableFailOnWatermarkTestOperator.NO_FORWARDED_WATERMARKS_MARKER));

        // NOTE: normally, tasks will not have records to process while idle;
        // we're doing this here only to mimic watermark generating in operators
        testHarness.processElement(new StreamRecord<>("40"), 0, 0);
        testHarness.processElement(new StreamRecord<>("50"), 0, 0);
        testHarness.processElement(new StreamRecord<>("60"), 0, 0);
        testHarness.processElement(
                new Watermark(
                        65)); // the test will fail if any of the operators were forwarded this
        testHarness.waitForInputProcessing();

        // the 40 - 60 watermarks should not be forwarded, only the stream status toggle element and
        // records
        expectedOutput.add(StreamStatus.IDLE);
        expectedOutput.add(
                new StreamRecord<>(
                        TriggerableFailOnWatermarkTestOperator.NO_FORWARDED_WATERMARKS_MARKER));
        expectedOutput.add(new StreamRecord<>("40"));
        expectedOutput.add(new StreamRecord<>("50"));
        expectedOutput.add(new StreamRecord<>("60"));
        TestHarnessUtil.assertOutputEquals(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // re-toggle the task to be active and see if new watermarks are correctly forwarded again
        testHarness.processElement(StreamStatus.ACTIVE);
        testHarness.processElement(
                new StreamRecord<>(
                        TriggerableFailOnWatermarkTestOperator.EXPECT_FORWARDED_WATERMARKS_MARKER));

        testHarness.processElement(new StreamRecord<>("70"), 0, 0);
        testHarness.processElement(new StreamRecord<>("80"), 0, 0);
        testHarness.processElement(new StreamRecord<>("90"), 0, 0);
        testHarness.waitForInputProcessing();

        expectedOutput.add(StreamStatus.ACTIVE);
        expectedOutput.add(
                new StreamRecord<>(
                        TriggerableFailOnWatermarkTestOperator.EXPECT_FORWARDED_WATERMARKS_MARKER));
        expectedOutput.add(new StreamRecord<>("70"));
        expectedOutput.add(new Watermark(70));
        expectedOutput.add(new StreamRecord<>("80"));
        expectedOutput.add(new Watermark(80));
        expectedOutput.add(new StreamRecord<>("90"));
        expectedOutput.add(new Watermark(90));
        TestHarnessUtil.assertOutputEquals(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.endInput();

        testHarness.waitForTaskCompletion();

        List<String> resultElements =
                TestHarnessUtil.getRawElementsFromOutput(testHarness.getOutput());
        assertEquals(12, resultElements.size());
    }

    /** This test verifies that checkpoint barriers are correctly forwarded. */
    @Test
    public void testCheckpointBarriers() throws Exception {
        final OneInputStreamTaskTestHarness<String, String> testHarness =
                new OneInputStreamTaskTestHarness<>(
                        OneInputStreamTask::new,
                        2,
                        2,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO);

        testHarness.setupOutputForSingletonOperatorChain();

        StreamConfig streamConfig = testHarness.getStreamConfig();
        StreamMap<String, String> mapOperator = new StreamMap<>(new IdentityMap());
        streamConfig.setStreamOperator(mapOperator);
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
        testHarness.processElement(new StreamRecord<>("Hello-1-1", initialTime), 1, 1);
        testHarness.processElement(new StreamRecord<>("Ciao-1-1", initialTime), 1, 1);
        expectedOutput.add(new StreamRecord<>("Hello-1-1", initialTime));
        expectedOutput.add(new StreamRecord<>("Ciao-1-1", initialTime));

        testHarness.waitForInputProcessing();
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

        // now we should see the barrier
        expectedOutput.add(
                new CheckpointBarrier(0, 0, CheckpointOptions.forCheckpointWithDefaultLocation()));

        testHarness.endInput();

        testHarness.waitForTaskCompletion();

        TestHarnessUtil.assertOutputEquals(
                "Output was not correct.", expectedOutput, testHarness.getOutput());
    }

    /**
     * This test verifies that checkpoint barriers and barrier buffers work correctly with
     * concurrent checkpoint barriers where one checkpoint is "overtaking" another checkpoint, i.e.
     * some inputs receive barriers from an earlier checkpoint, thereby blocking, then all inputs
     * receive barriers from a later checkpoint.
     */
    @Test
    public void testOvertakingCheckpointBarriers() throws Exception {
        final OneInputStreamTaskTestHarness<String, String> testHarness =
                new OneInputStreamTaskTestHarness<>(
                        OneInputStreamTask::new,
                        2,
                        2,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO);

        testHarness.setupOutputForSingletonOperatorChain();

        StreamConfig streamConfig = testHarness.getStreamConfig();
        StreamMap<String, String> mapOperator = new StreamMap<>(new IdentityMap());
        streamConfig.setStreamOperator(mapOperator);
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
        testHarness.processElement(new StreamRecord<>("Hello-1-1", initialTime), 1, 1);
        testHarness.processElement(new StreamRecord<>("Ciao-1-1", initialTime), 1, 1);
        expectedOutput.add(new StreamRecord<>("Hello-1-1", initialTime));
        expectedOutput.add(new StreamRecord<>("Ciao-1-1", initialTime));

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

    /**
     * Tests that the stream operator can snapshot and restore the operator state of chained
     * operators.
     */
    @Test
    public void testSnapshottingAndRestoring() throws Exception {
        final Deadline deadline = new FiniteDuration(2, TimeUnit.MINUTES).fromNow();

        final OneInputStreamTaskTestHarness<String, String> testHarness =
                new OneInputStreamTaskTestHarness<>(
                        OneInputStreamTask::new,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO);

        testHarness.setupOutputForSingletonOperatorChain();

        IdentityKeySelector<String> keySelector = new IdentityKeySelector<>();
        testHarness.configureForKeyedStream(keySelector, BasicTypeInfo.STRING_TYPE_INFO);

        long checkpointId = 1L;
        long checkpointTimestamp = 1L;
        int numberChainedTasks = 11;

        StreamConfig streamConfig = testHarness.getStreamConfig();

        configureChainedTestingStreamOperator(streamConfig, numberChainedTasks);
        TestTaskStateManager taskStateManager = testHarness.taskStateManager;
        OneShotLatch waitForAcknowledgeLatch = new OneShotLatch();

        taskStateManager.setWaitForReportLatch(waitForAcknowledgeLatch);

        // reset number of restore calls
        TestingStreamOperator.numberRestoreCalls = 0;

        testHarness.invoke();
        testHarness.waitForTaskRunning();

        final OneInputStreamTask<String, String> streamTask = testHarness.getTask();

        CheckpointMetaData checkpointMetaData =
                new CheckpointMetaData(checkpointId, checkpointTimestamp);

        streamTask
                .triggerCheckpointAsync(
                        checkpointMetaData,
                        CheckpointOptions.forCheckpointWithDefaultLocation(),
                        false)
                .get();

        // since no state was set, there shouldn't be restore calls
        assertEquals(0, TestingStreamOperator.numberRestoreCalls);

        waitForAcknowledgeLatch.await();

        assertEquals(checkpointId, taskStateManager.getReportedCheckpointId());

        testHarness.endInput();
        testHarness.waitForTaskCompletion(deadline.timeLeft().toMillis());

        final OneInputStreamTaskTestHarness<String, String> restoredTaskHarness =
                new OneInputStreamTaskTestHarness<>(
                        OneInputStreamTask::new,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO);

        restoredTaskHarness.configureForKeyedStream(keySelector, BasicTypeInfo.STRING_TYPE_INFO);

        restoredTaskHarness.setTaskStateSnapshot(
                checkpointId, taskStateManager.getLastJobManagerTaskStateSnapshot());

        StreamConfig restoredTaskStreamConfig = restoredTaskHarness.getStreamConfig();

        configureChainedTestingStreamOperator(restoredTaskStreamConfig, numberChainedTasks);

        TaskStateSnapshot stateHandles = taskStateManager.getLastJobManagerTaskStateSnapshot();
        Assert.assertEquals(numberChainedTasks, stateHandles.getSubtaskStateMappings().size());

        TestingStreamOperator.numberRestoreCalls = 0;

        // transfer state to new harness
        restoredTaskHarness.taskStateManager.restoreLatestCheckpointState(
                taskStateManager.getJobManagerTaskStateSnapshotsByCheckpointId());
        restoredTaskHarness.invoke();
        restoredTaskHarness.endInput();
        restoredTaskHarness.waitForTaskCompletion(deadline.timeLeft().toMillis());

        // restore of every chained operator should have been called
        assertEquals(numberChainedTasks, TestingStreamOperator.numberRestoreCalls);

        TestingStreamOperator.numberRestoreCalls = 0;
        TestingStreamOperator.numberSnapshotCalls = 0;
    }

    @Test
    public void testQuiesceTimerServiceAfterOpClose() throws Exception {

        final OneInputStreamTaskTestHarness<String, String> testHarness =
                new OneInputStreamTaskTestHarness<>(
                        OneInputStreamTask::new,
                        2,
                        2,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO);
        testHarness.setupOutputForSingletonOperatorChain();

        StreamConfig streamConfig = testHarness.getStreamConfig();
        streamConfig.setStreamOperator(new TestOperator());
        streamConfig.setOperatorID(new OperatorID());

        testHarness.invoke();
        testHarness.waitForTaskRunning();

        SystemProcessingTimeService timeService =
                (SystemProcessingTimeService) testHarness.getTimerService();

        // verify that the timer service is running
        Assert.assertTrue(timeService.isAlive());

        testHarness.endInput();
        testHarness.waitForTaskCompletion();
        timeService.shutdownService();
    }

    @Test
    public void testClosingAllOperatorsOnChainProperly() throws Exception {
        final OneInputStreamTaskTestHarness<String, String> testHarness =
                new OneInputStreamTaskTestHarness<>(
                        OneInputStreamTask::new,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO);

        testHarness
                .setupOperatorChain(
                        new OperatorID(), new TestBoundedOneInputStreamOperator("Operator0"))
                .chain(
                        new OperatorID(),
                        new TestBoundedOneInputStreamOperator("Operator1"),
                        BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()))
                .finish();

        testHarness.invoke();
        testHarness.waitForTaskRunning();

        testHarness.processElement(new StreamRecord<>("Hello"));
        testHarness.endInput();

        testHarness.waitForTaskCompletion();

        ArrayList<StreamRecord<String>> expected = new ArrayList<>();
        Collections.addAll(
                expected,
                new StreamRecord<>("Hello"),
                new StreamRecord<>("[Operator0]: End of input"),
                new StreamRecord<>("[Operator0]: Bye"),
                new StreamRecord<>("[Operator1]: End of input"),
                new StreamRecord<>("[Operator1]: Bye"));

        final Object[] output = testHarness.getOutput().toArray();
        assertArrayEquals("Output was not correct.", expected.toArray(), output);
    }

    private static class TestOperator extends AbstractStreamOperator<String>
            implements OneInputStreamOperator<String, String> {

        private static final long serialVersionUID = 1L;

        @Override
        public void processElement(StreamRecord<String> element) throws Exception {
            output.collect(element);
        }

        @Override
        public void close() throws Exception {

            // verify that the timer service is still running
            Assert.assertTrue(
                    ((SystemProcessingTimeService) getContainingTask().getTimerService())
                            .isAlive());
            super.close();
        }
    }

    @Test
    public void testOperatorMetricReuse() throws Exception {
        final OneInputStreamTaskTestHarness<String, String> testHarness =
                new OneInputStreamTaskTestHarness<>(
                        OneInputStreamTask::new,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO);

        testHarness
                .setupOperatorChain(new OperatorID(), new DuplicatingOperator())
                .chain(
                        new OperatorID(),
                        new DuplicatingOperator(),
                        BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()))
                .chain(
                        new OperatorID(),
                        new DuplicatingOperator(),
                        BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()))
                .finish();

        final TaskMetricGroup taskMetricGroup =
                new UnregisteredMetricGroups.UnregisteredTaskMetricGroup() {
                    @Override
                    public OperatorMetricGroup getOrAddOperator(
                            OperatorID operatorID, String name) {
                        return new OperatorMetricGroup(
                                NoOpMetricRegistry.INSTANCE, this, operatorID, name);
                    }
                };

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

        final int numRecords = 5;

        for (int x = 0; x < numRecords; x++) {
            testHarness.processElement(new StreamRecord<>("hello"));
        }
        testHarness.waitForInputProcessing();

        assertEquals(numRecords, numRecordsInCounter.getCount());
        assertEquals(numRecords * 2 * 2 * 2, numRecordsOutCounter.getCount());

        testHarness.endInput();
        testHarness.waitForTaskCompletion();
    }

    static class DuplicatingOperator extends AbstractStreamOperator<String>
            implements OneInputStreamOperator<String, String> {
        @Override
        public void processElement(StreamRecord<String> element) {
            output.collect(element);
            output.collect(element);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testWatermarkMetrics() throws Exception {
        final OneInputStreamTaskTestHarness<String, String> testHarness =
                new OneInputStreamTaskTestHarness<>(
                        OneInputStreamTask::new,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO);

        OneInputStreamOperator<String, String> headOperator = new WatermarkMetricOperator();
        OperatorID headOperatorId = new OperatorID();

        OneInputStreamOperator<String, String> chainedOperator = new WatermarkMetricOperator();
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
                    public OperatorMetricGroup getOrAddOperator(OperatorID id, String name) {
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
                5,
                new HashSet<>(
                                Arrays.asList(
                                        taskInputWatermarkGauge,
                                        headInputWatermarkGauge,
                                        headOutputWatermarkGauge,
                                        chainedInputWatermarkGauge,
                                        chainedOutputWatermarkGauge))
                        .size());

        Assert.assertEquals(Long.MIN_VALUE, taskInputWatermarkGauge.getValue().longValue());
        Assert.assertEquals(Long.MIN_VALUE, headInputWatermarkGauge.getValue().longValue());
        Assert.assertEquals(Long.MIN_VALUE, headOutputWatermarkGauge.getValue().longValue());
        Assert.assertEquals(Long.MIN_VALUE, chainedInputWatermarkGauge.getValue().longValue());
        Assert.assertEquals(Long.MIN_VALUE, chainedOutputWatermarkGauge.getValue().longValue());

        testHarness.processElement(new Watermark(1L));
        testHarness.waitForInputProcessing();
        Assert.assertEquals(1L, taskInputWatermarkGauge.getValue().longValue());
        Assert.assertEquals(1L, headInputWatermarkGauge.getValue().longValue());
        Assert.assertEquals(2L, headOutputWatermarkGauge.getValue().longValue());
        Assert.assertEquals(2L, chainedInputWatermarkGauge.getValue().longValue());
        Assert.assertEquals(4L, chainedOutputWatermarkGauge.getValue().longValue());

        testHarness.processElement(new Watermark(2L));
        testHarness.waitForInputProcessing();
        Assert.assertEquals(2L, taskInputWatermarkGauge.getValue().longValue());
        Assert.assertEquals(2L, headInputWatermarkGauge.getValue().longValue());
        Assert.assertEquals(4L, headOutputWatermarkGauge.getValue().longValue());
        Assert.assertEquals(4L, chainedInputWatermarkGauge.getValue().longValue());
        Assert.assertEquals(8L, chainedOutputWatermarkGauge.getValue().longValue());

        testHarness.endInput();
        testHarness.waitForTaskCompletion();
    }

    /**
     * Tests the checkpoint related metrics are registered into {@link TaskIOMetricGroup} correctly
     * while generating the {@link OneInputStreamTask}.
     */
    @Test
    public void testCheckpointBarrierMetrics() throws Exception {
        final OneInputStreamTaskTestHarness<String, String> testHarness =
                new OneInputStreamTaskTestHarness<>(
                        OneInputStreamTask::new,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO);

        testHarness.setupOutputForSingletonOperatorChain();
        StreamConfig streamConfig = testHarness.getStreamConfig();
        streamConfig.setStreamOperator(new TestOperator());

        final Map<String, Metric> metrics = new ConcurrentHashMap<>();
        final TaskMetricGroup taskMetricGroup =
                new StreamTaskTestHarness.TestTaskMetricGroup(metrics);
        final StreamMockEnvironment environment = testHarness.createEnvironment();
        environment.setTaskMetricGroup(taskMetricGroup);

        testHarness.invoke(environment);
        testHarness.waitForTaskRunning();

        assertThat(metrics, IsMapContaining.hasKey(MetricNames.CHECKPOINT_ALIGNMENT_TIME));
        assertThat(metrics, IsMapContaining.hasKey(MetricNames.CHECKPOINT_START_DELAY_TIME));

        testHarness.endInput();
        testHarness.waitForTaskCompletion();
    }

    static class WatermarkMetricOperator extends AbstractStreamOperator<String>
            implements OneInputStreamOperator<String, String> {

        @Override
        public void processElement(StreamRecord<String> element) throws Exception {
            output.collect(element);
        }

        @Override
        public void processWatermark(Watermark mark) {
            output.emitWatermark(new Watermark(mark.getTimestamp() * 2));
        }
    }

    // ==============================================================================================
    // Utility functions and classes
    // ==============================================================================================

    private void configureChainedTestingStreamOperator(
            StreamConfig streamConfig, int numberChainedTasks) {

        Preconditions.checkArgument(
                numberChainedTasks >= 1,
                "The operator chain must at least " + "contain one operator.");

        TestingStreamOperator<Integer, Integer> previousOperator = new TestingStreamOperator<>();
        streamConfig.setStreamOperator(previousOperator);
        streamConfig.setOperatorID(new OperatorID(0L, 0L));

        // create the chain of operators
        Map<Integer, StreamConfig> chainedTaskConfigs = new HashMap<>(numberChainedTasks - 1);
        List<StreamEdge> outputEdges = new ArrayList<>(numberChainedTasks - 1);

        for (int chainedIndex = 1; chainedIndex < numberChainedTasks; chainedIndex++) {
            TestingStreamOperator<Integer, Integer> chainedOperator = new TestingStreamOperator<>();
            StreamConfig chainedConfig = new StreamConfig(new Configuration());
            chainedConfig.setupNetworkInputs(StringSerializer.INSTANCE);
            chainedConfig.setStreamOperator(chainedOperator);
            chainedConfig.setOperatorID(new OperatorID(0L, chainedIndex));
            chainedTaskConfigs.put(chainedIndex, chainedConfig);

            StreamEdge outputEdge =
                    new StreamEdge(
                            new StreamNode(
                                    chainedIndex - 1,
                                    null,
                                    null,
                                    (StreamOperator<?>) null,
                                    null,
                                    null),
                            new StreamNode(
                                    chainedIndex, null, null, (StreamOperator<?>) null, null, null),
                            0,
                            null,
                            null);

            outputEdges.add(outputEdge);
        }

        streamConfig.setChainedOutputs(outputEdges);
        streamConfig.setTransitiveChainedTaskConfigs(chainedTaskConfigs);
    }

    private static class IdentityKeySelector<IN> implements KeySelector<IN, IN> {

        private static final long serialVersionUID = -3555913664416688425L;

        @Override
        public IN getKey(IN value) throws Exception {
            return value;
        }
    }

    private static class TestingStreamOperator<IN, OUT> extends AbstractStreamOperator<OUT>
            implements OneInputStreamOperator<IN, OUT> {

        private static final long serialVersionUID = 774614855940397174L;

        public static int numberRestoreCalls = 0;
        public static int numberSnapshotCalls = 0;

        @Override
        public void snapshotState(StateSnapshotContext context) throws Exception {
            ListState<Integer> partitionableState =
                    getOperatorStateBackend().getListState(TEST_DESCRIPTOR);
            partitionableState.clear();

            partitionableState.add(42);
            partitionableState.add(4711);

            ++numberSnapshotCalls;
        }

        @Override
        public void initializeState(StateInitializationContext context) throws Exception {
            if (context.isRestored()) {
                ++numberRestoreCalls;
            }

            ListState<Integer> partitionableState =
                    context.getOperatorStateStore().getListState(TEST_DESCRIPTOR);

            if (numberSnapshotCalls == 0) {
                for (Integer v : partitionableState.get()) {
                    fail();
                }
            } else {
                Set<Integer> result = new HashSet<>();
                for (Integer v : partitionableState.get()) {
                    result.add(v);
                }

                assertEquals(2, result.size());
                assertTrue(result.contains(42));
                assertTrue(result.contains(4711));
            }
        }

        @Override
        public void processElement(StreamRecord<IN> element) throws Exception {}
    }

    /**
     * This must only be used in one test, otherwise the static fields will be changed by several
     * tests concurrently.
     */
    private static class TestOpenCloseMapFunction extends RichMapFunction<String, String> {
        private static final long serialVersionUID = 1L;

        public static boolean openCalled = false;
        public static boolean closeCalled = false;

        TestOpenCloseMapFunction() {
            openCalled = false;
            closeCalled = false;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
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
        public String map(String value) throws Exception {
            if (!openCalled) {
                Assert.fail("Open was not called before run.");
            }
            return value;
        }
    }

    private static class IdentityMap implements MapFunction<String, String> {
        private static final long serialVersionUID = 1L;

        @Override
        public String map(String value) throws Exception {
            return value;
        }
    }

    /** A {@link TriggerableFailOnWatermarkTestOperator} that generates watermarks. */
    private static class WatermarkGeneratingTestOperator
            extends TriggerableFailOnWatermarkTestOperator {

        private static final long serialVersionUID = -5064871833244157221L;

        private long lastWatermark;

        @Override
        protected void handleElement(StreamRecord<String> element) {
            long timestamp = Long.valueOf(element.getValue());
            if (timestamp > lastWatermark) {
                output.emitWatermark(new Watermark(timestamp));
                lastWatermark = timestamp;
            }
        }

        @Override
        protected void handleWatermark(Watermark mark) {
            if (mark.equals(Watermark.MAX_WATERMARK)) {
                output.emitWatermark(mark);
                lastWatermark = Long.MAX_VALUE;
            }
        }
    }

    /**
     * An operator that can be triggered whether or not to expect watermarks forwarded to it,
     * toggled by letting it process special trigger marker records.
     *
     * <p>If it receives a watermark when it's not expecting one, it'll throw an exception and fail.
     */
    private static class TriggerableFailOnWatermarkTestOperator
            extends AbstractStreamOperator<String>
            implements OneInputStreamOperator<String, String> {

        private static final long serialVersionUID = 2048954179291813243L;

        public static final String EXPECT_FORWARDED_WATERMARKS_MARKER = "EXPECT_WATERMARKS";
        public static final String NO_FORWARDED_WATERMARKS_MARKER = "NO_WATERMARKS";

        protected boolean expectForwardedWatermarks;

        @Override
        public void processElement(StreamRecord<String> element) throws Exception {
            output.collect(element);

            if (element.getValue().equals(EXPECT_FORWARDED_WATERMARKS_MARKER)) {
                this.expectForwardedWatermarks = true;
            } else if (element.getValue().equals(NO_FORWARDED_WATERMARKS_MARKER)) {
                this.expectForwardedWatermarks = false;
            } else {
                handleElement(element);
            }
        }

        @Override
        public void processWatermark(Watermark mark) throws Exception {
            if (!expectForwardedWatermarks) {
                throw new Exception(
                        "Received a "
                                + mark
                                + ", but this operator should not be forwarded watermarks.");
            } else {
                handleWatermark(mark);
            }
        }

        protected void handleElement(StreamRecord<String> element) {
            // do nothing
        }

        protected void handleWatermark(Watermark mark) {
            output.emitWatermark(mark);
        }
    }
}
