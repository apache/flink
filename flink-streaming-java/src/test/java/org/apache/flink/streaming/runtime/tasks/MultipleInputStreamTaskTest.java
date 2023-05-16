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
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.mocks.MockSource;
import org.apache.flink.api.connector.source.mocks.MockSourceReader;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.api.connector.source.mocks.MockSourceSplitSerializer;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.AvailabilityProvider;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.EndOfData;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.runtime.io.network.api.writer.RecordOrEventCollectingResultPartitionWriter;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriterWithAvailabilityHelper;
import org.apache.flink.runtime.io.network.partition.PartitionTestUtils;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.InternalOperatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.metrics.util.InterceptingOperatorMetricGroup;
import org.apache.flink.runtime.metrics.util.InterceptingTaskMetricGroup;
import org.apache.flink.runtime.source.event.AddSplitEvent;
import org.apache.flink.runtime.source.event.NoMoreSplitsEvent;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.taskmanager.CheckpointResponder;
import org.apache.flink.runtime.taskmanager.TestCheckpointResponder;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractInput;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.SourceOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.StreamMultipleInputProcessor;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.MultipleInputStreamTaskChainedSourcesCheckpointingTest.LifeCycleMonitorMultipleInputOperatorFactory;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTaskTest.WatermarkMetricOperator;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.streaming.util.CompletingCheckpointResponder;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.testutils.junit.SharedObjects;
import org.apache.flink.testutils.junit.SharedReference;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.SerializedValue;

import org.hamcrest.collection.IsMapContaining;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import static org.apache.flink.streaming.runtime.tasks.StreamTaskFinalCheckpointsTest.processMailTillCheckpointSucceeds;
import static org.apache.flink.streaming.runtime.tasks.StreamTaskFinalCheckpointsTest.triggerCheckpoint;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link MultipleInputStreamTask}. Theses tests implicitly also test the {@link
 * StreamMultipleInputProcessor}.
 */
@SuppressWarnings("serial")
@RunWith(Parameterized.class)
public class MultipleInputStreamTaskTest {

    private static final List<String> LIFE_CYCLE_EVENTS = new ArrayList<>();

    @Parameters(name = "objectReuse = {0}")
    public static Boolean[] parameters() {
        return new Boolean[] {true, false};
    }

    @Parameter public boolean objectReuse;

    @Rule public final SharedObjects sharedObjects = SharedObjects.create();

    @Before
    public void setUp() {
        LIFE_CYCLE_EVENTS.clear();
    }

    @Test
    public void testBasicProcessing() throws Exception {
        try (StreamTaskMailboxTestHarness<String> testHarness = buildTestHarness(objectReuse)) {
            long initialTime = 0L;
            ArrayDeque<Object> expectedOutput = new ArrayDeque<>();

            addSourceRecords(testHarness, 1, 42, 43);
            expectedOutput.add(new StreamRecord<>("42", TimestampAssigner.NO_TIMESTAMP));
            expectedOutput.add(new StreamRecord<>("43", TimestampAssigner.NO_TIMESTAMP));
            testHarness.processElement(new StreamRecord<>("Hello", initialTime + 1), 0);
            expectedOutput.add(new StreamRecord<>("Hello", initialTime + 1));
            testHarness.processElement(new StreamRecord<>(42.44d, initialTime + 3), 1);
            expectedOutput.add(new StreamRecord<>("42.44", initialTime + 3));

            testHarness.endInput();
            testHarness.waitForTaskCompletion();

            assertThat(testHarness.getOutput(), containsInAnyOrder(expectedOutput.toArray()));
        }
    }

    @Test
    public void testCopyForObjectReuse() throws Exception {
        SharedReference<List<Integer>> copiedElementsRef = sharedObjects.add(new ArrayList<>());
        CopyProxySerializer proxySerializer = new CopyProxySerializer(copiedElementsRef);

        try (StreamTaskMailboxTestHarness<String> testHarness =
                new StreamTaskMailboxTestHarnessBuilder<>(
                                MultipleInputStreamTask::new, BasicTypeInfo.STRING_TYPE_INFO)
                        .modifyExecutionConfig(applyObjectReuse(objectReuse))
                        .addInput(BasicTypeInfo.STRING_TYPE_INFO)
                        .addSourceInput(
                                new SourceOperatorFactory<>(
                                        new MockSource(Boundedness.BOUNDED, 1),
                                        WatermarkStrategy.noWatermarks()),
                                proxySerializer)
                        .addInput(BasicTypeInfo.DOUBLE_TYPE_INFO)
                        .setupOutputForSingletonOperatorChain(
                                new MapToStringMultipleInputOperatorFactory(3))
                        .build()) {

            addSourceRecords(testHarness, 1, 42, 43);

            testHarness.endInput();
            testHarness.waitForTaskCompletion();

            if (objectReuse) {
                assertTrue(copiedElementsRef.get().isEmpty());
            } else {
                assertThat(copiedElementsRef.get(), containsInAnyOrder(42, 43));
            }
        }
    }

    /** This test verifies that checkpoint barriers are correctly forwarded. */
    @Test
    public void testCheckpointBarriers() throws Exception {
        try (StreamTaskMailboxTestHarness<String> testHarness =
                new StreamTaskMailboxTestHarnessBuilder<>(
                                MultipleInputStreamTask::new, BasicTypeInfo.STRING_TYPE_INFO)
                        .addInput(BasicTypeInfo.STRING_TYPE_INFO, 2)
                        .addInput(BasicTypeInfo.INT_TYPE_INFO, 2)
                        .addInput(BasicTypeInfo.DOUBLE_TYPE_INFO, 2)
                        .setupOutputForSingletonOperatorChain(
                                new MapToStringMultipleInputOperatorFactory(3))
                        .build()) {
            ArrayDeque<Object> expectedOutput = new ArrayDeque<>();
            long initialTime = 0L;

            testHarness.processEvent(
                    new CheckpointBarrier(
                            0, 0, CheckpointOptions.forCheckpointWithDefaultLocation()),
                    0,
                    0);

            // This one should go through
            testHarness.processElement(new StreamRecord<>("Ciao-0-0", initialTime), 0, 1);
            expectedOutput.add(new StreamRecord<>("Ciao-0-0", initialTime));

            // These elements should be forwarded, since we did not yet receive a checkpoint barrier
            // on that input, only add to same input, otherwise we would not know the ordering
            // of the output since the Task might read the inputs in any order
            testHarness.processElement(new StreamRecord<>(11, initialTime), 1, 1);
            testHarness.processElement(new StreamRecord<>(1.0d, initialTime), 2, 0);
            expectedOutput.add(new StreamRecord<>("11", initialTime));
            expectedOutput.add(new StreamRecord<>("1.0", initialTime));

            assertThat(testHarness.getOutput(), contains(expectedOutput.toArray()));

            testHarness.processEvent(
                    new CheckpointBarrier(
                            0, 0, CheckpointOptions.forCheckpointWithDefaultLocation()),
                    0,
                    1);
            testHarness.processEvent(
                    new CheckpointBarrier(
                            0, 0, CheckpointOptions.forCheckpointWithDefaultLocation()),
                    1,
                    0);
            testHarness.processEvent(
                    new CheckpointBarrier(
                            0, 0, CheckpointOptions.forCheckpointWithDefaultLocation()),
                    1,
                    1);
            testHarness.processEvent(
                    new CheckpointBarrier(
                            0, 0, CheckpointOptions.forCheckpointWithDefaultLocation()),
                    2,
                    0);
            testHarness.processEvent(
                    new CheckpointBarrier(
                            0, 0, CheckpointOptions.forCheckpointWithDefaultLocation()),
                    2,
                    1);

            // now we should see the barrier
            expectedOutput.add(
                    new CheckpointBarrier(
                            0, 0, CheckpointOptions.forCheckpointWithDefaultLocation()));

            assertThat(testHarness.getOutput(), contains(expectedOutput.toArray()));
        }
    }

    /**
     * This test verifies that checkpoint barriers and barrier buffers work correctly with
     * concurrent checkpoint barriers where one checkpoint is "overtaking" another checkpoint, i.e.
     * some inputs receive barriers from an earlier checkpoint, thereby blocking, then all inputs
     * receive barriers from a later checkpoint.
     */
    @Test
    public void testOvertakingCheckpointBarriers() throws Exception {
        try (StreamTaskMailboxTestHarness<String> testHarness =
                new StreamTaskMailboxTestHarnessBuilder<>(
                                MultipleInputStreamTask::new, BasicTypeInfo.STRING_TYPE_INFO)
                        .addInput(BasicTypeInfo.STRING_TYPE_INFO, 2)
                        .addInput(BasicTypeInfo.INT_TYPE_INFO, 2)
                        .addInput(BasicTypeInfo.DOUBLE_TYPE_INFO, 2)
                        .setupOutputForSingletonOperatorChain(
                                new MapToStringMultipleInputOperatorFactory(3))
                        .build()) {
            ArrayDeque<Object> expectedOutput = new ArrayDeque<>();
            long initialTime = 0L;

            testHarness.processEvent(
                    new CheckpointBarrier(
                            0, 0, CheckpointOptions.forCheckpointWithDefaultLocation()),
                    0,
                    0);

            // These elements should be forwarded, since we did not yet receive a checkpoint barrier
            // on that input, only add to same input, otherwise we would not know the ordering
            // of the output since the Task might read the inputs in any order
            testHarness.processElement(new StreamRecord<>("Witam-0-1", initialTime), 0, 1);
            testHarness.processElement(new StreamRecord<>(42, initialTime), 1, 1);
            testHarness.processElement(new StreamRecord<>(1.0d, initialTime), 2, 1);
            expectedOutput.add(new StreamRecord<>("Witam-0-1", initialTime));
            expectedOutput.add(new StreamRecord<>("42", initialTime));
            expectedOutput.add(new StreamRecord<>("1.0", initialTime));

            // we should not yet see the barrier, only the two elements from non-blocked input

            assertThat(testHarness.getOutput(), contains(expectedOutput.toArray()));

            // Now give a later barrier to all inputs, this should unblock the first channel
            testHarness.processEvent(
                    new CheckpointBarrier(
                            1, 1, CheckpointOptions.forCheckpointWithDefaultLocation()),
                    0,
                    1);
            testHarness.processEvent(
                    new CheckpointBarrier(
                            1, 1, CheckpointOptions.forCheckpointWithDefaultLocation()),
                    1,
                    0);
            testHarness.processEvent(
                    new CheckpointBarrier(
                            1, 1, CheckpointOptions.forCheckpointWithDefaultLocation()),
                    1,
                    1);
            testHarness.processEvent(
                    new CheckpointBarrier(
                            1, 1, CheckpointOptions.forCheckpointWithDefaultLocation()),
                    2,
                    0);
            testHarness.processEvent(
                    new CheckpointBarrier(
                            1, 1, CheckpointOptions.forCheckpointWithDefaultLocation()),
                    2,
                    1);
            testHarness.processEvent(
                    new CheckpointBarrier(
                            1, 1, CheckpointOptions.forCheckpointWithDefaultLocation()),
                    0,
                    0);

            expectedOutput.add(new CancelCheckpointMarker(0));
            expectedOutput.add(
                    new CheckpointBarrier(
                            1, 1, CheckpointOptions.forCheckpointWithDefaultLocation()));

            assertThat(testHarness.getOutput(), contains(expectedOutput.toArray()));

            // Then give the earlier barrier, these should be ignored
            testHarness.processEvent(
                    new CheckpointBarrier(
                            0, 0, CheckpointOptions.forCheckpointWithDefaultLocation()),
                    0,
                    1);
            testHarness.processEvent(
                    new CheckpointBarrier(
                            0, 0, CheckpointOptions.forCheckpointWithDefaultLocation()),
                    1,
                    0);
            testHarness.processEvent(
                    new CheckpointBarrier(
                            0, 0, CheckpointOptions.forCheckpointWithDefaultLocation()),
                    1,
                    1);
            testHarness.processEvent(
                    new CheckpointBarrier(
                            0, 0, CheckpointOptions.forCheckpointWithDefaultLocation()),
                    2,
                    0);
            testHarness.processEvent(
                    new CheckpointBarrier(
                            0, 0, CheckpointOptions.forCheckpointWithDefaultLocation()),
                    2,
                    1);

            testHarness.waitForTaskCompletion();
            assertThat(testHarness.getOutput(), contains(expectedOutput.toArray()));
        }
    }

    /**
     * With chained sources, task's and main operator's number of input records are two different
     * things. The first one should take into account only records comming in from the network,
     * ignoring records produced inside the task itself (like via a chained source). Main operator
     * should on the other hand report all records from all of the inputs (regardless if it's a
     * network or chained input).
     */
    @Test
    public void testMetrics() throws Exception {

        HashMap<String, OperatorMetricGroup> operatorMetrics = new HashMap<>();

        TaskMetricGroup taskMetricGroup =
                new UnregisteredMetricGroups.UnregisteredTaskMetricGroup() {
                    @Override
                    public InternalOperatorMetricGroup getOrAddOperator(
                            OperatorID operatorID, String name) {
                        InternalOperatorMetricGroup operatorMetricGroup =
                                super.getOrAddOperator(operatorID, name);
                        operatorMetrics.put(name, operatorMetricGroup);
                        return operatorMetricGroup;
                    }
                };

        String mainOperatorName = "MainOperator";
        try (StreamTaskMailboxTestHarness<String> testHarness =
                new StreamTaskMailboxTestHarnessBuilder<>(
                                MultipleInputStreamTask::new, BasicTypeInfo.STRING_TYPE_INFO)
                        .modifyExecutionConfig(applyObjectReuse(objectReuse))
                        .addInput(BasicTypeInfo.STRING_TYPE_INFO)
                        .addSourceInput(
                                new SourceOperatorFactory<>(
                                        new LifeCycleTrackingMockSource(Boundedness.BOUNDED, 1),
                                        WatermarkStrategy.noWatermarks()),
                                BasicTypeInfo.INT_TYPE_INFO)
                        .addInput(BasicTypeInfo.STRING_TYPE_INFO)
                        .setupOperatorChain(new MapToStringMultipleInputOperatorFactory(3))
                        .name(mainOperatorName)
                        .chain(
                                new OneInputStreamTaskTest.DuplicatingOperator(),
                                BasicTypeInfo.STRING_TYPE_INFO.createSerializer(
                                        new ExecutionConfig()))
                        .chain(
                                new OneInputStreamTaskTest.DuplicatingOperator(),
                                BasicTypeInfo.STRING_TYPE_INFO.createSerializer(
                                        new ExecutionConfig()))
                        .chain(
                                new OneInputStreamTaskTest.DuplicatingOperator(),
                                BasicTypeInfo.STRING_TYPE_INFO.createSerializer(
                                        new ExecutionConfig()))
                        .finish()
                        .setTaskMetricGroup(taskMetricGroup)
                        .build()) {

            assertTrue(operatorMetrics.containsKey(mainOperatorName));
            OperatorMetricGroup mainOperatorMetrics = operatorMetrics.get(mainOperatorName);
            Counter numRecordsInCounter =
                    taskMetricGroup.getIOMetricGroup().getNumRecordsInCounter();
            Counter numRecordsOutCounter =
                    taskMetricGroup.getIOMetricGroup().getNumRecordsOutCounter();

            int numRecords1 = 5;
            int numRecords2 = 3;
            int numRecords3 = 2;
            // add source splits before processing any elements, so the MockSourceReader does not
            // end prematurely
            for (int x = 0; x < numRecords2; x++) {
                addSourceRecords(testHarness, 1, 42);
            }
            for (int x = 0; x < numRecords1; x++) {
                testHarness.processElement(new StreamRecord<>("hello"), 0, 0);
            }
            for (int x = 0; x < numRecords3; x++) {
                testHarness.processElement(new StreamRecord<>("hello"), 1, 0);
            }

            int networkRecordsIn = numRecords1 + numRecords3;
            int mainOperatorRecordsIn = networkRecordsIn + numRecords2;
            int totalRecordsOut =
                    mainOperatorRecordsIn
                            * 2
                            * 2
                            * 2; // there are three operators duplicating the records
            assertEquals(
                    mainOperatorRecordsIn,
                    mainOperatorMetrics.getIOMetricGroup().getNumRecordsInCounter().getCount());
            assertEquals(networkRecordsIn, numRecordsInCounter.getCount());
            assertEquals(totalRecordsOut, numRecordsOutCounter.getCount());
            testHarness.waitForTaskCompletion();
        }
    }

    static class DuplicatingOperator extends AbstractStreamOperatorV2<String>
            implements MultipleInputStreamOperator<String> {

        public DuplicatingOperator(StreamOperatorParameters<String> parameters) {
            super(parameters, 3);
        }

        @Override
        public List<Input> getInputs() {
            return Arrays.asList(
                    new DuplicatingInput(this, 1),
                    new DuplicatingInput(this, 2),
                    new DuplicatingInput(this, 3));
        }

        class DuplicatingInput extends AbstractInput<String, String> {
            public DuplicatingInput(AbstractStreamOperatorV2<String> owner, int inputId) {
                super(owner, inputId);
            }

            @Override
            public void processElement(StreamRecord<String> element) throws Exception {
                output.collect(element);
                output.collect(element);
            }
        }
    }

    @Test
    public void testLifeCycleOrder() throws Exception {
        try (StreamTaskMailboxTestHarness<String> testHarness =
                new StreamTaskMailboxTestHarnessBuilder<>(
                                MultipleInputStreamTask::new, BasicTypeInfo.STRING_TYPE_INFO)
                        .modifyExecutionConfig(applyObjectReuse(objectReuse))
                        .addInput(BasicTypeInfo.STRING_TYPE_INFO)
                        .addSourceInput(
                                new SourceOperatorFactory<>(
                                        new LifeCycleTrackingMockSource(Boundedness.BOUNDED, 1),
                                        WatermarkStrategy.noWatermarks()),
                                BasicTypeInfo.INT_TYPE_INFO)
                        .addInput(BasicTypeInfo.DOUBLE_TYPE_INFO)
                        .setupOperatorChain(
                                new LifeCycleTrackingMapToStringMultipleInputOperatorFactory())
                        .chain(
                                new LifeCycleTrackingMap<>(),
                                BasicTypeInfo.STRING_TYPE_INFO.createSerializer(
                                        new ExecutionConfig()))
                        .finish()
                        .build()) {

            testHarness.waitForTaskCompletion();
        }
        assertThat(
                LIFE_CYCLE_EVENTS,
                contains(
                        LifeCycleTrackingMap.OPEN,
                        LifeCycleTrackingMapToStringMultipleInputOperator.OPEN,
                        LifeCycleTrackingMockSourceReader.START,
                        LifeCycleTrackingMapToStringMultipleInputOperator.END_INPUT,
                        LifeCycleTrackingMapToStringMultipleInputOperator.END_INPUT,
                        LifeCycleTrackingMapToStringMultipleInputOperator.END_INPUT,
                        LifeCycleTrackingMapToStringMultipleInputOperator.FINISH,
                        LifeCycleTrackingMap.END_INPUT,
                        LifeCycleTrackingMap.CLOSE,
                        LifeCycleTrackingMapToStringMultipleInputOperator.CLOSE,
                        LifeCycleTrackingMockSourceReader.CLOSE));
    }

    @Test
    public void testInputFairness() throws Exception {
        try (StreamTaskMailboxTestHarness<String> testHarness =
                new StreamTaskMailboxTestHarnessBuilder<>(
                                MultipleInputStreamTask::new, BasicTypeInfo.STRING_TYPE_INFO)
                        .addInput(BasicTypeInfo.STRING_TYPE_INFO)
                        .addInput(BasicTypeInfo.STRING_TYPE_INFO)
                        .addInput(BasicTypeInfo.STRING_TYPE_INFO)
                        .setupOutputForSingletonOperatorChain(
                                new MapToStringMultipleInputOperatorFactory(3))
                        .build()) {
            ArrayDeque<Object> expectedOutput = new ArrayDeque<>();

            testHarness.setAutoProcess(false);
            testHarness.processElement(new StreamRecord<>("0"), 0);
            testHarness.processElement(new StreamRecord<>("1"), 0);
            testHarness.processElement(new StreamRecord<>("2"), 0);
            testHarness.processElement(new StreamRecord<>("3"), 0);

            testHarness.processElement(new StreamRecord<>("0"), 2);
            testHarness.processElement(new StreamRecord<>("1"), 2);

            testHarness.processAll();

            // We do not know which of the input will be picked first, but we are expecting them
            // to alternate
            // NOTE: the behaviour of alternation once per record is not part of any contract.
            // Task is just expected to not starve any of the inputs, it just happens to be
            // currently implemented in truly "fair" fashion. That means this test might need
            // to be adjusted if logic changes.
            expectedOutput.add(new StreamRecord<>("0"));
            expectedOutput.add(new StreamRecord<>("0"));
            expectedOutput.add(new StreamRecord<>("1"));
            expectedOutput.add(new StreamRecord<>("1"));
            expectedOutput.add(new StreamRecord<>("2"));
            expectedOutput.add(new StreamRecord<>("3"));

            assertThat(testHarness.getOutput(), contains(expectedOutput.toArray()));
        }
    }

    @Test
    public void testWatermark() throws Exception {
        try (StreamTaskMailboxTestHarness<String> testHarness =
                buildWatermarkTestHarness(2, false)) {
            ArrayDeque<Object> expectedOutput = new ArrayDeque<>();

            int initialTime = 0;

            testHarness.processElement(new Watermark(initialTime), 0, 0);
            testHarness.processElement(new Watermark(initialTime), 0, 1);

            addSourceRecords(testHarness, 1, initialTime);
            expectedOutput.add(
                    new StreamRecord<>("" + (initialTime), TimestampAssigner.NO_TIMESTAMP));

            testHarness.processElement(new Watermark(initialTime), 1, 0);

            assertThat(testHarness.getOutput(), contains(expectedOutput.toArray()));

            testHarness.processElement(new Watermark(initialTime), 1, 1);

            // now the watermark should have propagated, Map simply forward Watermarks
            expectedOutput.add(new Watermark(initialTime));
            assertThat(testHarness.getOutput(), contains(expectedOutput.toArray()));

            // contrary to checkpoint barriers these elements are not blocked by watermarks
            testHarness.processElement(new StreamRecord<>("Hello", initialTime), 0, 0);
            testHarness.processElement(new StreamRecord<>(42.0, initialTime), 1, 1);
            expectedOutput.add(new StreamRecord<>("Hello", initialTime));
            expectedOutput.add(new StreamRecord<>("42.0", initialTime));

            assertThat(testHarness.getOutput(), contains(expectedOutput.toArray()));

            testHarness.processElement(new Watermark(initialTime + 4), 0, 0);
            testHarness.processElement(new Watermark(initialTime + 3), 0, 1);

            addSourceRecords(testHarness, 1, initialTime + 3);
            expectedOutput.add(
                    new StreamRecord<>("" + (initialTime + 3), TimestampAssigner.NO_TIMESTAMP));

            testHarness.processElement(new Watermark(initialTime + 3), 1, 0);
            testHarness.processElement(new Watermark(initialTime + 2), 1, 1);

            // check whether we get the minimum of all the watermarks, this must also only occur in
            // the output after the two StreamRecords
            expectedOutput.add(new Watermark(initialTime + 2));
            assertThat(testHarness.getOutput(), contains(expectedOutput.toArray()));

            // advance watermark from one of the inputs, now we should get a new one since the
            // minimum increases
            testHarness.processElement(new Watermark(initialTime + 4), 1, 1);
            expectedOutput.add(new Watermark(initialTime + 3));
            assertThat(testHarness.getOutput(), contains(expectedOutput.toArray()));

            // advance the other inputs, now we should get a new one since the minimum increases
            // again
            testHarness.processElement(new Watermark(initialTime + 4), 0, 1);

            addSourceRecords(testHarness, 1, initialTime + 4);
            expectedOutput.add(
                    new StreamRecord<>("" + (initialTime + 4), TimestampAssigner.NO_TIMESTAMP));

            testHarness.processElement(new Watermark(initialTime + 4), 1, 0);
            expectedOutput.add(new Watermark(initialTime + 4));
            assertThat(testHarness.getOutput(), contains(expectedOutput.toArray()));

            List<String> resultElements =
                    TestHarnessUtil.getRawElementsFromOutput(testHarness.getOutput());
            assertEquals(5, resultElements.size());
        }
    }

    /**
     * This test verifies that watermarks and watermark statuses are correctly forwarded. This also
     * checks whether watermarks are forwarded only when we have received watermarks from all
     * inputs. The forwarded watermark must be the minimum of the watermarks of all active inputs.
     */
    @Test
    public void testWatermarkAndWatermarkStatusForwarding() throws Exception {
        try (StreamTaskMailboxTestHarness<String> testHarness =
                buildWatermarkTestHarness(2, true)) {
            ArrayDeque<Object> expectedOutput = new ArrayDeque<>();

            int initialTime = 0;

            // test whether idle input channels are acknowledged correctly when forwarding
            // watermarks
            testHarness.processElement(WatermarkStatus.IDLE, 0, 1);
            testHarness.processElement(new Watermark(initialTime + 6), 0, 0);
            testHarness.processElement(new Watermark(initialTime + 5), 1, 1);
            testHarness.processElement(WatermarkStatus.IDLE, 1, 0); // once this is acknowledged,
            expectedOutput.add(new Watermark(initialTime + 5));
            assertThat(testHarness.getOutput(), contains(expectedOutput.toArray()));

            // We make the second input idle, which should forward W=6 from the first input
            testHarness.processElement(WatermarkStatus.IDLE, 1, 1);
            expectedOutput.add(new Watermark(initialTime + 6));
            assertThat(testHarness.getOutput(), contains(expectedOutput.toArray()));

            // Make the first input idle
            testHarness.processElement(WatermarkStatus.IDLE, 0, 0);
            expectedOutput.add(WatermarkStatus.IDLE);
            assertThat(testHarness.getOutput(), contains(expectedOutput.toArray()));

            // make source active once again, emit a watermark and go idle again.
            addSourceRecords(testHarness, 1, initialTime + 10);

            expectedOutput.add(WatermarkStatus.ACTIVE); // activate source on new record
            expectedOutput.add(
                    new StreamRecord<>("" + (initialTime + 10), TimestampAssigner.NO_TIMESTAMP));
            expectedOutput.add(new Watermark(initialTime + 10)); // forward W from source
            expectedOutput.add(WatermarkStatus.IDLE); // go idle after reading all records
            testHarness.processAll();
            assertThat(testHarness.getOutput(), contains(expectedOutput.toArray()));

            // make some network input channel active again
            testHarness.processElement(WatermarkStatus.ACTIVE, 0, 1);
            expectedOutput.add(WatermarkStatus.ACTIVE);
            assertThat(testHarness.getOutput(), contains(expectedOutput.toArray()));
        }
    }

    @Test
    public void testAdvanceToEndOfEventTime() throws Exception {
        try (StreamTaskMailboxTestHarness<String> testHarness =
                buildWatermarkTestHarness(2, false)) {
            testHarness.processElement(Watermark.MAX_WATERMARK, 0, 0);
            testHarness.processElement(Watermark.MAX_WATERMARK, 0, 1);

            testHarness.getStreamTask().advanceToEndOfEventTime();

            testHarness.processElement(Watermark.MAX_WATERMARK, 1, 0);

            assertThat(testHarness.getOutput(), not(contains(Watermark.MAX_WATERMARK)));

            testHarness.processElement(Watermark.MAX_WATERMARK, 1, 1);
            assertThat(testHarness.getOutput(), contains(Watermark.MAX_WATERMARK));
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testWatermarkMetrics() throws Exception {
        OperatorID mainOperatorId = new OperatorID();
        OperatorID chainedOperatorId = new OperatorID();

        InterceptingOperatorMetricGroup mainOperatorMetricGroup =
                new InterceptingOperatorMetricGroup();
        InterceptingOperatorMetricGroup chainedOperatorMetricGroup =
                new InterceptingOperatorMetricGroup();
        InterceptingTaskMetricGroup taskMetricGroup =
                new InterceptingTaskMetricGroup() {
                    @Override
                    public InternalOperatorMetricGroup getOrAddOperator(
                            OperatorID id, String name) {
                        if (id.equals(mainOperatorId)) {
                            return mainOperatorMetricGroup;
                        } else if (id.equals(chainedOperatorId)) {
                            return chainedOperatorMetricGroup;
                        } else {
                            return super.getOrAddOperator(id, name);
                        }
                    }
                };

        try (StreamTaskMailboxTestHarness<String> testHarness =
                new StreamTaskMailboxTestHarnessBuilder<>(
                                MultipleInputStreamTask::new, BasicTypeInfo.STRING_TYPE_INFO)
                        .modifyExecutionConfig(applyObjectReuse(objectReuse))
                        .addInput(BasicTypeInfo.STRING_TYPE_INFO)
                        .addSourceInput(
                                new SourceOperatorFactory<>(
                                        new MockSource(
                                                Boundedness.CONTINUOUS_UNBOUNDED, 2, true, false),
                                        WatermarkStrategy.forGenerator(
                                                ctx -> new RecordToWatermarkGenerator())),
                                BasicTypeInfo.INT_TYPE_INFO)
                        .addInput(BasicTypeInfo.DOUBLE_TYPE_INFO)
                        .setupOperatorChain(
                                mainOperatorId, new MapToStringMultipleInputOperatorFactory(3))
                        .chain(
                                chainedOperatorId,
                                new WatermarkMetricOperator(),
                                BasicTypeInfo.STRING_TYPE_INFO.createSerializer(
                                        new ExecutionConfig()))
                        .finish()
                        .setTaskMetricGroup(taskMetricGroup)
                        .build()) {
            Gauge<Long> taskInputWatermarkGauge =
                    (Gauge<Long>) taskMetricGroup.get(MetricNames.IO_CURRENT_INPUT_WATERMARK);
            Gauge<Long> mainInput1WatermarkGauge =
                    (Gauge<Long>)
                            mainOperatorMetricGroup.get(MetricNames.currentInputWatermarkName(1));
            Gauge<Long> mainInput2WatermarkGauge =
                    (Gauge<Long>)
                            mainOperatorMetricGroup.get(MetricNames.currentInputWatermarkName(2));
            Gauge<Long> mainInput3WatermarkGauge =
                    (Gauge<Long>)
                            mainOperatorMetricGroup.get(MetricNames.currentInputWatermarkName(3));
            Gauge<Long> mainInputWatermarkGauge =
                    (Gauge<Long>)
                            mainOperatorMetricGroup.get(MetricNames.IO_CURRENT_INPUT_WATERMARK);
            Gauge<Long> mainOutputWatermarkGauge =
                    (Gauge<Long>)
                            mainOperatorMetricGroup.get(MetricNames.IO_CURRENT_OUTPUT_WATERMARK);
            Gauge<Long> chainedInputWatermarkGauge =
                    (Gauge<Long>)
                            chainedOperatorMetricGroup.get(MetricNames.IO_CURRENT_INPUT_WATERMARK);
            Gauge<Long> chainedOutputWatermarkGauge =
                    (Gauge<Long>)
                            chainedOperatorMetricGroup.get(MetricNames.IO_CURRENT_OUTPUT_WATERMARK);

            assertEquals(Long.MIN_VALUE, taskInputWatermarkGauge.getValue().longValue());
            assertEquals(Long.MIN_VALUE, mainInputWatermarkGauge.getValue().longValue());
            assertEquals(Long.MIN_VALUE, mainInput1WatermarkGauge.getValue().longValue());
            assertEquals(Long.MIN_VALUE, mainInput2WatermarkGauge.getValue().longValue());
            assertEquals(Long.MIN_VALUE, mainInput3WatermarkGauge.getValue().longValue());
            assertEquals(Long.MIN_VALUE, mainOutputWatermarkGauge.getValue().longValue());
            assertEquals(Long.MIN_VALUE, chainedInputWatermarkGauge.getValue().longValue());
            assertEquals(Long.MIN_VALUE, chainedOutputWatermarkGauge.getValue().longValue());

            testHarness.processElement(new Watermark(1L), 0);
            assertEquals(Long.MIN_VALUE, taskInputWatermarkGauge.getValue().longValue());
            assertEquals(Long.MIN_VALUE, mainInputWatermarkGauge.getValue().longValue());
            assertEquals(1L, mainInput1WatermarkGauge.getValue().longValue());
            assertEquals(Long.MIN_VALUE, mainInput2WatermarkGauge.getValue().longValue());
            assertEquals(Long.MIN_VALUE, mainInput3WatermarkGauge.getValue().longValue());
            assertEquals(Long.MIN_VALUE, mainOutputWatermarkGauge.getValue().longValue());
            assertEquals(Long.MIN_VALUE, chainedInputWatermarkGauge.getValue().longValue());
            assertEquals(Long.MIN_VALUE, chainedOutputWatermarkGauge.getValue().longValue());

            addSourceRecords(testHarness, 1, 2);
            testHarness.processAll();
            assertEquals(Long.MIN_VALUE, taskInputWatermarkGauge.getValue().longValue());
            assertEquals(Long.MIN_VALUE, mainInputWatermarkGauge.getValue().longValue());
            assertEquals(1L, mainInput1WatermarkGauge.getValue().longValue());
            assertEquals(2L, mainInput2WatermarkGauge.getValue().longValue());
            assertEquals(Long.MIN_VALUE, mainInput3WatermarkGauge.getValue().longValue());
            assertEquals(Long.MIN_VALUE, mainOutputWatermarkGauge.getValue().longValue());
            assertEquals(Long.MIN_VALUE, chainedInputWatermarkGauge.getValue().longValue());
            assertEquals(Long.MIN_VALUE, chainedOutputWatermarkGauge.getValue().longValue());

            testHarness.processElement(new Watermark(2L), 1);
            assertEquals(1L, taskInputWatermarkGauge.getValue().longValue());
            assertEquals(1L, mainInputWatermarkGauge.getValue().longValue());
            assertEquals(1L, mainInput1WatermarkGauge.getValue().longValue());
            assertEquals(2L, mainInput2WatermarkGauge.getValue().longValue());
            assertEquals(2L, mainInput3WatermarkGauge.getValue().longValue());
            assertEquals(1L, mainOutputWatermarkGauge.getValue().longValue());
            assertEquals(1L, chainedInputWatermarkGauge.getValue().longValue());
            assertEquals(2L, chainedOutputWatermarkGauge.getValue().longValue());

            testHarness.processElement(new Watermark(4L), 0);
            addSourceRecords(testHarness, 1, 3);
            testHarness.processAll();
            assertEquals(2L, taskInputWatermarkGauge.getValue().longValue());
            assertEquals(2L, mainInputWatermarkGauge.getValue().longValue());
            assertEquals(4L, mainInput1WatermarkGauge.getValue().longValue());
            assertEquals(3L, mainInput2WatermarkGauge.getValue().longValue());
            assertEquals(2L, mainInput3WatermarkGauge.getValue().longValue());
            assertEquals(2L, mainOutputWatermarkGauge.getValue().longValue());
            assertEquals(2L, chainedInputWatermarkGauge.getValue().longValue());
            assertEquals(4L, chainedOutputWatermarkGauge.getValue().longValue());

            finishAddingRecords(testHarness, 1);
            testHarness.endInput();
            testHarness.waitForTaskCompletion();
            testHarness.finishProcessing();
        }
    }

    /**
     * Tests the checkpoint related metrics are registered into {@link TaskIOMetricGroup} correctly
     * while generating the {@link TwoInputStreamTask}.
     */
    @Test
    public void testCheckpointBarrierMetrics() throws Exception {
        final Map<String, Metric> metrics = new ConcurrentHashMap<>();
        final TaskMetricGroup taskMetricGroup =
                StreamTaskTestHarness.createTaskMetricGroup(metrics);

        try (StreamTaskMailboxTestHarness<String> testHarness =
                new StreamTaskMailboxTestHarnessBuilder<>(
                                MultipleInputStreamTask::new, BasicTypeInfo.STRING_TYPE_INFO)
                        .addInput(BasicTypeInfo.STRING_TYPE_INFO, 2)
                        .addInput(BasicTypeInfo.INT_TYPE_INFO, 2)
                        .addInput(BasicTypeInfo.DOUBLE_TYPE_INFO, 2)
                        .setupOutputForSingletonOperatorChain(
                                new MapToStringMultipleInputOperatorFactory(3))
                        .setTaskMetricGroup(taskMetricGroup)
                        .build()) {

            assertThat(metrics, IsMapContaining.hasKey(MetricNames.CHECKPOINT_ALIGNMENT_TIME));
            assertThat(metrics, IsMapContaining.hasKey(MetricNames.CHECKPOINT_START_DELAY_TIME));

            testHarness.endInput();
            testHarness.waitForTaskCompletion();
        }
    }

    /** The CanEmitBatchOfRecords should always be false for {@link MultipleInputStreamTask}. */
    @Test
    public void testCanEmitBatchOfRecords() throws Exception {
        AvailabilityProvider.AvailabilityHelper availabilityHelper =
                new AvailabilityProvider.AvailabilityHelper();
        try (StreamTaskMailboxTestHarness<String> testHarness =
                new StreamTaskMailboxTestHarnessBuilder<>(
                                MultipleInputStreamTask::new, BasicTypeInfo.STRING_TYPE_INFO)
                        .addInput(BasicTypeInfo.STRING_TYPE_INFO)
                        .addInput(BasicTypeInfo.INT_TYPE_INFO)
                        .addInput(BasicTypeInfo.DOUBLE_TYPE_INFO)
                        .addAdditionalOutput(
                                new ResultPartitionWriterWithAvailabilityHelper(availabilityHelper))
                        .setupOperatorChain(new MapToStringMultipleInputOperatorFactory(3))
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
    public void testLatencyMarker() throws Exception {
        final Map<String, Metric> metrics = new ConcurrentHashMap<>();
        final TaskMetricGroup taskMetricGroup =
                StreamTaskTestHarness.createTaskMetricGroup(metrics);

        try (StreamTaskMailboxTestHarness<String> testHarness =
                new StreamTaskMailboxTestHarnessBuilder<>(
                                MultipleInputStreamTask::new, BasicTypeInfo.STRING_TYPE_INFO)
                        .addInput(BasicTypeInfo.STRING_TYPE_INFO)
                        .addInput(BasicTypeInfo.INT_TYPE_INFO)
                        .addInput(BasicTypeInfo.DOUBLE_TYPE_INFO)
                        .setupOutputForSingletonOperatorChain(
                                new MapToStringMultipleInputOperatorFactory(3))
                        .setTaskMetricGroup(taskMetricGroup)
                        .build()) {
            ArrayDeque<Object> expectedOutput = new ArrayDeque<>();

            OperatorID sourceId = new OperatorID();
            LatencyMarker latencyMarker = new LatencyMarker(42L, sourceId, 0);
            testHarness.processElement(latencyMarker);
            expectedOutput.add(latencyMarker);

            assertThat(testHarness.getOutput(), contains(expectedOutput.toArray()));

            testHarness.endInput();
            testHarness.waitForTaskCompletion();
        }
    }

    @Test
    public void testTriggeringAlignedNoTimeoutCheckpointWithFinishedChannels() throws Exception {
        testTriggeringCheckpointWithFinishedChannels(
                CheckpointOptions.alignedNoTimeout(
                        CheckpointType.CHECKPOINT,
                        CheckpointStorageLocationReference.getDefault()));
    }

    @Test
    public void testTriggeringUnalignedCheckpointWithFinishedChannels() throws Exception {
        testTriggeringCheckpointWithFinishedChannels(
                CheckpointOptions.unaligned(
                        CheckpointType.CHECKPOINT,
                        CheckpointStorageLocationReference.getDefault()));
    }

    @Test
    public void testTriggeringAlignedWithTimeoutCheckpointWithFinishedChannels() throws Exception {
        testTriggeringCheckpointWithFinishedChannels(
                CheckpointOptions.alignedWithTimeout(
                        CheckpointType.CHECKPOINT,
                        CheckpointStorageLocationReference.getDefault(),
                        10L));
    }

    private void testTriggeringCheckpointWithFinishedChannels(CheckpointOptions checkpointOptions)
            throws Exception {
        ResultPartition[] partitionWriters = new ResultPartition[2];
        try {
            for (int i = 0; i < partitionWriters.length; ++i) {
                partitionWriters[i] =
                        PartitionTestUtils.createPartition(ResultPartitionType.PIPELINED_BOUNDED);
                partitionWriters[i].setup();
            }

            CompletingCheckpointResponder checkpointResponder = new CompletingCheckpointResponder();
            try (StreamTaskMailboxTestHarness<String> testHarness =
                    new StreamTaskMailboxTestHarnessBuilder<>(
                                    MultipleInputStreamTask::new, BasicTypeInfo.STRING_TYPE_INFO)
                            .addInput(BasicTypeInfo.STRING_TYPE_INFO)
                            .addInput(BasicTypeInfo.INT_TYPE_INFO)
                            .addInput(BasicTypeInfo.DOUBLE_TYPE_INFO)
                            .addAdditionalOutput(partitionWriters)
                            .setCheckpointResponder(checkpointResponder)
                            .modifyStreamConfig(
                                    config -> {
                                        config.setCheckpointingEnabled(true);
                                        config.setUnalignedCheckpointsEnabled(
                                                checkpointOptions.isUnalignedCheckpoint()
                                                        || checkpointOptions.isTimeoutable());
                                    })
                            .setupOperatorChain(new MapToStringMultipleInputOperatorFactory(3))
                            .finishForSingletonOperatorChain(StringSerializer.INSTANCE)
                            .build()) {

                checkpointResponder.setHandlers(
                        testHarness.streamTask::notifyCheckpointCompleteAsync,
                        testHarness.streamTask::notifyCheckpointAbortAsync);
                testHarness.getStreamTask().getCheckpointBarrierHandler().get();

                // Tests triggering checkpoint when all the inputs are alive.
                CompletableFuture<Boolean> checkpointFuture =
                        triggerCheckpoint(testHarness, 2, checkpointOptions);
                processMailTillCheckpointSucceeds(testHarness, checkpointFuture);
                assertEquals(2, testHarness.getTaskStateManager().getReportedCheckpointId());

                // Tests triggering checkpoint after some inputs have received EndOfPartition.
                testHarness.processEvent(new EndOfData(StopMode.DRAIN), 0, 0);
                testHarness.processEvent(EndOfPartitionEvent.INSTANCE, 0, 0);
                checkpointFuture = triggerCheckpoint(testHarness, 4, checkpointOptions);
                processMailTillCheckpointSucceeds(testHarness, checkpointFuture);
                assertEquals(4, testHarness.getTaskStateManager().getReportedCheckpointId());

                // Tests triggering checkpoint after all the inputs have received EndOfPartition.
                testHarness.processEvent(new EndOfData(StopMode.DRAIN), 1, 0);
                testHarness.processEvent(new EndOfData(StopMode.DRAIN), 2, 0);
                testHarness.processEvent(EndOfPartitionEvent.INSTANCE, 1, 0);
                testHarness.processEvent(EndOfPartitionEvent.INSTANCE, 2, 0);
                checkpointFuture = triggerCheckpoint(testHarness, 6, checkpointOptions);

                // Notifies the result partition that all records are processed after the
                // last checkpoint is triggered.
                checkpointFuture.thenAccept(
                        (ignored) -> {
                            for (ResultPartition resultPartition : partitionWriters) {
                                resultPartition.onSubpartitionAllDataProcessed(0);
                            }
                        });

                // The checkpoint 6 would be triggered successfully.
                testHarness.processAll();
                testHarness.finishProcessing();
                assertTrue(checkpointFuture.isDone());
                testHarness.getTaskStateManager().getWaitForReportLatch().await();
                assertEquals(6, testHarness.getTaskStateManager().getReportedCheckpointId());

                // Each result partition should have emitted 3 barriers and 1 EndOfUserRecordsEvent.
                for (ResultPartition resultPartition : partitionWriters) {
                    assertEquals(4, resultPartition.getNumberOfQueuedBuffers());
                }
            }
        } finally {
            for (ResultPartitionWriter writer : partitionWriters) {
                if (writer != null) {
                    writer.close();
                }
            }
        }
    }

    @Test
    public void testSkipExecutionsIfFinishedOnRestore() throws Exception {
        OperatorID nonSourceOperatorId = new OperatorID();

        try (StreamTaskMailboxTestHarness<String> testHarness =
                new StreamTaskMailboxTestHarnessBuilder<>(
                                MultipleInputStreamTask::new, BasicTypeInfo.STRING_TYPE_INFO)
                        .setCollectNetworkEvents()
                        .modifyStreamConfig(config -> config.setCheckpointingEnabled(true))
                        .modifyExecutionConfig(applyObjectReuse(objectReuse))
                        .addInput(BasicTypeInfo.INT_TYPE_INFO)
                        .addInput(BasicTypeInfo.INT_TYPE_INFO)
                        .addInput(BasicTypeInfo.INT_TYPE_INFO)
                        .setTaskStateSnapshot(1, TaskStateSnapshot.FINISHED_ON_RESTORE)
                        .setupOperatorChain(
                                nonSourceOperatorId,
                                new LifeCycleMonitorMultipleInputOperatorFactory())
                        .chain(new TestFinishedOnRestoreStreamOperator(), StringSerializer.INSTANCE)
                        .finish()
                        .build()) {

            testHarness.processElement(Watermark.MAX_WATERMARK, 0);
            testHarness.processElement(Watermark.MAX_WATERMARK, 1);
            testHarness.processElement(Watermark.MAX_WATERMARK, 2);
            testHarness.waitForTaskCompletion();
            assertThat(
                    testHarness.getOutput(),
                    contains(Watermark.MAX_WATERMARK, new EndOfData(StopMode.DRAIN)));
        }
    }

    @Test
    public void testTriggeringStopWithSavepointWithDrain() throws Exception {
        SourceOperatorFactory<Integer> sourceOperatorFactory =
                new SourceOperatorFactory<>(
                        new MockSource(Boundedness.CONTINUOUS_UNBOUNDED, 2),
                        WatermarkStrategy.noWatermarks());

        CompletableFuture<Boolean> checkpointCompleted = new CompletableFuture<>();
        CheckpointResponder checkpointResponder =
                new TestCheckpointResponder() {
                    @Override
                    public void acknowledgeCheckpoint(
                            JobID jobID,
                            ExecutionAttemptID executionAttemptID,
                            long checkpointId,
                            CheckpointMetrics checkpointMetrics,
                            TaskStateSnapshot subtaskState) {
                        super.acknowledgeCheckpoint(
                                jobID,
                                executionAttemptID,
                                checkpointId,
                                checkpointMetrics,
                                subtaskState);
                        checkpointCompleted.complete(null);
                    }
                };

        try (StreamTaskMailboxTestHarness<String> testHarness =
                new StreamTaskMailboxTestHarnessBuilder<>(
                                MultipleInputStreamTask::new, BasicTypeInfo.STRING_TYPE_INFO)
                        .setCollectNetworkEvents()
                        .modifyStreamConfig(config -> config.setCheckpointingEnabled(true))
                        .modifyExecutionConfig(applyObjectReuse(objectReuse))
                        .addInput(BasicTypeInfo.INT_TYPE_INFO)
                        .addInput(BasicTypeInfo.INT_TYPE_INFO)
                        .addInput(BasicTypeInfo.INT_TYPE_INFO)
                        .setTaskStateSnapshot(1, TaskStateSnapshot.FINISHED_ON_RESTORE)
                        .setupOperatorChain(new LifeCycleMonitorMultipleInputOperatorFactory())
                        .finishForSingletonOperatorChain(StringSerializer.INSTANCE)
                        .setCheckpointResponder(checkpointResponder)
                        .build()) {
            CompletableFuture<Boolean> triggerResult =
                    testHarness.streamTask.triggerCheckpointAsync(
                            new CheckpointMetaData(2, 2),
                            CheckpointOptions.alignedNoTimeout(
                                    SavepointType.terminate(SavepointFormatType.CANONICAL),
                                    CheckpointStorageLocationReference.getDefault()));
            checkpointCompleted.whenComplete(
                    (ignored, exception) ->
                            testHarness.streamTask.notifyCheckpointCompleteAsync(2));
            testHarness.waitForTaskCompletion();
            testHarness.finishProcessing();

            assertTrue(triggerResult.isDone());
            assertTrue(triggerResult.get());
            assertTrue(checkpointCompleted.isDone());
        }
    }

    /** Test implementation of {@link MultipleInputStreamOperator}. */
    protected static class MapToStringMultipleInputOperator extends AbstractStreamOperatorV2<String>
            implements MultipleInputStreamOperator<String> {
        private static final long serialVersionUID = 1L;

        private final int numberOfInputs;
        private final boolean emitOnFinish;
        private boolean openCalled;
        private boolean closeCalled;

        public MapToStringMultipleInputOperator(
                StreamOperatorParameters<String> parameters, int numberOfInputs) {
            this(parameters, numberOfInputs, false);
        }

        public MapToStringMultipleInputOperator(
                StreamOperatorParameters<String> parameters,
                int numberOfInputs,
                boolean emitOnFinish) {
            super(parameters, numberOfInputs);
            this.numberOfInputs = numberOfInputs;
            this.emitOnFinish = emitOnFinish;
        }

        @Override
        public void open() throws Exception {
            super.open();
            if (closeCalled) {
                Assert.fail("Close called before open.");
            }
            openCalled = true;
        }

        @Override
        public void finish() throws Exception {
            if (emitOnFinish) {
                output.collect(new StreamRecord<>("FINISH"));
            }
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
        public List<Input> getInputs() {
            checkArgument(numberOfInputs <= 4);
            return Arrays.<Input>asList(
                            new MapToStringInput<String>(this, 1),
                            new MapToStringInput<Integer>(this, 2),
                            new MapToStringInput<Double>(this, 3),
                            new MapToStringInput<String>(this, 4))
                    .subList(0, numberOfInputs);
        }

        public boolean wasCloseCalled() {
            return closeCalled;
        }

        /** {@link Input} for {@link MapToStringMultipleInputOperator}. */
        public class MapToStringInput<T> extends AbstractInput<T, String> {
            public MapToStringInput(AbstractStreamOperatorV2<String> owner, int inputId) {
                super(owner, inputId);
            }

            @Override
            public void processElement(StreamRecord<T> element) throws Exception {
                if (!openCalled) {
                    Assert.fail("Open was not called before run.");
                }
                if (element.hasTimestamp()) {
                    output.collect(
                            new StreamRecord<>(
                                    element.getValue().toString(), element.getTimestamp()));
                } else {
                    output.collect(new StreamRecord<>(element.getValue().toString()));
                }
            }
        }
    }

    /** Factory for {@link MapToStringMultipleInputOperator}. */
    protected static class MapToStringMultipleInputOperatorFactory
            extends AbstractStreamOperatorFactory<String> {
        private final int numberOfInputs;
        private final boolean emitOnFinish;

        public MapToStringMultipleInputOperatorFactory(int numberOfInputs) {
            this(numberOfInputs, false);
        }

        public MapToStringMultipleInputOperatorFactory(int numberOfInputs, boolean emitOnFinish) {
            this.numberOfInputs = numberOfInputs;
            this.emitOnFinish = emitOnFinish;
        }

        @Override
        public <T extends StreamOperator<String>> T createStreamOperator(
                StreamOperatorParameters<String> parameters) {
            return (T)
                    new MapToStringMultipleInputOperator(parameters, numberOfInputs, emitOnFinish);
        }

        @Override
        public Class<? extends StreamOperator<String>> getStreamOperatorClass(
                ClassLoader classLoader) {
            return MapToStringMultipleInputOperator.class;
        }
    }

    static Consumer<ExecutionConfig> applyObjectReuse(boolean objectReuse) {
        return config -> {
            if (objectReuse) {
                config.enableObjectReuse();
            } else {
                config.disableObjectReuse();
            }
        };
    }

    static StreamTaskMailboxTestHarness<String> buildTestHarness(boolean objectReuse)
            throws Exception {
        return buildTestHarness(false, objectReuse);
    }

    static StreamTaskMailboxTestHarness<String> buildTestHarness(
            boolean unaligned, boolean objectReuse) throws Exception {
        return new StreamTaskMailboxTestHarnessBuilder<>(
                        MultipleInputStreamTask::new, BasicTypeInfo.STRING_TYPE_INFO)
                .modifyExecutionConfig(applyObjectReuse(objectReuse))
                .modifyStreamConfig(config -> config.setUnalignedCheckpointsEnabled(unaligned))
                .modifyStreamConfig(config -> config.setAlignedCheckpointTimeout(Duration.ZERO))
                .addInput(BasicTypeInfo.STRING_TYPE_INFO)
                .addSourceInput(
                        new SourceOperatorFactory<>(
                                new MockSource(Boundedness.BOUNDED, 1),
                                WatermarkStrategy.noWatermarks()),
                        BasicTypeInfo.INT_TYPE_INFO)
                .addInput(BasicTypeInfo.DOUBLE_TYPE_INFO)
                .setupOutputForSingletonOperatorChain(
                        new MapToStringMultipleInputOperatorFactory(3))
                .build();
    }

    static void addSourceRecords(
            StreamTaskMailboxTestHarness<String> testHarness, int sourceId, int... records)
            throws Exception {
        addSourceRecords(testHarness, sourceId, Boundedness.BOUNDED, records);
    }

    static void addSourceRecords(
            StreamTaskMailboxTestHarness<String> testHarness,
            int sourceId,
            Boundedness boundedness,
            int... records)
            throws Exception {
        OperatorID sourceOperatorID = getSourceOperatorID(testHarness, sourceId);

        // Prepare the source split and assign it to the source reader.
        MockSourceSplit split =
                new MockSourceSplit(
                        0,
                        0,
                        boundedness == Boundedness.BOUNDED ? records.length : Integer.MAX_VALUE);
        for (int record : records) {
            split.addRecord(record);
        }

        // Assign the split to the source reader.
        AddSplitEvent<MockSourceSplit> addSplitEvent =
                new AddSplitEvent<>(
                        Collections.singletonList(split), new MockSourceSplitSerializer());

        testHarness
                .getStreamTask()
                .dispatchOperatorEvent(sourceOperatorID, new SerializedValue<>(addSplitEvent));
    }

    private StreamTaskMailboxTestHarness<String> buildWatermarkTestHarness(
            int inputChannels, boolean readerMarkIdleOnNoSplits) throws Exception {
        return new StreamTaskMailboxTestHarnessBuilder<>(
                        MultipleInputStreamTask::new, BasicTypeInfo.STRING_TYPE_INFO)
                .modifyExecutionConfig(applyObjectReuse(objectReuse))
                .addInput(BasicTypeInfo.STRING_TYPE_INFO, inputChannels)
                .addSourceInput(
                        new SourceOperatorFactory<>(
                                new MockSource(
                                        Boundedness.CONTINUOUS_UNBOUNDED,
                                        2,
                                        true,
                                        readerMarkIdleOnNoSplits),
                                WatermarkStrategy.forGenerator(
                                        ctx -> new RecordToWatermarkGenerator())),
                        BasicTypeInfo.INT_TYPE_INFO)
                .addInput(BasicTypeInfo.DOUBLE_TYPE_INFO, inputChannels)
                .setupOutputForSingletonOperatorChain(
                        new MapToStringMultipleInputOperatorFactory(3))
                .build();
    }

    private static OperatorID getSourceOperatorID(
            StreamTaskMailboxTestHarness<String> testHarness, int sourceId) {
        StreamConfig.InputConfig[] inputs =
                testHarness
                        .getStreamTask()
                        .getConfiguration()
                        .getInputs(testHarness.getClass().getClassLoader());
        StreamConfig.SourceInputConfig input = (StreamConfig.SourceInputConfig) inputs[sourceId];
        return testHarness.getStreamTask().operatorChain.getSourceTaskInput(input).getOperatorID();
    }

    private void finishAddingRecords(StreamTaskMailboxTestHarness<String> testHarness, int sourceId)
            throws Exception {
        testHarness
                .getStreamTask()
                .dispatchOperatorEvent(
                        getSourceOperatorID(testHarness, sourceId),
                        new SerializedValue<>(new NoMoreSplitsEvent()));
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
                                MultipleInputStreamTask::new, BasicTypeInfo.INT_TYPE_INFO)
                        .modifyExecutionConfig(applyObjectReuse(objectReuse))
                        .addInput(BasicTypeInfo.INT_TYPE_INFO)
                        .addInput(BasicTypeInfo.INT_TYPE_INFO)
                        .addInput(BasicTypeInfo.INT_TYPE_INFO)
                        .addAdditionalOutput(partitionWriters)
                        .setupOperatorChain(new PassThroughOperatorFactory<>())
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

            int numOddRecords = 5;
            int numEvenRecords = 3;
            int numNaturalRecords = 2;
            for (int x = 0; x < numOddRecords; x++) {
                testHarness.processElement(new StreamRecord<>(x * 2 + 1));
            }
            for (int x = 0; x < numEvenRecords; x++) {
                testHarness.processElement(new StreamRecord<>(x * 2));
            }
            for (int x = 0; x < numNaturalRecords; x++) {
                testHarness.processElement(new StreamRecord<>(x));
            }

            int totalOddRecords = numOddRecords + numNaturalRecords / 2;
            int totalEvenRecords = numEvenRecords + (int) Math.ceil(numNaturalRecords / 2.0);

            final int oddEvenOperatorOutputsWithOddTag = totalOddRecords;
            final int oddEvenOperatorOutputsWithoutTag = totalOddRecords + totalEvenRecords;
            final int duplicatingOperatorOutput = (totalOddRecords + totalEvenRecords) * 2;
            assertEquals(totalOddRecords + totalEvenRecords, numRecordsInCounter.getCount());
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

    static class PassThroughOperator<T> extends AbstractStreamOperatorV2<T>
            implements MultipleInputStreamOperator<T> {

        public PassThroughOperator(StreamOperatorParameters<T> parameters) {
            super(parameters, 3);
        }

        @Override
        public List<Input> getInputs() {
            return Arrays.asList(
                    new PassThroughInput<>(this, 1),
                    new PassThroughInput<>(this, 2),
                    new PassThroughInput<>(this, 3));
        }

        static class PassThroughInput<I> extends AbstractInput<I, I> {

            public PassThroughInput(AbstractStreamOperatorV2<I> owner, int inputId) {
                super(owner, inputId);
            }

            @Override
            public void processElement(StreamRecord<I> element) throws Exception {
                output.collect(element);
            }
        }
    }

    private static class PassThroughOperatorFactory<T> extends AbstractStreamOperatorFactory<T> {
        @Override
        public <O extends StreamOperator<T>> O createStreamOperator(
                StreamOperatorParameters<T> parameters) {
            return (O) new PassThroughOperator<>(parameters);
        }

        @Override
        public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
            return PassThroughOperator.class;
        }
    }

    static class OddEvenOperator extends AbstractStreamOperatorV2<Integer>
            implements MultipleInputStreamOperator<Integer> {

        public OddEvenOperator(StreamOperatorParameters<Integer> parameters) {
            super(parameters, 3);
        }

        @Override
        public List<Input> getInputs() {
            return Arrays.asList(
                    new OddEvenInput(this, 1),
                    new OddEvenInput(this, 2),
                    new OddEvenInput(this, 3));
        }

        static class OddEvenInput extends AbstractInput<Integer, Integer> {
            private final OutputTag<Integer> oddOutputTag =
                    new OutputTag<>("odd", BasicTypeInfo.INT_TYPE_INFO);
            private final OutputTag<Integer> evenOutputTag =
                    new OutputTag<>("even", BasicTypeInfo.INT_TYPE_INFO);

            public OddEvenInput(AbstractStreamOperatorV2<Integer> owner, int inputId) {
                super(owner, inputId);
            }

            @Override
            public void processElement(StreamRecord<Integer> element) throws Exception {
                if (element.getValue() % 2 == 0) {
                    output.collect(evenOutputTag, element);
                } else {
                    output.collect(oddOutputTag, element);
                }
                output.collect(element);
            }
        }
    }

    private static class OddEvenOperatorFactory extends AbstractStreamOperatorFactory<Integer> {
        @Override
        public <T extends StreamOperator<Integer>> T createStreamOperator(
                StreamOperatorParameters<Integer> parameters) {
            return (T) new OddEvenOperator(parameters);
        }

        @Override
        public Class<? extends StreamOperator<Integer>> getStreamOperatorClass(
                ClassLoader classLoader) {
            return OddEvenOperator.class;
        }
    }

    static class LifeCycleTrackingMapToStringMultipleInputOperator
            extends MapToStringMultipleInputOperator implements BoundedMultiInput {
        public static final String OPEN = "MultipleInputOperator#open";
        public static final String CLOSE = "MultipleInputOperator#close";
        public static final String FINISH = "MultipleInputOperator#finish";
        public static final String END_INPUT = "MultipleInputOperator#endInput";

        private static final long serialVersionUID = 1L;

        public LifeCycleTrackingMapToStringMultipleInputOperator(
                StreamOperatorParameters<String> parameters) {
            super(parameters, 3);
        }

        @Override
        public void open() throws Exception {
            LIFE_CYCLE_EVENTS.add(OPEN);
            super.open();
        }

        @Override
        public void close() throws Exception {
            LIFE_CYCLE_EVENTS.add(CLOSE);
            super.close();
        }

        @Override
        public void endInput(int inputId) {
            LIFE_CYCLE_EVENTS.add(END_INPUT);
        }

        @Override
        public void finish() throws Exception {
            LIFE_CYCLE_EVENTS.add(FINISH);
        }
    }

    static class LifeCycleTrackingMapToStringMultipleInputOperatorFactory
            extends AbstractStreamOperatorFactory<String> {
        @Override
        public <T extends StreamOperator<String>> T createStreamOperator(
                StreamOperatorParameters<String> parameters) {
            return (T) new LifeCycleTrackingMapToStringMultipleInputOperator(parameters);
        }

        @Override
        public Class<? extends StreamOperator<String>> getStreamOperatorClass(
                ClassLoader classLoader) {
            return LifeCycleTrackingMapToStringMultipleInputOperator.class;
        }
    }

    static class LifeCycleTrackingMockSource extends MockSource {
        public LifeCycleTrackingMockSource(Boundedness boundedness, int numSplits) {
            super(boundedness, numSplits);
        }

        @Override
        public SourceReader<Integer, MockSourceSplit> createReader(
                SourceReaderContext readerContext) {
            LifeCycleTrackingMockSourceReader sourceReader =
                    new LifeCycleTrackingMockSourceReader();
            createdReaders.add(sourceReader);
            return sourceReader;
        }
    }

    static class LifeCycleTrackingMockSourceReader extends MockSourceReader {
        public static final String START = "SourceReader#start";
        public static final String CLOSE = "SourceReader#close";

        @Override
        public void start() {
            LIFE_CYCLE_EVENTS.add(START);
            super.start();
        }

        @Override
        public void close() throws Exception {
            LIFE_CYCLE_EVENTS.add(CLOSE);
            super.close();
        }
    }

    static class LifeCycleTrackingMap<T> extends AbstractStreamOperator<T>
            implements OneInputStreamOperator<T, T>, BoundedOneInput {
        public static final String OPEN = "LifeCycleTrackingMap#open";
        public static final String CLOSE = "LifeCycleTrackingMap#close";
        public static final String END_INPUT = "LifeCycleTrackingMap#endInput";

        @Override
        public void processElement(StreamRecord<T> element) throws Exception {
            output.collect(element);
        }

        @Override
        public void open() throws Exception {
            LIFE_CYCLE_EVENTS.add(OPEN);
            super.open();
        }

        @Override
        public void close() throws Exception {
            LIFE_CYCLE_EVENTS.add(CLOSE);
            super.close();
        }

        @Override
        public void endInput() throws Exception {
            LIFE_CYCLE_EVENTS.add(END_INPUT);
        }
    }

    private static class RecordToWatermarkGenerator
            implements WatermarkGenerator<Integer>, Serializable {
        @Override
        public void onEvent(Integer event, long eventTimestamp, WatermarkOutput output) {
            output.emitWatermark(new org.apache.flink.api.common.eventtime.Watermark(event));
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {}
    }

    private static class CopyProxySerializer extends TypeSerializer<Integer> {

        SharedReference<List<Integer>> copiedElementsRef;

        public CopyProxySerializer(SharedReference<List<Integer>> copiedElementsRef) {
            this.copiedElementsRef = copiedElementsRef;
        }

        @Override
        public boolean isImmutableType() {
            return false; // to force copy
        }

        @Override
        public TypeSerializer<Integer> duplicate() {
            return new CopyProxySerializer(copiedElementsRef);
        }

        @Override
        public Integer createInstance() {
            return IntSerializer.INSTANCE.createInstance();
        }

        @Override
        public Integer copy(Integer from) {
            copiedElementsRef.applySync(list -> list.add(from));
            return IntSerializer.INSTANCE.copy(from);
        }

        @Override
        public Integer copy(Integer from, Integer reuse) {
            copiedElementsRef.applySync(list -> list.add(from));
            return IntSerializer.INSTANCE.copy(from, reuse);
        }

        @Override
        public int getLength() {
            return IntSerializer.INSTANCE.getLength();
        }

        @Override
        public void serialize(Integer record, DataOutputView target) throws IOException {
            IntSerializer.INSTANCE.serialize(record, target);
        }

        @Override
        public Integer deserialize(DataInputView source) throws IOException {
            return IntSerializer.INSTANCE.deserialize(source);
        }

        @Override
        public Integer deserialize(Integer reuse, DataInputView source) throws IOException {
            return IntSerializer.INSTANCE.deserialize(reuse, source);
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean equals(Object obj) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int hashCode() {
            throw new UnsupportedOperationException();
        }

        @Override
        public TypeSerializerSnapshot<Integer> snapshotConfiguration() {
            throw new UnsupportedOperationException();
        }
    }
}
