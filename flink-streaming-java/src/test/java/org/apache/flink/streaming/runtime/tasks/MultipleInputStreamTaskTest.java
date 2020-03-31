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
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Metric;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
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
import org.apache.flink.streaming.api.operators.AbstractInput;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.co.CoStreamMap;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.InputStatus;
import org.apache.flink.streaming.runtime.io.StreamMultipleInputProcessor;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTaskTest.WatermarkMetricOperator;
import org.apache.flink.streaming.util.TestBoundedMultipleInputOperator;
import org.apache.flink.streaming.util.TestHarnessUtil;

import org.hamcrest.collection.IsEmptyCollection;
import org.hamcrest.collection.IsMapContaining;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link MultipleInputStreamTask}. Theses tests implicitly also test the
 * {@link StreamMultipleInputProcessor}.
 */
public class MultipleInputStreamTaskTest {
	/**
	 * This test verifies that open() and close() are correctly called. This test also verifies
	 * that timestamps of emitted elements are correct. {@link CoStreamMap} assigns the input
	 * timestamp to emitted elements.
	 */
	@Test
	public void testOpenCloseAndTimestamps() throws Exception {
		try (StreamTaskMailboxTestHarness<String> testHarness =
			new MultipleInputStreamTaskTestHarnessBuilder<>(MultipleInputStreamTask::new, BasicTypeInfo.STRING_TYPE_INFO)
				.addInput(BasicTypeInfo.STRING_TYPE_INFO)
				.addInput(BasicTypeInfo.INT_TYPE_INFO)
				.addInput(BasicTypeInfo.DOUBLE_TYPE_INFO)
				.setupOutputForSingletonOperatorChain(new MapToStringMultipleInputOperatorFactory())
				.build()) {

			long initialTime = 0L;
			ArrayDeque<Object> expectedOutput = new ArrayDeque<>();

			testHarness.processElement(new StreamRecord<>("Hello", initialTime + 1), 0);
			expectedOutput.add(new StreamRecord<>("Hello", initialTime + 1));
			testHarness.processElement(new StreamRecord<>(1337, initialTime + 2), 1);
			expectedOutput.add(new StreamRecord<>("1337", initialTime + 2));
			testHarness.processElement(new StreamRecord<>(42.44d, initialTime + 3), 2);
			expectedOutput.add(new StreamRecord<>("42.44", initialTime + 3));

			testHarness.endInput();
			testHarness.waitForTaskCompletion();

			assertThat(testHarness.getOutput(), contains(expectedOutput.toArray()));
		}
	}

	/**
	 * This test verifies that checkpoint barriers are correctly forwarded.
	 */
	@Test
	public void testCheckpointBarriers() throws Exception {
		try (StreamTaskMailboxTestHarness<String> testHarness =
			new MultipleInputStreamTaskTestHarnessBuilder<>(MultipleInputStreamTask::new, BasicTypeInfo.STRING_TYPE_INFO)
				.addInput(BasicTypeInfo.STRING_TYPE_INFO, 2)
				.addInput(BasicTypeInfo.INT_TYPE_INFO, 2)
				.addInput(BasicTypeInfo.DOUBLE_TYPE_INFO, 2)
				.setupOutputForSingletonOperatorChain(new MapToStringMultipleInputOperatorFactory())
				.build()) {
			ArrayDeque<Object> expectedOutput = new ArrayDeque<>();
			long initialTime = 0L;

			testHarness.processEvent(new CheckpointBarrier(0, 0, CheckpointOptions.forCheckpointWithDefaultLocation()), 0, 0);

			// This element should be buffered since we received a checkpoint barrier on this input
			testHarness.processElement(new StreamRecord<>("Hello-0-0", initialTime), 0, 0);
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

			testHarness.processEvent(new CheckpointBarrier(0, 0, CheckpointOptions.forCheckpointWithDefaultLocation()), 0, 1);
			testHarness.processEvent(new CheckpointBarrier(0, 0, CheckpointOptions.forCheckpointWithDefaultLocation()), 1, 0);
			testHarness.processEvent(new CheckpointBarrier(0, 0, CheckpointOptions.forCheckpointWithDefaultLocation()), 1, 1);
			testHarness.processEvent(new CheckpointBarrier(0, 0, CheckpointOptions.forCheckpointWithDefaultLocation()), 2, 0);
			testHarness.processEvent(new CheckpointBarrier(0, 0, CheckpointOptions.forCheckpointWithDefaultLocation()), 2, 1);

			// now we should see the barrier and after that the buffered elements
			expectedOutput.add(new CheckpointBarrier(0, 0, CheckpointOptions.forCheckpointWithDefaultLocation()));
			expectedOutput.add(new StreamRecord<>("Hello-0-0", initialTime));

			assertThat(testHarness.getOutput(), contains(expectedOutput.toArray()));
		}
	}

	/**
	 * This test verifies that checkpoint barriers and barrier buffers work correctly with
	 * concurrent checkpoint barriers where one checkpoint is "overtaking" another checkpoint, i.e.
	 * some inputs receive barriers from an earlier checkpoint, thereby blocking,
	 * then all inputs receive barriers from a later checkpoint.
	 */
	@Test
	public void testOvertakingCheckpointBarriers() throws Exception {
		try (StreamTaskMailboxTestHarness<String> testHarness =
			new MultipleInputStreamTaskTestHarnessBuilder<>(MultipleInputStreamTask::new, BasicTypeInfo.STRING_TYPE_INFO)
				.addInput(BasicTypeInfo.STRING_TYPE_INFO, 2)
				.addInput(BasicTypeInfo.INT_TYPE_INFO, 2)
				.addInput(BasicTypeInfo.DOUBLE_TYPE_INFO, 2)
				.setupOutputForSingletonOperatorChain(new MapToStringMultipleInputOperatorFactory())
				.build()) {
			ArrayDeque<Object> expectedOutput = new ArrayDeque<>();
			long initialTime = 0L;

			testHarness.processEvent(new CheckpointBarrier(0, 0, CheckpointOptions.forCheckpointWithDefaultLocation()), 0, 0);

			// These elements should be buffered until we receive barriers from
			// all inputs
			testHarness.processElement(new StreamRecord<>("Hello-0-0", initialTime), 0, 0);
			testHarness.processElement(new StreamRecord<>("Ciao-0-0", initialTime), 0, 0);

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

			// Now give a later barrier to all inputs, this should unblock the first channel,
			// thereby allowing the two blocked elements through
			testHarness.processEvent(new CheckpointBarrier(1, 1, CheckpointOptions.forCheckpointWithDefaultLocation()), 0, 0);
			testHarness.processEvent(new CheckpointBarrier(1, 1, CheckpointOptions.forCheckpointWithDefaultLocation()), 0, 1);
			testHarness.processEvent(new CheckpointBarrier(1, 1, CheckpointOptions.forCheckpointWithDefaultLocation()), 1, 0);
			testHarness.processEvent(new CheckpointBarrier(1, 1, CheckpointOptions.forCheckpointWithDefaultLocation()), 1, 1);
			testHarness.processEvent(new CheckpointBarrier(1, 1, CheckpointOptions.forCheckpointWithDefaultLocation()), 2, 0);
			testHarness.processEvent(new CheckpointBarrier(1, 1, CheckpointOptions.forCheckpointWithDefaultLocation()), 2, 1);

			expectedOutput.add(new CancelCheckpointMarker(0));
			expectedOutput.add(new StreamRecord<>("Hello-0-0", initialTime));
			expectedOutput.add(new StreamRecord<>("Ciao-0-0", initialTime));
			expectedOutput.add(new CheckpointBarrier(1, 1, CheckpointOptions.forCheckpointWithDefaultLocation()));

			assertThat(testHarness.getOutput(), contains(expectedOutput.toArray()));

			// Then give the earlier barrier, these should be ignored
			testHarness.processEvent(new CheckpointBarrier(0, 0, CheckpointOptions.forCheckpointWithDefaultLocation()), 0, 1);
			testHarness.processEvent(new CheckpointBarrier(0, 0, CheckpointOptions.forCheckpointWithDefaultLocation()), 1, 0);
			testHarness.processEvent(new CheckpointBarrier(0, 0, CheckpointOptions.forCheckpointWithDefaultLocation()), 1, 1);
			testHarness.processEvent(new CheckpointBarrier(0, 0, CheckpointOptions.forCheckpointWithDefaultLocation()), 2, 0);
			testHarness.processEvent(new CheckpointBarrier(0, 0, CheckpointOptions.forCheckpointWithDefaultLocation()), 2, 1);

			testHarness.waitForTaskCompletion();
			assertThat(testHarness.getOutput(), contains(expectedOutput.toArray()));
		}
	}

	@Test
	public void testOperatorMetricReuse() throws Exception {

		TaskMetricGroup taskMetricGroup = new UnregisteredMetricGroups.UnregisteredTaskMetricGroup() {
			@Override
			public OperatorMetricGroup getOrAddOperator(OperatorID operatorID, String name) {
				return new OperatorMetricGroup(NoOpMetricRegistry.INSTANCE, this, operatorID, name);
			}
		};

		try (StreamTaskMailboxTestHarness<String> testHarness =
			new MultipleInputStreamTaskTestHarnessBuilder<>(MultipleInputStreamTask::new, BasicTypeInfo.STRING_TYPE_INFO)
				.addInput(BasicTypeInfo.STRING_TYPE_INFO)
				.addInput(BasicTypeInfo.STRING_TYPE_INFO)
				.addInput(BasicTypeInfo.STRING_TYPE_INFO)
				.setupOperatorChain(new DuplicatingOperatorFactory())
				.chain(new OneInputStreamTaskTest.DuplicatingOperator(), BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()))
				.chain(new OneInputStreamTaskTest.DuplicatingOperator(), BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()))
				.finish()
				.setTaskMetricGroup(taskMetricGroup)
				.build()) {
			Counter numRecordsInCounter = taskMetricGroup.getIOMetricGroup().getNumRecordsInCounter();
			Counter numRecordsOutCounter = taskMetricGroup.getIOMetricGroup().getNumRecordsOutCounter();

			int numRecords1 = 5;
			int numRecords2 = 3;
			int numRecords3 = 2;
			for (int x = 0; x < numRecords1; x++) {
				testHarness.processElement(new StreamRecord<>("hello"), 0, 0);
			}
			for (int x = 0; x < numRecords2; x++) {
				testHarness.processElement(new StreamRecord<>("hello"), 1, 0);
			}
			for (int x = 0; x < numRecords3; x++) {
				testHarness.processElement(new StreamRecord<>("hello"), 2, 0);
			}

			int totalRecords = numRecords1 + numRecords2 + numRecords3;
			assertEquals(totalRecords, numRecordsInCounter.getCount());
			assertEquals((totalRecords) * 2 * 2 * 2, numRecordsOutCounter.getCount());
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
	public void testClosingAllOperatorsOnChainProperly() throws Exception {
		StreamTaskMailboxTestHarness<String> testHarness =
			new MultipleInputStreamTaskTestHarnessBuilder<>(MultipleInputStreamTask::new, BasicTypeInfo.STRING_TYPE_INFO)
				.addInput(BasicTypeInfo.STRING_TYPE_INFO)
				.addInput(BasicTypeInfo.STRING_TYPE_INFO)
				.addInput(BasicTypeInfo.STRING_TYPE_INFO)
				.setupOperatorChain(new TestBoundedMultipleInputOperatorFactory())
				.chain(new TestBoundedOneInputStreamOperator("Operator1"), BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()))
				.finish()
				.build();

		ArrayDeque<Object> expectedOutput = new ArrayDeque<>();
		try {
			testHarness.processElement(new StreamRecord<>("Hello-1"), 0);
			testHarness.endInput(0);
			testHarness.process();

			testHarness.processElement(new StreamRecord<>("Hello-2"), 1);
			testHarness.processElement(new StreamRecord<>("Hello-3"), 2);
			testHarness.endInput(1);
			testHarness.process();
			testHarness.endInput(2);
			testHarness.process();
			assertEquals(
				true,
				testHarness.getStreamTask().getInputOutputJointFuture(InputStatus.NOTHING_AVAILABLE).isDone());

			testHarness.waitForTaskCompletion();
		}
		finally {
			testHarness.close();
		}

		expectedOutput.add(new StreamRecord<>("[Operator0-1]: Hello-1"));
		expectedOutput.add(new StreamRecord<>("[Operator0-1]: End of input"));
		expectedOutput.add(new StreamRecord<>("[Operator0-2]: Hello-2"));
		expectedOutput.add(new StreamRecord<>("[Operator0-3]: Hello-3"));
		expectedOutput.add(new StreamRecord<>("[Operator0-2]: End of input"));
		expectedOutput.add(new StreamRecord<>("[Operator0-3]: End of input"));
		expectedOutput.add(new StreamRecord<>("[Operator0]: Bye"));
		expectedOutput.add(new StreamRecord<>("[Operator1]: End of input"));
		expectedOutput.add(new StreamRecord<>("[Operator1]: Bye"));

		assertThat(testHarness.getOutput(), contains(expectedOutput.toArray()));
	}

	@Test
	public void testInputFairness() throws Exception {
		try (StreamTaskMailboxTestHarness<String> testHarness =
				new MultipleInputStreamTaskTestHarnessBuilder<>(MultipleInputStreamTask::new, BasicTypeInfo.STRING_TYPE_INFO)
					.addInput(BasicTypeInfo.STRING_TYPE_INFO)
					.addInput(BasicTypeInfo.STRING_TYPE_INFO)
					.addInput(BasicTypeInfo.STRING_TYPE_INFO)
					.setupOutputForSingletonOperatorChain(new MapToStringMultipleInputOperatorFactory())
					.build()) {
			ArrayDeque<Object> expectedOutput = new ArrayDeque<>();

			testHarness.setAutoProcess(false);
			testHarness.processElement(new StreamRecord<>("0"), 0);
			testHarness.processElement(new StreamRecord<>("1"), 0);
			testHarness.processElement(new StreamRecord<>("2"), 0);
			testHarness.processElement(new StreamRecord<>("3"), 0);

			testHarness.processElement(new StreamRecord<>("0"), 2);
			testHarness.processElement(new StreamRecord<>("1"), 2);

			testHarness.process();

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
				new MultipleInputStreamTaskTestHarnessBuilder<>(MultipleInputStreamTask::new, BasicTypeInfo.STRING_TYPE_INFO)
					.addInput(BasicTypeInfo.STRING_TYPE_INFO, 2)
					.addInput(BasicTypeInfo.INT_TYPE_INFO, 2)
					.addInput(BasicTypeInfo.DOUBLE_TYPE_INFO, 2)
					.setupOutputForSingletonOperatorChain(new MapToStringMultipleInputOperatorFactory())
					.build()) {
			ArrayDeque<Object> expectedOutput = new ArrayDeque<>();

			long initialTime = 0L;

			testHarness.processElement(new Watermark(initialTime), 0, 0);
			testHarness.processElement(new Watermark(initialTime), 0, 1);
			testHarness.processElement(new Watermark(initialTime), 1, 0);
			testHarness.processElement(new Watermark(initialTime), 1, 1);

			testHarness.processElement(new Watermark(initialTime), 2, 0);

			assertThat(testHarness.getOutput(), IsEmptyCollection.empty());

			testHarness.processElement(new Watermark(initialTime), 2, 1);

			// now the watermark should have propagated, Map simply forward Watermarks
			expectedOutput.add(new Watermark(initialTime));
			assertThat(testHarness.getOutput(), contains(expectedOutput.toArray()));

			// contrary to checkpoint barriers these elements are not blocked by watermarks
			testHarness.processElement(new StreamRecord<>("Hello", initialTime), 0, 0);
			testHarness.processElement(new StreamRecord<>(42, initialTime), 1, 1);
			expectedOutput.add(new StreamRecord<>("Hello", initialTime));
			expectedOutput.add(new StreamRecord<>("42", initialTime));

			assertThat(testHarness.getOutput(), contains(expectedOutput.toArray()));

			testHarness.processElement(new Watermark(initialTime + 4), 0, 0);
			testHarness.processElement(new Watermark(initialTime + 3), 0, 1);
			testHarness.processElement(new Watermark(initialTime + 3), 1, 0);
			testHarness.processElement(new Watermark(initialTime + 4), 1, 1);
			testHarness.processElement(new Watermark(initialTime + 3), 2, 0);
			testHarness.processElement(new Watermark(initialTime + 2), 2, 1);

			// check whether we get the minimum of all the watermarks, this must also only occur in
			// the output after the two StreamRecords
			expectedOutput.add(new Watermark(initialTime + 2));
			assertThat(testHarness.getOutput(), contains(expectedOutput.toArray()));

			// advance watermark from one of the inputs, now we should get a new one since the
			// minimum increases
			testHarness.processElement(new Watermark(initialTime + 4), 2, 1);
			expectedOutput.add(new Watermark(initialTime + 3));
			assertThat(testHarness.getOutput(), contains(expectedOutput.toArray()));

			// advance the other two inputs, now we should get a new one since the
			// minimum increases again
			testHarness.processElement(new Watermark(initialTime + 4), 0, 1);
			testHarness.processElement(new Watermark(initialTime + 4), 1, 0);
			testHarness.processElement(new Watermark(initialTime + 4), 2, 0);
			expectedOutput.add(new Watermark(initialTime + 4));
			assertThat(testHarness.getOutput(), contains(expectedOutput.toArray()));

			List<String> resultElements = TestHarnessUtil.getRawElementsFromOutput(testHarness.getOutput());
			assertEquals(2, resultElements.size());
		}
	}

	/**
	 * This test verifies that watermarks and stream statuses are correctly forwarded. This also checks whether
	 * watermarks are forwarded only when we have received watermarks from all inputs. The
	 * forwarded watermark must be the minimum of the watermarks of all active inputs.
	 */
	@Test
	public void testWatermarkAndStreamStatusForwarding() throws Exception {
		try (StreamTaskMailboxTestHarness<String> testHarness =
				new MultipleInputStreamTaskTestHarnessBuilder<>(MultipleInputStreamTask::new, BasicTypeInfo.STRING_TYPE_INFO)
					.addInput(BasicTypeInfo.STRING_TYPE_INFO, 2)
					.addInput(BasicTypeInfo.INT_TYPE_INFO, 2)
					.addInput(BasicTypeInfo.DOUBLE_TYPE_INFO, 2)
					.setupOutputForSingletonOperatorChain(new MapToStringMultipleInputOperatorFactory())
					.build()) {
			ArrayDeque<Object> expectedOutput = new ArrayDeque<>();

			long initialTime = 0L;

			// test whether idle input channels are acknowledged correctly when forwarding watermarks
			testHarness.processElement(StreamStatus.IDLE, 0, 1);
			testHarness.processElement(StreamStatus.IDLE, 1, 1);
			testHarness.processElement(StreamStatus.IDLE, 2, 0);
			testHarness.processElement(new Watermark(initialTime + 6), 0, 0);
			testHarness.processElement(new Watermark(initialTime + 6), 1, 0);
			testHarness.processElement(new Watermark(initialTime + 5), 2, 1); // this watermark should be advanced first
			testHarness.processElement(StreamStatus.IDLE, 2, 1); // once this is acknowledged,

			expectedOutput.add(new Watermark(initialTime + 5));
			// We don't expect to see Watermark(6) here because the idle status of one
			// input doesn't propagate to the other input. That is, if input 1 is at WM 6 and input
			// two was at WM 5 before going to IDLE then the output watermark will not jump to WM 6.
			assertThat(testHarness.getOutput(), contains(expectedOutput.toArray()));

			// make all input channels idle and check that the operator's idle status is forwarded
			testHarness.processElement(StreamStatus.IDLE, 0, 0);
			testHarness.processElement(StreamStatus.IDLE, 1, 0);
			expectedOutput.add(StreamStatus.IDLE);
			assertThat(testHarness.getOutput(), contains(expectedOutput.toArray()));

			// make some input channels active again and check that the operator's active status is forwarded only once
			testHarness.processElement(StreamStatus.ACTIVE, 1, 0);
			testHarness.processElement(StreamStatus.ACTIVE, 0, 1);
			expectedOutput.add(StreamStatus.ACTIVE);
			assertThat(testHarness.getOutput(), contains(expectedOutput.toArray()));
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testWatermarkMetrics() throws Exception {
		OperatorID headOperatorId = new OperatorID();
		OperatorID chainedOperatorId = new OperatorID();

		InterceptingOperatorMetricGroup headOperatorMetricGroup = new InterceptingOperatorMetricGroup();
		InterceptingOperatorMetricGroup chainedOperatorMetricGroup = new InterceptingOperatorMetricGroup();
		InterceptingTaskMetricGroup taskMetricGroup = new InterceptingTaskMetricGroup() {
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

		try (StreamTaskMailboxTestHarness<String> testHarness =
				new MultipleInputStreamTaskTestHarnessBuilder<>(MultipleInputStreamTask::new, BasicTypeInfo.STRING_TYPE_INFO)
					.addInput(BasicTypeInfo.STRING_TYPE_INFO)
					.addInput(BasicTypeInfo.INT_TYPE_INFO)
					.addInput(BasicTypeInfo.DOUBLE_TYPE_INFO)
					.setupOperatorChain(headOperatorId, new MapToStringMultipleInputOperatorFactory())
					.chain(
						chainedOperatorId,
						new WatermarkMetricOperator(),
						BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()))
					.finish()
					.setTaskMetricGroup(taskMetricGroup)
					.build()) {
			Gauge<Long> taskInputWatermarkGauge = (Gauge<Long>) taskMetricGroup.get(MetricNames.IO_CURRENT_INPUT_WATERMARK);
			Gauge<Long> headInput1WatermarkGauge = (Gauge<Long>) headOperatorMetricGroup.get(MetricNames.currentInputWatermarkName(1));
			Gauge<Long> headInput2WatermarkGauge = (Gauge<Long>) headOperatorMetricGroup.get(MetricNames.currentInputWatermarkName(2));
			Gauge<Long> headInput3WatermarkGauge = (Gauge<Long>) headOperatorMetricGroup.get(MetricNames.currentInputWatermarkName(3));
			Gauge<Long> headInputWatermarkGauge = (Gauge<Long>) headOperatorMetricGroup.get(MetricNames.IO_CURRENT_INPUT_WATERMARK);
			Gauge<Long> headOutputWatermarkGauge = (Gauge<Long>) headOperatorMetricGroup.get(MetricNames.IO_CURRENT_OUTPUT_WATERMARK);
			Gauge<Long> chainedInputWatermarkGauge = (Gauge<Long>) chainedOperatorMetricGroup.get(MetricNames.IO_CURRENT_INPUT_WATERMARK);
			Gauge<Long> chainedOutputWatermarkGauge = (Gauge<Long>) chainedOperatorMetricGroup.get(MetricNames.IO_CURRENT_OUTPUT_WATERMARK);

			assertEquals(Long.MIN_VALUE, taskInputWatermarkGauge.getValue().longValue());
			assertEquals(Long.MIN_VALUE, headInputWatermarkGauge.getValue().longValue());
			assertEquals(Long.MIN_VALUE, headInput1WatermarkGauge.getValue().longValue());
			assertEquals(Long.MIN_VALUE, headInput2WatermarkGauge.getValue().longValue());
			assertEquals(Long.MIN_VALUE, headInput3WatermarkGauge.getValue().longValue());
			assertEquals(Long.MIN_VALUE, headOutputWatermarkGauge.getValue().longValue());
			assertEquals(Long.MIN_VALUE, chainedInputWatermarkGauge.getValue().longValue());
			assertEquals(Long.MIN_VALUE, chainedOutputWatermarkGauge.getValue().longValue());

			testHarness.processElement(new Watermark(1L), 0);
			assertEquals(Long.MIN_VALUE, taskInputWatermarkGauge.getValue().longValue());
			assertEquals(Long.MIN_VALUE, headInputWatermarkGauge.getValue().longValue());
			assertEquals(1L, headInput1WatermarkGauge.getValue().longValue());
			assertEquals(Long.MIN_VALUE, headInput2WatermarkGauge.getValue().longValue());
			assertEquals(Long.MIN_VALUE, headInput3WatermarkGauge.getValue().longValue());
			assertEquals(Long.MIN_VALUE, headOutputWatermarkGauge.getValue().longValue());
			assertEquals(Long.MIN_VALUE, chainedInputWatermarkGauge.getValue().longValue());
			assertEquals(Long.MIN_VALUE, chainedOutputWatermarkGauge.getValue().longValue());

			testHarness.processElement(new Watermark(2L), 1);
			assertEquals(Long.MIN_VALUE, taskInputWatermarkGauge.getValue().longValue());
			assertEquals(Long.MIN_VALUE, headInputWatermarkGauge.getValue().longValue());
			assertEquals(1L, headInput1WatermarkGauge.getValue().longValue());
			assertEquals(2L, headInput2WatermarkGauge.getValue().longValue());
			assertEquals(Long.MIN_VALUE, headInput3WatermarkGauge.getValue().longValue());
			assertEquals(Long.MIN_VALUE, headOutputWatermarkGauge.getValue().longValue());
			assertEquals(Long.MIN_VALUE, chainedInputWatermarkGauge.getValue().longValue());
			assertEquals(Long.MIN_VALUE, chainedOutputWatermarkGauge.getValue().longValue());

			testHarness.processElement(new Watermark(2L), 2);
			assertEquals(1L, taskInputWatermarkGauge.getValue().longValue());
			assertEquals(1L, headInputWatermarkGauge.getValue().longValue());
			assertEquals(1L, headInput1WatermarkGauge.getValue().longValue());
			assertEquals(2L, headInput2WatermarkGauge.getValue().longValue());
			assertEquals(2L, headInput3WatermarkGauge.getValue().longValue());
			assertEquals(1L, headOutputWatermarkGauge.getValue().longValue());
			assertEquals(1L, chainedInputWatermarkGauge.getValue().longValue());
			assertEquals(2L, chainedOutputWatermarkGauge.getValue().longValue());

			testHarness.processElement(new Watermark(4L), 0);
			testHarness.processElement(new Watermark(3L), 1);
			assertEquals(2L, taskInputWatermarkGauge.getValue().longValue());
			assertEquals(2L, headInputWatermarkGauge.getValue().longValue());
			assertEquals(4L, headInput1WatermarkGauge.getValue().longValue());
			assertEquals(3L, headInput2WatermarkGauge.getValue().longValue());
			assertEquals(2L, headInput3WatermarkGauge.getValue().longValue());
			assertEquals(2L, headOutputWatermarkGauge.getValue().longValue());
			assertEquals(2L, chainedInputWatermarkGauge.getValue().longValue());
			assertEquals(4L, chainedOutputWatermarkGauge.getValue().longValue());

			testHarness.endInput();
			testHarness.waitForTaskCompletion();
		}
	}

	/**
	 * Tests the checkpoint related metrics are registered into {@link TaskIOMetricGroup}
	 * correctly while generating the {@link TwoInputStreamTask}.
	 */
	@Test
	public void testCheckpointBarrierMetrics() throws Exception {
		final Map<String, Metric> metrics = new ConcurrentHashMap<>();
		final TaskMetricGroup taskMetricGroup = new StreamTaskTestHarness.TestTaskMetricGroup(metrics);

		try (StreamTaskMailboxTestHarness<String> testHarness =
				new MultipleInputStreamTaskTestHarnessBuilder<>(MultipleInputStreamTask::new, BasicTypeInfo.STRING_TYPE_INFO)
					.addInput(BasicTypeInfo.STRING_TYPE_INFO, 2)
					.addInput(BasicTypeInfo.INT_TYPE_INFO, 2)
					.addInput(BasicTypeInfo.DOUBLE_TYPE_INFO, 2)
					.setupOutputForSingletonOperatorChain(new MapToStringMultipleInputOperatorFactory())
					.setTaskMetricGroup(taskMetricGroup)
					.build()) {

			assertThat(metrics, IsMapContaining.hasKey(MetricNames.CHECKPOINT_ALIGNMENT_TIME));
			assertThat(metrics, IsMapContaining.hasKey(MetricNames.CHECKPOINT_START_DELAY_TIME));

			testHarness.endInput();
			testHarness.waitForTaskCompletion();
		}
	}

	@Test
	public void testLatencyMarker() throws Exception {
		final Map<String, Metric> metrics = new ConcurrentHashMap<>();
		final TaskMetricGroup taskMetricGroup = new StreamTaskTestHarness.TestTaskMetricGroup(metrics);

		try (StreamTaskMailboxTestHarness<String> testHarness =
				new MultipleInputStreamTaskTestHarnessBuilder<>(MultipleInputStreamTask::new, BasicTypeInfo.STRING_TYPE_INFO)
					.addInput(BasicTypeInfo.STRING_TYPE_INFO)
					.addInput(BasicTypeInfo.INT_TYPE_INFO)
					.addInput(BasicTypeInfo.DOUBLE_TYPE_INFO)
					.setupOutputForSingletonOperatorChain(new MapToStringMultipleInputOperatorFactory())
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

	private static class MapToStringMultipleInputOperator
			extends AbstractStreamOperatorV2<String> implements MultipleInputStreamOperator<String> {
		private static final long serialVersionUID = 1L;

		private boolean openCalled;
		private boolean closeCalled;

		public MapToStringMultipleInputOperator(StreamOperatorParameters<String> parameters) {
			super(parameters, 3);
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
		public void close() throws Exception {
			super.close();
			if (!openCalled) {
				Assert.fail("Open was not called before close.");
			}
			closeCalled = true;
		}

		@Override
		public List<Input> getInputs() {
			return Arrays.asList(
				new MapToStringInput<String>(this, 1),
				new MapToStringInput<Integer>(this, 2),
				new MapToStringInput<Double>(this, 3));
		}

		public boolean wasCloseCalled() {
			return closeCalled;
		}

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
					output.collect(new StreamRecord<>(element.getValue().toString(), element.getTimestamp()));
				}
				else {
					output.collect(new StreamRecord<>(element.getValue().toString()));
				}
			}
		}
	}

	private static class TestBoundedMultipleInputOperatorFactory extends AbstractStreamOperatorFactory<String> {
		@Override
		public <T extends StreamOperator<String>> T createStreamOperator(StreamOperatorParameters<String> parameters) {
			return (T) new TestBoundedMultipleInputOperator("Operator0", parameters);
		}

		@Override
		public Class<? extends StreamOperator<String>> getStreamOperatorClass(ClassLoader classLoader) {
			return TestBoundedMultipleInputOperator.class;
		}
	}

	private static class DuplicatingOperatorFactory extends AbstractStreamOperatorFactory<String> {
		@Override
		public <T extends StreamOperator<String>> T createStreamOperator(StreamOperatorParameters<String> parameters) {
			return (T) new DuplicatingOperator(parameters);
		}

		@Override
		public Class<? extends StreamOperator<String>> getStreamOperatorClass(ClassLoader classLoader) {
			return DuplicatingOperator.class;
		}
	}

	private static class MapToStringMultipleInputOperatorFactory extends AbstractStreamOperatorFactory<String> {
		@Override
		public <T extends StreamOperator<String>> T createStreamOperator(StreamOperatorParameters<String> parameters) {
			return (T) new MapToStringMultipleInputOperator(parameters);
		}

		@Override
		public Class<? extends StreamOperator<String>> getStreamOperatorClass(ClassLoader classLoader) {
			return MapToStringMultipleInputOperator.class;
		}
	}
}

