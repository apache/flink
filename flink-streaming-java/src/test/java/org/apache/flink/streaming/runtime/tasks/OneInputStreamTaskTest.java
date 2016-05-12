/**
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


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.PartitionedState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.state.KvState;
import org.apache.flink.runtime.state.KvStateSnapshot;
import org.apache.flink.runtime.state.LocalStateHandle;
import org.apache.flink.runtime.state.PartitionedStateBackend;
import org.apache.flink.runtime.state.PartitionedStateSnapshot;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.util.TestLogger;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Tests for {@link OneInputStreamTask}.
 *
 * <p>
 * Note:<br>
 * We only use a {@link StreamMap} operator here. We also test the individual operators but Map is
 * used as a representative to test OneInputStreamTask, since OneInputStreamTask is used for all
 * OneInputStreamOperators.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ResultPartitionWriter.class})
public class OneInputStreamTaskTest extends TestLogger {

	/**
	 * This test verifies that open() and close() are correctly called. This test also verifies
	 * that timestamps of emitted elements are correct. {@link StreamMap} assigns the input
	 * timestamp to emitted elements.
	 */
	@Test
	public void testOpenCloseAndTimestamps() throws Exception {
		final OneInputStreamTask<String, String> mapTask = new OneInputStreamTask<String, String>();
		final OneInputStreamTaskTestHarness<String, String> testHarness = new OneInputStreamTaskTestHarness<String, String>(mapTask, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);

		StreamConfig streamConfig = testHarness.getStreamConfig();
		StreamMap<String, String> mapOperator = new StreamMap<String, String>(new TestOpenCloseMapFunction());
		streamConfig.setStreamOperator(mapOperator);

		long initialTime = 0L;
		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<Object>();

		testHarness.invoke();
		testHarness.waitForTaskRunning();

		testHarness.processElement(new StreamRecord<String>("Hello", initialTime + 1));
		testHarness.processElement(new StreamRecord<String>("Ciao", initialTime + 2));
		expectedOutput.add(new StreamRecord<String>("Hello", initialTime + 1));
		expectedOutput.add(new StreamRecord<String>("Ciao", initialTime + 2));

		testHarness.endInput();

		testHarness.waitForTaskCompletion();

		Assert.assertTrue("RichFunction methods where not called.", TestOpenCloseMapFunction.closeCalled);

		TestHarnessUtil.assertOutputEquals("Output was not correct.",
				expectedOutput,
				testHarness.getOutput());
	}

	/**
	 * This test verifies that watermarks are correctly forwarded. This also checks whether
	 * watermarks are forwarded only when we have received watermarks from all inputs. The
	 * forwarded watermark must be the minimum of the watermarks of all inputs.
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void testWatermarkForwarding() throws Exception {
		final OneInputStreamTask<String, String> mapTask = new OneInputStreamTask<String, String>();
		final OneInputStreamTaskTestHarness<String, String> testHarness = new OneInputStreamTaskTestHarness<String, String>(mapTask, 2, 2, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);

		StreamConfig streamConfig = testHarness.getStreamConfig();
		StreamMap<String, String> mapOperator = new StreamMap<String, String>(new IdentityMap());
		streamConfig.setStreamOperator(mapOperator);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<Object>();
		long initialTime = 0L;

		testHarness.invoke();
		testHarness.waitForTaskRunning();

		testHarness.processElement(new Watermark(initialTime), 0, 0);
		testHarness.processElement(new Watermark(initialTime), 0, 1);
		testHarness.processElement(new Watermark(initialTime), 1, 0);

		// now the output should still be empty
		testHarness.waitForInputProcessing();
		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.processElement(new Watermark(initialTime), 1, 1);

		// now the watermark should have propagated, Map simply forward Watermarks
		testHarness.waitForInputProcessing();
		expectedOutput.add(new Watermark(initialTime));
		TestHarnessUtil.assertOutputEquals("Output was not correct.",
				expectedOutput,
				testHarness.getOutput());

		// contrary to checkpoint barriers these elements are not blocked by watermarks
		testHarness.processElement(new StreamRecord<String>("Hello", initialTime));
		testHarness.processElement(new StreamRecord<String>("Ciao", initialTime));
		expectedOutput.add(new StreamRecord<String>("Hello", initialTime));
		expectedOutput.add(new StreamRecord<String>("Ciao", initialTime));

		testHarness.processElement(new Watermark(initialTime + 4), 0, 0);
		testHarness.processElement(new Watermark(initialTime + 3), 0, 1);
		testHarness.processElement(new Watermark(initialTime + 3), 1, 0);
		testHarness.processElement(new Watermark(initialTime + 2), 1, 1);

		// check whether we get the minimum of all the watermarks, this must also only occur in
		// the output after the two StreamRecords
		testHarness.waitForInputProcessing();
		expectedOutput.add(new Watermark(initialTime + 2));
		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());


		// advance watermark from one of the inputs, now we should get a now one since the
		// minimum increases
		testHarness.processElement(new Watermark(initialTime + 4), 1, 1);
		testHarness.waitForInputProcessing();
		expectedOutput.add(new Watermark(initialTime + 3));
		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

		// advance the other two inputs, now we should get a new one since the
		// minimum increases again
		testHarness.processElement(new Watermark(initialTime + 4), 0, 1);
		testHarness.processElement(new Watermark(initialTime + 4), 1, 0);
		testHarness.waitForInputProcessing();
		expectedOutput.add(new Watermark(initialTime + 4));
		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.endInput();

		testHarness.waitForTaskCompletion();

		List<String> resultElements = TestHarnessUtil.getRawElementsFromOutput(testHarness.getOutput());
		Assert.assertEquals(2, resultElements.size());
	}

	/**
	 * This test verifies that checkpoint barriers are correctly forwarded.
	 */
	@Test
	public void testCheckpointBarriers() throws Exception {
		final OneInputStreamTask<String, String> mapTask = new OneInputStreamTask<String, String>();
		final OneInputStreamTaskTestHarness<String, String> testHarness = new OneInputStreamTaskTestHarness<String, String>(mapTask, 2, 2, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);

		StreamConfig streamConfig = testHarness.getStreamConfig();
		StreamMap<String, String> mapOperator = new StreamMap<String, String>(new IdentityMap());
		streamConfig.setStreamOperator(mapOperator);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<Object>();
		long initialTime = 0L;

		testHarness.invoke();
		testHarness.waitForTaskRunning();

		testHarness.processEvent(new CheckpointBarrier(0, 0), 0, 0);

		// These elements should be buffered until we receive barriers from
		// all inputs
		testHarness.processElement(new StreamRecord<String>("Hello-0-0", initialTime), 0, 0);
		testHarness.processElement(new StreamRecord<String>("Ciao-0-0", initialTime), 0, 0);

		// These elements should be forwarded, since we did not yet receive a checkpoint barrier
		// on that input, only add to same input, otherwise we would not know the ordering
		// of the output since the Task might read the inputs in any order
		testHarness.processElement(new StreamRecord<String>("Hello-1-1", initialTime), 1, 1);
		testHarness.processElement(new StreamRecord<String>("Ciao-1-1", initialTime), 1, 1);
		expectedOutput.add(new StreamRecord<String>("Hello-1-1", initialTime));
		expectedOutput.add(new StreamRecord<String>("Ciao-1-1", initialTime));

		testHarness.waitForInputProcessing();
		// we should not yet see the barrier, only the two elements from non-blocked input
		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.processEvent(new CheckpointBarrier(0, 0), 0, 1);
		testHarness.processEvent(new CheckpointBarrier(0, 0), 1, 0);
		testHarness.processEvent(new CheckpointBarrier(0, 0), 1, 1);

		testHarness.waitForInputProcessing();

		// now we should see the barrier and after that the buffered elements
		expectedOutput.add(new CheckpointBarrier(0, 0));
		expectedOutput.add(new StreamRecord<String>("Hello-0-0", initialTime));
		expectedOutput.add(new StreamRecord<String>("Ciao-0-0", initialTime));

		testHarness.endInput();

		testHarness.waitForTaskCompletion();

		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());
	}

	/**
	 * This test verifies that checkpoint barriers and barrier buffers work correctly with
	 * concurrent checkpoint barriers where one checkpoint is "overtaking" another checkpoint, i.e.
	 * some inputs receive barriers from an earlier checkpoint, thereby blocking,
	 * then all inputs receive barriers from a later checkpoint.
	 */
	@Test
	public void testOvertakingCheckpointBarriers() throws Exception {
		final OneInputStreamTask<String, String> mapTask = new OneInputStreamTask<String, String>();
		final OneInputStreamTaskTestHarness<String, String> testHarness = new OneInputStreamTaskTestHarness<String, String>(mapTask, 2, 2, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);

		StreamConfig streamConfig = testHarness.getStreamConfig();
		StreamMap<String, String> mapOperator = new StreamMap<String, String>(new IdentityMap());
		streamConfig.setStreamOperator(mapOperator);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<Object>();
		long initialTime = 0L;

		testHarness.invoke();
		testHarness.waitForTaskRunning();

		testHarness.processEvent(new CheckpointBarrier(0, 0), 0, 0);

		// These elements should be buffered until we receive barriers from
		// all inputs
		testHarness.processElement(new StreamRecord<String>("Hello-0-0", initialTime), 0, 0);
		testHarness.processElement(new StreamRecord<String>("Ciao-0-0", initialTime), 0, 0);

		// These elements should be forwarded, since we did not yet receive a checkpoint barrier
		// on that input, only add to same input, otherwise we would not know the ordering
		// of the output since the Task might read the inputs in any order
		testHarness.processElement(new StreamRecord<String>("Hello-1-1", initialTime), 1, 1);
		testHarness.processElement(new StreamRecord<String>("Ciao-1-1", initialTime), 1, 1);
		expectedOutput.add(new StreamRecord<String>("Hello-1-1", initialTime));
		expectedOutput.add(new StreamRecord<String>("Ciao-1-1", initialTime));

		testHarness.waitForInputProcessing();
		// we should not yet see the barrier, only the two elements from non-blocked input
		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

		// Now give a later barrier to all inputs, this should unblock the first channel,
		// thereby allowing the two blocked elements through
		testHarness.processEvent(new CheckpointBarrier(1, 1), 0, 0);
		testHarness.processEvent(new CheckpointBarrier(1, 1), 0, 1);
		testHarness.processEvent(new CheckpointBarrier(1, 1), 1, 0);
		testHarness.processEvent(new CheckpointBarrier(1, 1), 1, 1);

		expectedOutput.add(new StreamRecord<String>("Hello-0-0", initialTime));
		expectedOutput.add(new StreamRecord<String>("Ciao-0-0", initialTime));
		expectedOutput.add(new CheckpointBarrier(1, 1));

		testHarness.waitForInputProcessing();

		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());


		// Then give the earlier barrier, these should be ignored
		testHarness.processEvent(new CheckpointBarrier(0, 0), 0, 1);
		testHarness.processEvent(new CheckpointBarrier(0, 0), 1, 0);
		testHarness.processEvent(new CheckpointBarrier(0, 0), 1, 1);

		testHarness.waitForInputProcessing();

		testHarness.endInput();

		testHarness.waitForTaskCompletion();

		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());
	}

	/**
	 * Tests that the stream operator can snapshot and restore the operator state
	 */
	@Test
	public void testSnapshottingAndRestoring() throws Exception {
		final OneInputStreamTask<String, String> mapTask = new OneInputStreamTask<String, String>();
		final OneInputStreamTaskTestHarness<String, String> testHarness = new OneInputStreamTaskTestHarness<String, String>(mapTask, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);

		long checkpointId = 1L;
		long checkpointTimestamp = 1L;

		testHarness.configureForKeyedStream(new KeySelector<String, String>() {
			private static final long serialVersionUID = -5783601122991736160L;

			@Override
			public String getKey(String value) throws Exception {
				return value;
			}
		}, BasicTypeInfo.STRING_TYPE_INFO);

		StreamConfig streamConfig = testHarness.getStreamConfig();
		TestingStreamOperator<Integer, Integer> headOperator = new TestingStreamOperator<>();
		TestingStreamOperator<Integer, Integer> chainedOperator = new TestingStreamOperator<>();

		streamConfig.setStreamOperator(headOperator);

		// add a chained operator to the stream config
		Map<Integer, StreamConfig> chainedTaskConfigs = new HashMap<>();

		StreamConfig chainedConfig = new StreamConfig(new Configuration());
		chainedConfig.setStreamOperator(chainedOperator);
		chainedTaskConfigs.put(1, chainedConfig);

		StreamEdge outputEdge = new StreamEdge(
			new StreamNode(
				null,
				0,
				null,
				null,
				null,
				null,
				null
			),
			new StreamNode(
				null,
				1,
				null,
				null,
				null,
				null,
				null
			),
			0,
			Collections.<String>emptyList(),
			null
		);

		streamConfig.setChainedOutputs(Arrays.asList(outputEdge));
		streamConfig.setTransitiveChainedTaskConfigs(chainedTaskConfigs);

		testHarness.invoke();
		testHarness.waitForTaskRunning();

		mapTask.triggerCheckpoint(checkpointId, checkpointTimestamp);
	}

	private static class TestingStreamOperator<IN, OUT> implements StreamOperator<OUT>, OneInputStreamOperator<IN, OUT> {

		private static final long serialVersionUID = 774614855940397174L;

		private final long seed;

		private transient Random random;

		public TestingStreamOperator(long seed) {
			this.seed = seed;
		}

		@Override
		public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<OUT>> output) {

		}

		@Override
		public void open() throws Exception {
			random = new Random(seed);
		}

		@Override
		public void close() throws Exception {

		}

		@Override
		public void dispose() {

		}

		@Override
		public StreamOperatorState snapshotOperatorState(long checkpointId, long timestamp) throws Exception {
			StateHandle<Serializable> functionState = generateFunctionState();
			StateHandle<Integer> operatorState = generateOperatorState();

			StreamOperatorNonPartitionedState streamOperatorNonPartitionedState = new StreamOperatorNonPartitionedState();
			streamOperatorNonPartitionedState.setFunctionState(functionState);
			streamOperatorNonPartitionedState.setOperatorState(operatorState);

			Map<Integer, PartitionedStateSnapshot> partitionedStateSnapshots = generatePartitionedStateSnapshots();

			StreamOperatorPartitionedState streamOperatorPartitionedState = new StreamOperatorPartitionedState(partitionedStateSnapshots);

			return new StreamOperatorState(streamOperatorPartitionedState, streamOperatorNonPartitionedState);
		}

		private StateHandle<Serializable> generateFunctionState() {
			return new LocalStateHandle<Serializable>(random.nextInt());
		}

		private StateHandle<Integer> generateOperatorState() {
			return new LocalStateHandle<>(random.nextInt());
		}

		private Map<Integer, PartitionedStateSnapshot> generatePartitionedStateSnapshots() {
			int numberPartitionedStateSnapshots = random.nextInt() % 100 + 10;
			HashMap<Integer, PartitionedStateSnapshot> result = new HashMap<>(numberPartitionedStateSnapshots);

			for (int i = 0; i < numberPartitionedStateSnapshots; i++) {
				result.put(i, createPartitionedStateSnapshot());
			}
		}

		private PartitionedStateSnapshot createPartitionedStateSnapshot() {
			PartitionedStateSnapshot partitionedStateSnapshot = new PartitionedStateSnapshot();

			partitionedStateSnapshot.put("value", new TestingKvStateSnaphshot());

			return partitionedStateSnapshot;
		}

		@Override
		public void restoreState(StreamOperatorState state, long recoveryTimestamp) throws Exception {

		}

		@Override
		public void notifyOfCompletedCheckpoint(long checkpointId) throws Exception {

		}

		@Override
		public void setKeyContextElement1(StreamRecord<?> record) throws Exception {

		}

		@Override
		public void setKeyContextElement2(StreamRecord<?> record) throws Exception {

		}

		@Override
		public boolean isInputCopyingDisabled() {
			return false;
		}

		@Override
		public ChainingStrategy getChainingStrategy() {
			return null;
		}

		@Override
		public void setChainingStrategy(ChainingStrategy strategy) {

		}

		@Override
		public void processElement(StreamRecord<IN> element) throws Exception {

		}

		@Override
		public void processWatermark(Watermark mark) throws Exception {

		}
	}

	private static class TestingKvStateSnapshot

	// This must only be used in one test, otherwise the static fields will be changed
	// by several tests concurrently
	private static class TestOpenCloseMapFunction extends RichMapFunction<String, String> {
		private static final long serialVersionUID = 1L;

		public static boolean openCalled = false;
		public static boolean closeCalled = false;

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
}

