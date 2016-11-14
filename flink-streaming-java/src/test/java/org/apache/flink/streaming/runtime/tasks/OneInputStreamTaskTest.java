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
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.SubtaskState;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.runtime.state.TaskStateHandles;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamCheckpointedOperator;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;

import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link OneInputStreamTask}.
 *
 * <p>
 * Note:<br>
 * We only use a {@link StreamMap} operator here. We also test the individual operators but Map is
 * used as a representative to test OneInputStreamTask, since OneInputStreamTask is used for all
 * OneInputStreamOperators.
 */
public class OneInputStreamTaskTest extends TestLogger {

	private static final ListStateDescriptor<Integer> TEST_DESCRIPTOR =
			new ListStateDescriptor<>("test", new IntSerializer());

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

		assertTrue("RichFunction methods where not called.", TestOpenCloseMapFunction.closeCalled);

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
		assertEquals(2, resultElements.size());
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

		expectedOutput.add(new CancelCheckpointMarker(0));
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
	 * Tests that the stream operator can snapshot and restore the operator state of chained
	 * operators
	 */
	@Test
	public void testSnapshottingAndRestoring() throws Exception {
		final Deadline deadline = new FiniteDuration(2, TimeUnit.MINUTES).fromNow();
		final OneInputStreamTask<String, String> streamTask = new OneInputStreamTask<String, String>();
		final OneInputStreamTaskTestHarness<String, String> testHarness = new OneInputStreamTaskTestHarness<String, String>(streamTask, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);
		IdentityKeySelector<String> keySelector = new IdentityKeySelector<>();
		testHarness.configureForKeyedStream(keySelector, BasicTypeInfo.STRING_TYPE_INFO);

		long checkpointId = 1L;
		long checkpointTimestamp = 1L;
		long recoveryTimestamp = 3L;
		long seed = 2L;
		int numberChainedTasks = 11;

		StreamConfig streamConfig = testHarness.getStreamConfig();

		configureChainedTestingStreamOperator(streamConfig, numberChainedTasks, seed, recoveryTimestamp);

		AcknowledgeStreamMockEnvironment env = new AcknowledgeStreamMockEnvironment(
			testHarness.jobConfig,
			testHarness.taskConfig,
			testHarness.executionConfig,
			testHarness.memorySize,
			new MockInputSplitProvider(),
			testHarness.bufferSize);

		// reset number of restore calls
		TestingStreamOperator.numberRestoreCalls = 0;

		testHarness.invoke(env);
		testHarness.waitForTaskRunning(deadline.timeLeft().toMillis());

		CheckpointMetaData checkpointMetaData = new CheckpointMetaData(checkpointId, checkpointTimestamp);

		while(!streamTask.triggerCheckpoint(checkpointMetaData));

		// since no state was set, there shouldn't be restore calls
		assertEquals(0, TestingStreamOperator.numberRestoreCalls);

		env.getCheckpointLatch().await();

		assertEquals(checkpointId, env.getCheckpointId());

		testHarness.endInput();
		testHarness.waitForTaskCompletion(deadline.timeLeft().toMillis());

		final OneInputStreamTask<String, String> restoredTask = new OneInputStreamTask<String, String>();
		restoredTask.setInitialState(new TaskStateHandles(env.getCheckpointStateHandles()));

		final OneInputStreamTaskTestHarness<String, String> restoredTaskHarness = new OneInputStreamTaskTestHarness<String, String>(restoredTask, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);
		restoredTaskHarness.configureForKeyedStream(keySelector, BasicTypeInfo.STRING_TYPE_INFO);

		StreamConfig restoredTaskStreamConfig = restoredTaskHarness.getStreamConfig();

		configureChainedTestingStreamOperator(restoredTaskStreamConfig, numberChainedTasks, seed, recoveryTimestamp);

		TestingStreamOperator.numberRestoreCalls = 0;

		restoredTaskHarness.invoke();
		restoredTaskHarness.endInput();
		restoredTaskHarness.waitForTaskCompletion(deadline.timeLeft().toMillis());

		// restore of every chained operator should have been called
		assertEquals(numberChainedTasks, TestingStreamOperator.numberRestoreCalls);

		TestingStreamOperator.numberRestoreCalls = 0;
	}

	//==============================================================================================
	// Utility functions and classes
	//==============================================================================================

	private void configureChainedTestingStreamOperator(
		StreamConfig streamConfig,
		int numberChainedTasks,
		long seed,
		long recoveryTimestamp) {

		Preconditions.checkArgument(numberChainedTasks >= 1, "The operator chain must at least " +
			"contain one operator.");

		Random random = new Random(seed);

		TestingStreamOperator<Integer, Integer> previousOperator = new TestingStreamOperator<>(random.nextLong(), recoveryTimestamp);
		streamConfig.setStreamOperator(previousOperator);

		// create the chain of operators
		Map<Integer, StreamConfig> chainedTaskConfigs = new HashMap<>(numberChainedTasks - 1);
		List<StreamEdge> outputEdges = new ArrayList<>(numberChainedTasks - 1);

		for (int chainedIndex = 1; chainedIndex < numberChainedTasks; chainedIndex++) {
			TestingStreamOperator<Integer, Integer> chainedOperator = new TestingStreamOperator<>(random.nextLong(), recoveryTimestamp);
			StreamConfig chainedConfig = new StreamConfig(new Configuration());
			chainedConfig.setStreamOperator(chainedOperator);
			chainedTaskConfigs.put(chainedIndex, chainedConfig);

			StreamEdge outputEdge = new StreamEdge(
				new StreamNode(
					null,
					chainedIndex - 1,
					null,
					null,
					null,
					null,
					null
				),
				new StreamNode(
					null,
					chainedIndex,
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

	private static class AcknowledgeStreamMockEnvironment extends StreamMockEnvironment {
		private volatile long checkpointId;
		private volatile SubtaskState checkpointStateHandles;

		private final OneShotLatch checkpointLatch = new OneShotLatch();

		public long getCheckpointId() {
			return checkpointId;
		}

		AcknowledgeStreamMockEnvironment(
				Configuration jobConfig, Configuration taskConfig,
				ExecutionConfig executionConfig, long memorySize,
				MockInputSplitProvider inputSplitProvider, int bufferSize) {
			super(jobConfig, taskConfig, executionConfig, memorySize, inputSplitProvider, bufferSize);
		}


		@Override
		public void acknowledgeCheckpoint(
				CheckpointMetaData checkpointMetaData,
				SubtaskState checkpointStateHandles) {

			this.checkpointId = checkpointMetaData.getCheckpointId();
			this.checkpointStateHandles = checkpointStateHandles;
			checkpointLatch.trigger();
		}

		public OneShotLatch getCheckpointLatch() {
			return checkpointLatch;
		}

		public SubtaskState getCheckpointStateHandles() {
			return checkpointStateHandles;
		}
	}

	private static class TestingStreamOperator<IN, OUT>
			extends AbstractStreamOperator<OUT>
			implements OneInputStreamOperator<IN, OUT>, StreamCheckpointedOperator {

		private static final long serialVersionUID = 774614855940397174L;

		public static int numberRestoreCalls = 0;
		public static int numberSnapshotCalls = 0;

		private final long seed;
		private final long recoveryTimestamp;

		private transient Random random;

		@Override
		public void open() throws Exception {
			super.open();

			ListState<Integer> partitionableState = getOperatorStateBackend().getOperatorState(TEST_DESCRIPTOR);

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
		public void snapshotState(StateSnapshotContext context) throws Exception {
			ListState<Integer> partitionableState =
					getOperatorStateBackend().getOperatorState(TEST_DESCRIPTOR);
			partitionableState.clear();

			partitionableState.add(42);
			partitionableState.add(4711);

			++numberSnapshotCalls;
		}

		@Override
		public void initializeState(StateInitializationContext context) throws Exception {

		}

		TestingStreamOperator(long seed, long recoveryTimestamp) {
			this.seed = seed;
			this.recoveryTimestamp = recoveryTimestamp;
		}

		@Override
		public void processElement(StreamRecord<IN> element) throws Exception {

		}

		@Override
		public void snapshotState(FSDataOutputStream out, long checkpointId, long timestamp) throws Exception {
			if (random == null) {
				random = new Random(seed);
			}

			Serializable functionState = generateFunctionState();
			Integer operatorState = generateOperatorState();

			InstantiationUtil.serializeObject(out, functionState);
			InstantiationUtil.serializeObject(out, operatorState);
		}

		@Override
		public void restoreState(FSDataInputStream in) throws Exception {
			numberRestoreCalls++;

			if (random == null) {
				random = new Random(seed);
			}

			assertEquals(this.recoveryTimestamp, recoveryTimestamp);

			assertNotNull(in);

			ClassLoader cl = Thread.currentThread().getContextClassLoader();

			Serializable functionState= InstantiationUtil.deserializeObject(in, cl);
			Integer operatorState= InstantiationUtil.deserializeObject(in, cl);

			assertEquals(random.nextInt(), functionState);
			assertEquals(random.nextInt(), (int) operatorState);
		}

		private Serializable generateFunctionState() {
			return random.nextInt();
		}

		private Integer generateOperatorState() {
			return random.nextInt();
		}
	}


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

