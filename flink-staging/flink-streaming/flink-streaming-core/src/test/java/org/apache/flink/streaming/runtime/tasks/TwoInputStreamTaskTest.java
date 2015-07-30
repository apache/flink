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


import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.RichCoMapFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.co.CoStreamMap;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Tests for {@link org.apache.flink.streaming.runtime.tasks.TwoInputStreamTask}. Theses tests
 * implicitly also test the {@link org.apache.flink.streaming.runtime.io.StreamTwoInputProcessor}.
 *
 * <p>
 * Note:<br>
 * We only use a {@link CoStreamMap} operator here. We also test the individual operators but Map is
 * used as a representative to test TwoInputStreamTask, since TwoInputStreamTask is used for all
 * TwoInputStreamOperators.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ResultPartitionWriter.class})
public class TwoInputStreamTaskTest {

	/**
	 * This test verifies that open() and close() are correctly called. This test also verifies
	 * that timestamps of emitted elements are correct. {@link CoStreamMap} assigns the input
	 * timestamp to emitted elements.
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void testOpenCloseAndTimestamps() throws Exception {
		final TwoInputStreamTask<String, Integer, String> coMapTask = new TwoInputStreamTask<String, Integer, String>();
		final TwoInputStreamTaskTestHarness<String, Integer, String> testHarness = new TwoInputStreamTaskTestHarness<String, Integer, String>(coMapTask, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);

		StreamConfig streamConfig = testHarness.getStreamConfig();
		CoStreamMap<String, Integer, String> coMapOperator = new CoStreamMap<String, Integer, String>(new TestOpenCloseMapFunction());
		streamConfig.setStreamOperator(coMapOperator);

		long initialTime = 0L;
		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<Object>();

		testHarness.invoke();

		testHarness.processElement(new StreamRecord<String>("Hello", initialTime + 1), 0, 0);
		expectedOutput.add(new StreamRecord<String>("Hello", initialTime + 1));

		// wait until the input is processed to ensure ordering of the output
		testHarness.waitForInputProcessing();

		testHarness.processElement(new StreamRecord<Integer>(1337, initialTime + 2), 1, 0);

		expectedOutput.add(new StreamRecord<String>("1337", initialTime + 2));

		testHarness.endInput();

		testHarness.waitForTaskCompletion();

		Assert.assertTrue("RichFunction methods where not called.", TestOpenCloseMapFunction.closeCalled);

		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());
	}

	/**
	 * This test verifies that watermarks are correctly forwarded. This also checks whether
	 * watermarks are forwarded only when we have received watermarks from all inputs. The
	 * forwarded watermark must be the minimum of the watermarks of all inputs.
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void testWatermarkForwarding() throws Exception {
		final TwoInputStreamTask<String, Integer, String> coMapTask = new TwoInputStreamTask<String, Integer, String>();
		final TwoInputStreamTaskTestHarness<String, Integer, String> testHarness = new TwoInputStreamTaskTestHarness<String, Integer, String>(coMapTask, 2, 2, new int[] {1, 2}, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);

		StreamConfig streamConfig = testHarness.getStreamConfig();
		CoStreamMap<String, Integer, String> coMapOperator = new CoStreamMap<String, Integer, String>(new IdentityMap());
		streamConfig.setStreamOperator(coMapOperator);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<Object>();
		long initialTime = 0L;

		testHarness.invoke();

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
		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

		// contrary to checkpoint barriers these elements are not blocked by watermarks
		testHarness.processElement(new StreamRecord<String>("Hello", initialTime), 0, 0);
		testHarness.processElement(new StreamRecord<Integer>(42, initialTime), 1, 1);
		expectedOutput.add(new StreamRecord<String>("Hello", initialTime));
		expectedOutput.add(new StreamRecord<String>("42", initialTime));

		testHarness.waitForInputProcessing();
		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.processElement(new Watermark(initialTime + 4), 0, 0);
		testHarness.processElement(new Watermark(initialTime + 3), 0, 1);
		testHarness.processElement(new Watermark(initialTime + 3), 1, 0);
		testHarness.processElement(new Watermark(initialTime + 2), 1, 1);

		// check whether we get the minimum of all the watermarks, this must also only occur in
		// the output after the two StreamRecords
		expectedOutput.add(new Watermark(initialTime + 2));
		testHarness.waitForInputProcessing();
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
	@SuppressWarnings("unchecked")
	public void testCheckpointBarriers() throws Exception {
		final TwoInputStreamTask<String, Integer, String> coMapTask = new TwoInputStreamTask<String, Integer, String>();
		final TwoInputStreamTaskTestHarness<String, Integer, String> testHarness = new TwoInputStreamTaskTestHarness<String, Integer, String>(coMapTask, 2, 2, new int[] {1, 2}, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);

		StreamConfig streamConfig = testHarness.getStreamConfig();
		CoStreamMap<String, Integer, String> coMapOperator = new CoStreamMap<String, Integer, String>(new IdentityMap());
		streamConfig.setStreamOperator(coMapOperator);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<Object>();
		long initialTime = 0L;

		testHarness.invoke();

		testHarness.processEvent(new CheckpointBarrier(0, 0), 0, 0);

		// This element should be buffered since we received a checkpoint barrier on
		// this input
		testHarness.processElement(new StreamRecord<String>("Hello-0-0", initialTime), 0, 0);

		// This one should go through
		testHarness.processElement(new StreamRecord<String>("Ciao-0-0", initialTime), 0, 1);
		expectedOutput.add(new StreamRecord<String>("Ciao-0-0", initialTime));

		// These elements should be forwarded, since we did not yet receive a checkpoint barrier
		// on that input, only add to same input, otherwise we would not know the ordering
		// of the output since the Task might read the inputs in any order
		testHarness.processElement(new StreamRecord<Integer>(11, initialTime), 1, 1);
		testHarness.processElement(new StreamRecord<Integer>(111, initialTime), 1, 1);
		expectedOutput.add(new StreamRecord<String>("11", initialTime));
		expectedOutput.add(new StreamRecord<String>("111", initialTime));

		testHarness.waitForInputProcessing();
		// we should not yet see the barrier, only the two elements from non-blocked input
		TestHarnessUtil.assertOutputEquals("Output was not correct.",
				testHarness.getOutput(),
				expectedOutput);

		testHarness.processEvent(new CheckpointBarrier(0, 0), 0, 1);
		testHarness.processEvent(new CheckpointBarrier(0, 0), 1, 0);
		testHarness.processEvent(new CheckpointBarrier(0, 0), 1, 1);

		testHarness.waitForInputProcessing();

		// now we should see the barrier and after that the buffered elements
		expectedOutput.add(new CheckpointBarrier(0, 0));
		expectedOutput.add(new StreamRecord<String>("Hello-0-0", initialTime));
		TestHarnessUtil.assertOutputEquals("Output was not correct.",
				testHarness.getOutput(),
				expectedOutput);

		testHarness.endInput();

		testHarness.waitForTaskCompletion();

		List<String> resultElements = TestHarnessUtil.getRawElementsFromOutput(testHarness.getOutput());
		Assert.assertEquals(4, resultElements.size());
	}

	/**
	 * This test verifies that checkpoint barriers and barrier buffers work correctly with
	 * concurrent checkpoint barriers where one checkpoint is "overtaking" another checkpoint, i.e.
	 * some inputs receive barriers from an earlier checkpoint, thereby blocking,
	 * then all inputs receive barriers from a later checkpoint.
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void testOvertakingCheckpointBarriers() throws Exception {
		final TwoInputStreamTask<String, Integer, String> coMapTask = new TwoInputStreamTask<String, Integer, String>();
		final TwoInputStreamTaskTestHarness<String, Integer, String> testHarness = new TwoInputStreamTaskTestHarness<String, Integer, String>(coMapTask, 2, 2, new int[] {1, 2}, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);

		StreamConfig streamConfig = testHarness.getStreamConfig();
		CoStreamMap<String, Integer, String> coMapOperator = new CoStreamMap<String, Integer, String>(new IdentityMap());
		streamConfig.setStreamOperator(coMapOperator);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<Object>();
		long initialTime = 0L;

		testHarness.invoke();

		testHarness.processEvent(new CheckpointBarrier(0, 0), 0, 0);

		// These elements should be buffered until we receive barriers from
		// all inputs
		testHarness.processElement(new StreamRecord<String>("Hello-0-0", initialTime), 0, 0);
		testHarness.processElement(new StreamRecord<String>("Ciao-0-0", initialTime), 0, 0);

		// These elements should be forwarded, since we did not yet receive a checkpoint barrier
		// on that input, only add to same input, otherwise we would not know the ordering
		// of the output since the Task might read the inputs in any order
		testHarness.processElement(new StreamRecord<Integer>(42, initialTime), 1, 1);
		testHarness.processElement(new StreamRecord<Integer>(1337, initialTime), 1, 1);
		expectedOutput.add(new StreamRecord<String>("42", initialTime));
		expectedOutput.add(new StreamRecord<String>("1337", initialTime));

		testHarness.waitForInputProcessing();
		// we should not yet see the barrier, only the two elements from non-blocked input
		TestHarnessUtil.assertOutputEquals("Output was not correct.",
				expectedOutput,
				testHarness.getOutput());

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

		TestHarnessUtil.assertOutputEquals("Output was not correct.",
				expectedOutput,
				testHarness.getOutput());


		// Then give the earlier barrier, these should be ignored
		testHarness.processEvent(new CheckpointBarrier(0, 0), 0, 1);
		testHarness.processEvent(new CheckpointBarrier(0, 0), 1, 0);
		testHarness.processEvent(new CheckpointBarrier(0, 0), 1, 1);

		testHarness.waitForInputProcessing();


		testHarness.endInput();

		testHarness.waitForTaskCompletion();

		TestHarnessUtil.assertOutputEquals("Output was not correct.",
				expectedOutput,
				testHarness.getOutput());
	}

	// This must only be used in one test, otherwise the static fields will be changed
	// by several tests concurrently
	private static class TestOpenCloseMapFunction extends RichCoMapFunction<String, Integer, String> {
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
		public String map1(String value) throws Exception {
			if (!openCalled) {
				Assert.fail("Open was not called before run.");
			}
			return value;
		}

		@Override
		public String map2(Integer value) throws Exception {
			if (!openCalled) {
				Assert.fail("Open was not called before run.");
			}
			return value.toString();
		}
	}

	private static class IdentityMap implements CoMapFunction<String, Integer, String> {
		private static final long serialVersionUID = 1L;

		@Override
		public String map1(String value) throws Exception {
			return value;
		}

		@Override
		public String map2(Integer value) throws Exception {

			return value.toString();
		}
	}
}

