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

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProviderException;
import org.apache.flink.streaming.api.functions.source.InputFormatSourceFunctionV2;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.StreamSourceV2;
import org.apache.flink.streaming.util.TestHarnessUtil;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * These tests verify that the RichFunction methods are called (in correct order). And that
 * checkpointing/element emission don't occur concurrently.
 */
public class InputFormatSourceStreamTaskV2Test {

	/**
	 * This test verifies that open() and close() are correctly called by the StreamTask.
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void testRun() throws Exception {
		final StreamTaskTestHarness<String> testHarness = new StreamTaskTestHarness<>(
				SourceStreamTaskV2::new, BasicTypeInfo.STRING_TYPE_INFO);

		testHarness.setupOutputForSingletonOperatorChain();

		StreamConfig streamConfig = testHarness.getStreamConfig();
		StreamSourceV2<String, ?> sourceOperator = new StreamSourceV2<>(new InputFormatSourceFunctionV2(new TestInputFormat(), BasicTypeInfo.STRING_TYPE_INFO));
		streamConfig.setStreamOperator(sourceOperator);
		streamConfig.setOperatorID(new OperatorID());

		testHarness.invoke(
			new StreamMockEnvironment(
				testHarness.jobConfig,
				testHarness.taskConfig,
				testHarness.executionConfig,
				testHarness.memorySize,
				new TestInputSplitProvider(),
				testHarness.bufferSize,
				testHarness.taskStateManager));
		testHarness.waitForTaskCompletion();

		Assert.assertTrue("RichFunction methods where not called.", TestInputFormat.closeCalled);

		List<String> resultElements = TestHarnessUtil.getRawElementsFromOutput(testHarness.getOutput());
		Assert.assertEquals(10, resultElements.size());
		final List<String> expectedElements = new ArrayList<>();
		for (int i = 0; i < 10; i++) {
			expectedElements.add("Hello" + i);
		}
		Assert.assertEquals(expectedElements, resultElements);
	}

	private static class TestInputFormat implements InputFormat<String, TestingInputSplit> {
		private static final long serialVersionUID = 1L;

		public static boolean openCalled = false;
		public static boolean closeCalled = false;

		private int currentIndex = 0;

		private boolean isFinished = false;

		@Override
		public void configure(Configuration parameters) {

		}

		@Override
		public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
			return null;
		}

		@Override
		public TestingInputSplit[] createInputSplits(int minNumSplits) throws IOException {
			return null;
		}

		@Override
		public InputSplitAssigner getInputSplitAssigner(TestingInputSplit[] inputSplits) {
			return null;
		}

		@Override
		public void open(TestingInputSplit split) throws IOException {
			if (closeCalled) {
				Assert.fail("Close called before open.");
			}
			openCalled = true;
			Assert.assertEquals(0, split.getSplitNumber());
		}

		@Override
		public boolean reachedEnd() throws IOException {
			return isFinished;
		}

		@Override
		public String nextRecord(String reuse) throws IOException {
			if (!openCalled) {
				Assert.fail("Open was not called before run.");
			}
			if (currentIndex < 10) {
				return "Hello" + currentIndex++;
			}
			isFinished = true;
			return null;
		}

		@Override
		public void close() throws IOException {
			if (!openCalled) {
				Assert.fail("Open was not called before close.");
			}
			closeCalled = true;
		}
	}

	private static final class TestingInputSplit implements InputSplit {

		private final int splitNumber;

		public TestingInputSplit(int number) {
			this.splitNumber = number;
		}

		public int getSplitNumber() {
			return splitNumber;
		}
	}

	private static final class TestInputSplitProvider implements InputSplitProvider {

		private InputSplit currentSplit = new TestingInputSplit(0);

		@Override
		public InputSplit getNextInputSplit(OperatorID operatorID, ClassLoader userCodeClassLoader) throws InputSplitProviderException {
			InputSplit split = null;
			if (currentSplit != null) {
				split = currentSplit;
				currentSplit = null;
			}
			return split;
		}

		@Override
		public Map<OperatorID, List<InputSplit>> getAssignedInputSplits() {
			return null;
		}
	}
}

