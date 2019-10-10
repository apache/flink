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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.SourceReaderOperator;
import org.apache.flink.streaming.runtime.io.InputStatus;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.TestHarnessUtil;

import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Tests for verifying that the {@link SourceReaderOperator} as a task input can be integrated
 * well with {@link org.apache.flink.streaming.runtime.io.StreamOneInputProcessor}.
 */
public class SourceReaderStreamTaskTest {

	@Test
	public void testSourceOutputCorrectness() throws Exception {
		final int numRecords = 10;
		final StreamTaskTestHarness<Integer> testHarness = new StreamTaskTestHarness<>(
			SourceReaderStreamTask::new,
			BasicTypeInfo.INT_TYPE_INFO);
		final StreamConfig streamConfig = testHarness.getStreamConfig();

		testHarness.setupOutputForSingletonOperatorChain();
		streamConfig.setStreamOperator(new TestingFiniteSourceReaderOperator(numRecords));
		streamConfig.setOperatorID(new OperatorID());

		testHarness.invoke();
		testHarness.waitForTaskCompletion();

		final LinkedBlockingQueue<Object> expectedOutput = new LinkedBlockingQueue<>();
		for (int i = 1; i <= numRecords; i++) {
			expectedOutput.add(new StreamRecord<>(i));
		}

		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());
	}

	/**
	 * A simple {@link SourceReaderOperator} implementation for emitting limited int type records.
	 */
	private static class TestingFiniteSourceReaderOperator extends SourceReaderOperator<Integer> {
		private static final long serialVersionUID = 1L;

		private final int numRecords;
		private int counter;

		TestingFiniteSourceReaderOperator(int numRecords) {
			this.numRecords = numRecords;
		}

		@Override
		public InputStatus emitNext(DataOutput<Integer> output) throws Exception {
			output.emitRecord(new StreamRecord<>(++counter));

			return counter < numRecords ? InputStatus.MORE_AVAILABLE : InputStatus.END_OF_INPUT;
		}

		@Override
		public CompletableFuture<?> isAvailable() {
			throw new UnsupportedOperationException();
		}

		@Override
		public void close() {
		}
	}
}
