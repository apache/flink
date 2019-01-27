/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.streaming.api.functions.source.InputFormatSourceFunctionTest.LifeCycleTestInputFormat;
import org.apache.flink.streaming.api.functions.source.InputFormatSourceFunctionTest.MockRuntimeContext;
import org.apache.flink.streaming.api.watermark.Watermark;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link InputFormatSourceFunction}.
 */
public class InputFormatSourceFunctionV2Test {

	@Test
	public void testFormatLifecycle() throws Exception {

		final int noOfSplits = 5;

		final LifeCycleTestInputFormat format = new LifeCycleTestInputFormat();
		final InputFormatSourceFunctionV2<Integer> reader = new InputFormatSourceFunctionV2<>(format, TypeInformation.of(Integer.class));

		try (MockEnvironment environment =
				new MockEnvironmentBuilder()
					.setTaskName("no")
					.setMemorySize(4 * MemoryManager.DEFAULT_PAGE_SIZE)
					.build()) {

			reader.setRuntimeContext(new MockRuntimeContext(format, noOfSplits, environment));

			Assert.assertTrue(!format.isConfigured);
			Assert.assertTrue(!format.isInputFormatOpen);
			Assert.assertTrue(!format.isSplitOpen);

			reader.open(new Configuration());
			Assert.assertTrue(format.isConfigured);

			TestSourceContext ctx = new TestSourceContext(format);
			while (!reader.isFinished()) {
				SourceRecord<Integer> sourceRecord = reader.next();
				if (sourceRecord != null && sourceRecord.getRecord() != null) {
					ctx.collect(sourceRecord.getRecord());
				}
			}

			int splitsSeen = ctx.getSplitsSeen();
			Assert.assertEquals(noOfSplits, splitsSeen);

			// we have exhausted the splits so the
			// format and splits should be closed by now

			reader.close();

			Assert.assertTrue(!format.isSplitOpen);
			Assert.assertTrue(!format.isInputFormatOpen);
		}
	}

	private static class TestSourceContext implements SourceFunction.SourceContext<Integer> {

		private final LifeCycleTestInputFormat format;

		int splitIdx = 0;

		TestSourceContext(LifeCycleTestInputFormat format) {
			this.format = format;
		}

		@Override
		public void collect(Integer element) {
			Assert.assertTrue(format.isSplitOpen);
			Assert.assertTrue(splitIdx == element);
			splitIdx++;
		}

		@Override
		public void collectWithTimestamp(Integer element, long timestamp) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void emitWatermark(Watermark mark) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void markAsTemporarilyIdle() {
			throw new UnsupportedOperationException();
		}

		@Override
		public Object getCheckpointLock() {
			return null;
		}

		@Override
		public void close() {
			throw new UnsupportedOperationException();
		}

		public int getSplitsSeen() {
			return this.splitIdx;
		}
	}
}

