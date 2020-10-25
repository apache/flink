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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.watermark.Watermark;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

/**
 * Tests for {@link InputFormatSourceFunction}.
 */
public class InputFormatSourceFunctionTest {

	@Test
	public void testNormalOp() throws Exception {
		testFormatLifecycle(false);
	}

	@Test
	public void testCancelation() throws Exception {
		testFormatLifecycle(true);
	}

	private void testFormatLifecycle(final boolean midCancel) throws Exception {

		final int noOfSplits = 5;
		final int cancelAt = 2;

		final LifeCycleTestInputFormat format = new LifeCycleTestInputFormat();
		final InputFormatSourceFunction<Integer> reader = new InputFormatSourceFunction<>(format, TypeInformation.of(Integer.class));

		try (MockEnvironment environment =
				new MockEnvironmentBuilder()
					.setTaskName("no")
					.setManagedMemorySize(4 * MemoryManager.DEFAULT_PAGE_SIZE)
					.build()) {

			reader.setRuntimeContext(new MockRuntimeContext(format, noOfSplits, environment));

			Assert.assertTrue(!format.isConfigured);
			Assert.assertTrue(!format.isInputFormatOpen);
			Assert.assertTrue(!format.isSplitOpen);

			reader.open(new Configuration());
			Assert.assertTrue(format.isConfigured);

			TestSourceContext ctx = new TestSourceContext(reader, format, midCancel, cancelAt);
			reader.run(ctx);

			int splitsSeen = ctx.getSplitsSeen();
			Assert.assertTrue(midCancel ? splitsSeen == cancelAt : splitsSeen == noOfSplits);

			// we have exhausted the splits so the
			// format and splits should be closed by now

			Assert.assertTrue(!format.isSplitOpen);
			Assert.assertTrue(!format.isInputFormatOpen);
		}
	}

	private static class LifeCycleTestInputFormat extends RichInputFormat<Integer, InputSplit> {

		private static final long serialVersionUID = 7408902249499583273L;
		private boolean isConfigured = false;
		private boolean isInputFormatOpen = false;
		private boolean isSplitOpen = false;

		// end of split
		private boolean eos = false;

		private int splitCounter = 0;

		private int reachedEndCalls = 0;
		private int nextRecordCalls = 0;

		@Override
		public void openInputFormat() {
			Assert.assertTrue(isConfigured);
			Assert.assertTrue(!isInputFormatOpen);
			Assert.assertTrue(!isSplitOpen);
			this.isInputFormatOpen = true;
		}

		@Override
		public void closeInputFormat() {
			Assert.assertTrue(!isSplitOpen);
			this.isInputFormatOpen = false;
		}

		@Override
		public void configure(Configuration parameters) {
			Assert.assertTrue(!isConfigured);
			this.isConfigured = true;
		}

		@Override
		public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
			return null;
		}

		@Override
		public InputSplit[] createInputSplits(int minNumSplits) throws IOException {
			Assert.assertTrue(isConfigured);
			InputSplit[] splits = new InputSplit[minNumSplits];
			for (int i = 0; i < minNumSplits; i++) {
				final int idx = i;
				splits[idx] = new InputSplit() {
					private static final long serialVersionUID = -1480792932361908285L;

					@Override
					public int getSplitNumber() {
						return idx;
					}
				};
			}
			return splits;
		}

		@Override
		public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
			return null;
		}

		@Override
		public void open(InputSplit split) throws IOException {
			// whenever a new split opens,
			// the previous should have been closed
			Assert.assertTrue(isInputFormatOpen);
			Assert.assertTrue(isConfigured);
			Assert.assertTrue(!isSplitOpen);

			isSplitOpen = true;
			eos = false;
		}

		@Override
		public boolean reachedEnd() throws IOException {
			Assert.assertTrue(isInputFormatOpen);
			Assert.assertTrue(isConfigured);
			Assert.assertTrue(isSplitOpen);

			if (!eos) {
				reachedEndCalls++;
			}
			return eos;
		}

		@Override
		public Integer nextRecord(Integer reuse) throws IOException {
			Assert.assertTrue(isInputFormatOpen);
			Assert.assertTrue(isConfigured);
			Assert.assertTrue(isSplitOpen);

			Assert.assertTrue(reachedEndCalls == ++nextRecordCalls);

			eos = true;
			return splitCounter++;
		}

		@Override
		public void close() throws IOException {
			this.isSplitOpen = false;
		}
	}

	private static class TestSourceContext implements SourceFunction.SourceContext<Integer> {

		private final InputFormatSourceFunction<Integer> reader;
		private final LifeCycleTestInputFormat format;
		private final boolean shouldCancel;
		private final int cancelAt;

		int splitIdx = 0;

		private TestSourceContext(InputFormatSourceFunction<Integer> reader, LifeCycleTestInputFormat format, boolean shouldCancel, int cancelAt) {
			this.reader = reader;
			this.format = format;
			this.shouldCancel = shouldCancel;
			this.cancelAt = cancelAt;
		}

		@Override
		public void collect(Integer element) {
			Assert.assertTrue(format.isSplitOpen);
			Assert.assertTrue(splitIdx == element);
			if (shouldCancel && splitIdx == cancelAt) {
				reader.cancel();
			} else {
				splitIdx++;
			}
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

	@SuppressWarnings("deprecation")
	private static class MockRuntimeContext extends StreamingRuntimeContext {

		private final int noOfSplits;
		private int nextSplit = 0;
		private final LifeCycleTestInputFormat format;
		private InputSplit[] inputSplits;

		private MockRuntimeContext(LifeCycleTestInputFormat format, int noOfSplits, Environment environment) {
			super(new MockStreamOperator(),
				environment,
				Collections.<String, Accumulator<?, ?>>emptyMap());

			this.noOfSplits = noOfSplits;
			this.format = format;
		}

		@Override
		public MetricGroup getMetricGroup() {
			return new UnregisteredMetricsGroup();
		}

		@Override
		public InputSplitProvider getInputSplitProvider() {
			try {
				this.inputSplits = format.createInputSplits(noOfSplits);
				Assert.assertTrue(inputSplits.length == noOfSplits);
			} catch (IOException e) {
				e.printStackTrace();
			}

			return new InputSplitProvider() {
				@Override
				public InputSplit getNextInputSplit(ClassLoader userCodeClassLoader) {
					if (nextSplit < inputSplits.length) {
						return inputSplits[nextSplit++];
					}
					return null;
				}
			};
		}

		// ------------------------------------------------------------------------

		private static class MockStreamOperator extends AbstractStreamOperator<Integer> {
			private static final long serialVersionUID = -1153976702711944427L;

			@Override
			public ExecutionConfig getExecutionConfig() {
				return new ExecutionConfig();
			}

			@Override
			public OperatorID getOperatorID() {
				return new OperatorID();
			}
		}
	}
}
