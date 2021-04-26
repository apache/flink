/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators.source;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateInitializationContextImpl;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.operators.SourceOperator;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;

/**
 * Tests that validate correct handling of watermark generation in the {@link ReaderOutput} as created
 * by the {@link ProgressiveTimestampsAndWatermarks}.
 */
@RunWith(Parameterized.class)
public class SourceOperatorEventTimeTest {

	@Parameterized.Parameters(name = "Emit progressive watermarks: {0}")
	public static Collection<Boolean> parameters () {
		return Arrays.asList(true, false);
	}

	private final boolean emitProgressiveWatermarks;

	public SourceOperatorEventTimeTest(boolean emitProgressiveWatermarks) {
		this.emitProgressiveWatermarks = emitProgressiveWatermarks;
	}

	@Test
	public void testMainOutputPeriodicWatermarks() throws Exception {
		final WatermarkStrategy<Integer> watermarkStrategy =
				WatermarkStrategy
						.forGenerator((ctx) -> new OnPeriodicTestWatermarkGenerator<>());

		final List<Watermark> result = testSequenceOfWatermarks(
			emitProgressiveWatermarks,
			watermarkStrategy,
			(output) -> output.collect(0, 100L),
			(output) -> output.collect(0, 120L),
			(output) -> output.collect(0, 110L)
		);

		assertWatermarksOrEmpty(
				result,
				new Watermark(100L),
				new Watermark(120L));
	}

	@Test
	public void testMainOutputEventWatermarks() throws Exception {
		final WatermarkStrategy<Integer> watermarkStrategy =
				WatermarkStrategy
						.forGenerator((ctx) -> new OnEventTestWatermarkGenerator<>());

		final List<Watermark> result = testSequenceOfWatermarks(
			emitProgressiveWatermarks,
			watermarkStrategy,
			(output) -> output.collect(0, 100L),
			(output) -> output.collect(0, 120L),
			(output) -> output.collect(0, 110L)
		);

		assertWatermarksOrEmpty(
				result,
				new Watermark(100L),
				new Watermark(120L));
	}

	@Test
	public void testPerSplitOutputPeriodicWatermarks() throws Exception {
		final WatermarkStrategy<Integer> watermarkStrategy =
				WatermarkStrategy
						.forGenerator((ctx) -> new OnPeriodicTestWatermarkGenerator<>());

		final List<Watermark> result = testSequenceOfWatermarks(
			emitProgressiveWatermarks,
			watermarkStrategy,
			(output) -> {
				output.createOutputForSplit("A");
				output.createOutputForSplit("B");
			},
			(output) -> output.createOutputForSplit("A").collect(0, 100L),
			(output) -> output.createOutputForSplit("B").collect(0, 200L),
			(output) -> output.createOutputForSplit("A").collect(0, 150L),
			(output) -> output.releaseOutputForSplit("A"),
			(output) -> output.createOutputForSplit("B").collect(0, 200L)
		);

		assertWatermarksOrEmpty(
				result,
				new Watermark(100L),
				new Watermark(150L),
				new Watermark(200L));
	}

	@Test
	public void testPerSplitOutputEventWatermarks() throws Exception {
		final WatermarkStrategy<Integer> watermarkStrategy =
				WatermarkStrategy
						.forGenerator((ctx) -> new OnEventTestWatermarkGenerator<>());

		final List<Watermark> result = testSequenceOfWatermarks(
			emitProgressiveWatermarks,
			watermarkStrategy,
			(output) -> {
				output.createOutputForSplit("one");
				output.createOutputForSplit("two");
			},
			(output) -> output.createOutputForSplit("one").collect(0, 100L),
			(output) -> output.createOutputForSplit("two").collect(0, 200L),
			(output) -> output.createOutputForSplit("one").collect(0, 150L),
			(output) -> output.releaseOutputForSplit("one"),
			(output) -> output.createOutputForSplit("two").collect(0, 200L)
		);

		assertWatermarksOrEmpty(
				result,
				new Watermark(100L),
				new Watermark(150L),
				new Watermark(200L));
	}

	// ------------------------------------------------------------------------
	//   test execution helpers
	// ------------------------------------------------------------------------

	/**
	 * Asserts that the given expected watermarks are present in the actual watermarks in STREAMING
	 * mode. Otherwise, asserts that the list of actual watermarks is empty in BATCH mode.
	 */
	private void assertWatermarksOrEmpty(List<Watermark> actualWatermarks, Watermark... expectedWatermarks) {
		// We add the expected Long.MAX_VALUE watermark to the end. We expect that for both
		// "STREAMING" and "BATCH" mode.
		if (emitProgressiveWatermarks) {
			ArrayList<Watermark> watermarks = Lists.newArrayList(expectedWatermarks);
			watermarks.add(new Watermark(Long.MAX_VALUE));
			assertThat(actualWatermarks, contains(watermarks.toArray()));
		} else {
			assertThat(actualWatermarks, contains(new Watermark(Long.MAX_VALUE)));
		}
	}

	@SuppressWarnings("FinalPrivateMethod")
	@SafeVarargs
	private final List<Watermark> testSequenceOfWatermarks(
			final boolean emitProgressiveWatermarks,
			final WatermarkStrategy<Integer> watermarkStrategy,
			final Consumer<ReaderOutput<Integer>>... actions) throws Exception {

		final List<Object> allEvents = testSequenceOfEvents(emitProgressiveWatermarks, watermarkStrategy, actions);

		return allEvents.stream()
				.filter((evt) -> evt instanceof org.apache.flink.streaming.api.watermark.Watermark)
				.map((evt) -> new Watermark(((org.apache.flink.streaming.api.watermark.Watermark) evt).getTimestamp()))
				.collect(Collectors.toList());
	}

	@SuppressWarnings("FinalPrivateMethod")
	@SafeVarargs
	private final List<Object> testSequenceOfEvents(
			final boolean emitProgressiveWatermarks,
			final WatermarkStrategy<Integer> watermarkStrategy,
			final Consumer<ReaderOutput<Integer>>... actions) throws Exception {

		final CollectingDataOutput<Integer> out = new CollectingDataOutput<>();

		final TestProcessingTimeService timeService = new TestProcessingTimeService();
		timeService.setCurrentTime(Integer.MAX_VALUE); // start somewhere that is not zero

		final SourceReader<Integer, MockSourceSplit> reader = new InterpretingSourceReader(actions);

		final SourceOperator<Integer, MockSourceSplit> sourceOperator =
				createTestOperator(reader, watermarkStrategy, timeService, emitProgressiveWatermarks);

		while (sourceOperator.emitNext(out) != InputStatus.END_OF_INPUT) {
			timeService.setCurrentTime(timeService.getCurrentProcessingTime() + 100);
		}

		return out.events;
	}

	// ------------------------------------------------------------------------
	//   test setup helpers
	// ------------------------------------------------------------------------

	private static <T> SourceOperator<T, MockSourceSplit> createTestOperator(
			SourceReader<T, MockSourceSplit> reader,
			WatermarkStrategy<T> watermarkStrategy,
			ProcessingTimeService timeService,
			boolean emitProgressiveWatermarks) throws Exception {

		final OperatorStateStore operatorStateStore =
				new MemoryStateBackend().createOperatorStateBackend(
						new MockEnvironmentBuilder().build(),
						"test-operator",
						Collections.emptyList(),
						new CloseableRegistry());

		final StateInitializationContext stateContext = new StateInitializationContextImpl(
			false, operatorStateStore, null, null, null);

		final SourceOperator<T, MockSourceSplit> sourceOperator =
				new TestingSourceOperator<>(reader, watermarkStrategy, timeService, emitProgressiveWatermarks);
		sourceOperator.initializeState(stateContext);
		sourceOperator.open();

		return sourceOperator;
	}

	// ------------------------------------------------------------------------
	//   test mocks
	// ------------------------------------------------------------------------

	private static final class InterpretingSourceReader implements SourceReader<Integer, MockSourceSplit> {

		private final Iterator<Consumer<ReaderOutput<Integer>>> actions;

		@SafeVarargs
		private InterpretingSourceReader(Consumer<ReaderOutput<Integer>>... actions) {
			this.actions = Arrays.asList(actions).iterator();
		}

		@Override
		public void start() {}

		@Override
		public InputStatus pollNext(ReaderOutput<Integer> output) {
			if (actions.hasNext()) {
				actions.next().accept(output);
				return InputStatus.MORE_AVAILABLE;
			} else {
				return InputStatus.END_OF_INPUT;
			}
		}

		@Override
		public List<MockSourceSplit> snapshotState(long checkpointId) {
			throw new UnsupportedOperationException();
		}

		@Override
		public CompletableFuture<Void> isAvailable() {
			return CompletableFuture.completedFuture(null);
		}

		@Override
		public void addSplits(List<MockSourceSplit> splits) {}

		@Override
		public void notifyNoMoreSplits() {}

		@Override
		public void close() {}
	}
}
