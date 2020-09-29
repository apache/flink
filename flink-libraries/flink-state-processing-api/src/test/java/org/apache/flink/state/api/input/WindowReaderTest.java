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

package org.apache.flink.state.api.input;

import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.state.api.functions.WindowReaderFunction;
import org.apache.flink.state.api.input.operator.WindowReaderOperator;
import org.apache.flink.state.api.input.operator.window.PassThroughReader;
import org.apache.flink.state.api.input.splits.KeyGroupRangeInputSplit;
import org.apache.flink.state.api.runtime.OperatorIDGenerator;
import org.apache.flink.state.api.utils.AggregateSum;
import org.apache.flink.state.api.utils.ReduceSum;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.WindowOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;
import org.apache.flink.util.Collector;

import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.mockito.Mockito.mock;

/**
 * Tests reading window state.
 */
@SuppressWarnings("unchecked")
public class WindowReaderTest {

	private static final int MAX_PARALLELISM = 128;

	private static final String UID = "uid";

	@Test
	public void testReducingWindow() throws Exception {
		WindowOperator<Integer, Integer, ?, Void, ?> operator = getWindowOperator(stream -> stream
			.timeWindow(Time.milliseconds(1))
			.reduce(new ReduceSum()));

		OperatorState operatorState = getOperatorState(operator);

		KeyedStateInputFormat<Integer, TimeWindow, Integer> format = new KeyedStateInputFormat<>(
			operatorState,
			new MemoryStateBackend(),
			new Configuration(),
			WindowReaderOperator.reduce(
				new ReduceSum(),
				new PassThroughReader<>(),
				Types.INT,
				new TimeWindow.Serializer(),
				Types.INT));

		List<Integer> list = readState(format);
		Assert.assertEquals(Arrays.asList(1, 1), list);
	}

	@Test
	public void testSessionWindow() throws Exception {
		WindowOperator<Integer, Integer, ?, Void, ?> operator = getWindowOperator(stream -> stream
			.window(EventTimeSessionWindows.withGap(Time.milliseconds(3)))
			.reduce(new ReduceSum()));

		OperatorState operatorState = getOperatorState(operator);

		KeyedStateInputFormat<Integer, TimeWindow, Integer> format = new KeyedStateInputFormat<>(
			operatorState,
			new MemoryStateBackend(),
			new Configuration(),
			WindowReaderOperator.reduce(
				new ReduceSum(),
				new PassThroughReader<>(),
				Types.INT,
				new TimeWindow.Serializer(),
				Types.INT));

		List<Integer> list = readState(format);
		Assert.assertEquals(Collections.singletonList(2), list);
	}

	@Test
	public void testAggregateWindow() throws Exception {
		WindowOperator<Integer, Integer, ?, Void, ?> operator = getWindowOperator(stream -> stream
			.timeWindow(Time.milliseconds(1))
			.aggregate(new AggregateSum()));

		OperatorState operatorState = getOperatorState(operator);

		KeyedStateInputFormat<Integer, TimeWindow, Integer> format = new KeyedStateInputFormat<>(
			operatorState,
			new MemoryStateBackend(),
			new Configuration(),
			WindowReaderOperator.aggregate(
				new AggregateSum(),
				new PassThroughReader<>(),
				Types.INT,
				new TimeWindow.Serializer(),
				Types.INT));

		List<Integer> list = readState(format);
		Assert.assertEquals(Arrays.asList(1, 1), list);
	}

	@Test
	public void testProcessReader() throws Exception {
		WindowOperator<Integer, Integer, ?, Void, ?> operator = getWindowOperator(stream -> stream
			.timeWindow(Time.milliseconds(1))
			.process(mockProcessWindowFunction(), Types.INT));

		OperatorState operatorState = getOperatorState(operator);

		KeyedStateInputFormat<Integer, TimeWindow, Integer> format = new KeyedStateInputFormat<>(
			operatorState,
			new MemoryStateBackend(),
			new Configuration(),
			WindowReaderOperator.process(
				new PassThroughReader<>(),
				Types.INT,
				new TimeWindow.Serializer(),
				Types.INT));

		List<Integer> list = readState(format);
		Assert.assertEquals(Arrays.asList(1, 1), list);
	}

	@Test
	public void testPerPaneAndPerKeyState() throws Exception {
		WindowOperator<Integer, Integer, ?, Void, ?> operator = getWindowOperator(stream -> stream
			.timeWindow(Time.milliseconds(1))
			.trigger(new AlwaysFireTrigger<>())
			.process(new MultiFireWindow(), Types.INT));

		OperatorState operatorState = getOperatorState(operator);

		KeyedStateInputFormat<Integer, TimeWindow, Tuple2<Integer, Integer>> format = new KeyedStateInputFormat<>(
			operatorState,
			new MemoryStateBackend(),
			new Configuration(),
			WindowReaderOperator.process(
				new MultiFireReaderFunction(),
				Types.INT,
				new TimeWindow.Serializer(),
				Types.INT));

		List<Tuple2<Integer, Integer>> list = readState(format);
		Assert.assertEquals(Arrays.asList(Tuple2.of(2, 1), Tuple2.of(2, 1)), list);
	}

	private static WindowOperator<Integer, Integer, ?, Void, ?> getWindowOperator(
		Function<KeyedStream<Integer, Integer>, SingleOutputStreamOperator<Integer>> window) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		KeyedStream<Integer, Integer> keyedStream = env
			.addSource(mockSourceFunction())
			.returns(Integer.class)
			.keyBy(new IdentityKeySelector<>());

		DataStream<Integer> stream = window
			.apply(keyedStream)
			.uid(UID);

		return getLastOperator(stream);
	}

	private static SourceFunction<Integer> mockSourceFunction() {
		return (SourceFunction<Integer>) mock(SourceFunction.class);
	}

	private static <W extends Window> ProcessWindowFunction<Integer, Integer, Integer, W> mockProcessWindowFunction() {
		return mock(ProcessWindowFunction.class);
	}

	private static OperatorState getOperatorState(WindowOperator<Integer, Integer, ?, Void, ?> operator) throws Exception {
		KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Void> harness = new KeyedOneInputStreamOperatorTestHarness<>(
			operator,
			new IdentityKeySelector<>(),
			Types.INT,
			MAX_PARALLELISM, 1, 0);

		harness.open();
		harness.processElement(1, 0);
		harness.processElement(1, 1);
		OperatorSubtaskState state = harness.snapshot(0, 0L);
		harness.close();

		OperatorID operatorID = OperatorIDGenerator.fromUid(UID);
		OperatorState operatorState = new OperatorState(operatorID, 1, MAX_PARALLELISM);
		operatorState.putState(0, state);
		return operatorState;
	}

	private static <T> WindowOperator<Integer, Integer, ?, Void, ?> getLastOperator(DataStream<T> dataStream) {
		Transformation<T> transformation = dataStream.getTransformation();
		if (!(transformation instanceof OneInputTransformation)) {
			Assert.fail("This test only supports window operators");
		}

		OneInputTransformation<?, ?> oneInput = (OneInputTransformation<?, ?>) transformation;
		StreamOperator<?> operator = oneInput.getOperator();

		if (!(operator instanceof WindowOperator)) {
			Assert.fail("This test only supports window operators");
		}

		return (WindowOperator<Integer, Integer, ?, Void, ?>) operator;
	}

	@Nonnull
	private <OUT> List<OUT> readState(KeyedStateInputFormat<Integer, TimeWindow, OUT> format) throws IOException {
		KeyGroupRangeInputSplit split = format.createInputSplits(1)[0];
		List<OUT> data = new ArrayList<>();

		format.setRuntimeContext(new MockStreamingRuntimeContext(false, 1, 0));

		format.openInputFormat();
		format.open(split);

		while (!format.reachedEnd()) {
			data.add(format.nextRecord(null));
		}

		format.close();
		format.closeInputFormat();

		return data;
	}

	private static class IdentityKeySelector<T> implements KeySelector<T, T> {

		@Override
		public T getKey(T value) {
			return value;
		}
	}

	private static class MultiFireWindow extends ProcessWindowFunction<Integer, Integer, Integer, TimeWindow> {

		@Override
		public void process(Integer integer, Context context, Iterable<Integer> elements, Collector<Integer> out) throws Exception {
			Integer element = elements.iterator().next();
			context.globalState()
				.getReducingState(new ReducingStateDescriptor<>("per-key", new ReduceSum(), Types.INT))
				.add(element);

			context.windowState()
				.getReducingState(new ReducingStateDescriptor<>("per-pane", new ReduceSum(), Types.INT))
				.add(element);
		}
	}

	private static class MultiFireReaderFunction extends WindowReaderFunction<Integer, Tuple2<Integer, Integer>, Integer, TimeWindow> {

		@Override
		public void readWindow(Integer integer, Context<TimeWindow> context, Iterable<Integer> elements, Collector<Tuple2<Integer, Integer>> out) throws Exception {
			Integer perKey = context.globalState()
				.getReducingState(new ReducingStateDescriptor<>("per-key", new ReduceSum(), Types.INT))
				.get();

			Integer perPane = context.windowState()
				.getReducingState(new ReducingStateDescriptor<>("per-pane", new ReduceSum(), Types.INT))
				.get();

			out.collect(Tuple2.of(perKey, perPane));
		}
	}

	private static class AlwaysFireTrigger<W extends Window> extends Trigger<Object, W> {

		@Override
		public TriggerResult onElement(Object element, long timestamp, W window, TriggerContext ctx) throws Exception {
			return TriggerResult.FIRE;
		}

		@Override
		public TriggerResult onProcessingTime(long time, W window, TriggerContext ctx) throws Exception {
			return TriggerResult.FIRE;
		}

		@Override
		public TriggerResult onEventTime(long time, W window, TriggerContext ctx) throws Exception {
			return TriggerResult.FIRE;
		}

		@Override
		public void clear(W window, TriggerContext ctx) throws Exception {

		}
	}
}
