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

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.state.api.functions.KeyedStateReaderFunction;
import org.apache.flink.state.api.input.splits.KeyGroupRangeInputSplit;
import org.apache.flink.state.api.runtime.OperatorIDGenerator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamFlatMap;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;
import org.apache.flink.util.Collector;

import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

/**
 * Tests for keyed state input format.
 */
public class KeyedStateInputFormatTest {
	private static ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>("state", Types.INT);

	@Test
	public void testCreatePartitionedInputSplits() throws Exception {
		OperatorID operatorID = OperatorIDGenerator.fromUid("uid");

		OperatorSubtaskState state = createOperatorSubtaskState(new StreamFlatMap<>(new StatefulFunction()));
		OperatorState operatorState = new OperatorState(operatorID, 1, 128);
		operatorState.putState(0, state);

		KeyedStateInputFormat<?, ?> format = new KeyedStateInputFormat<>(operatorState, new MemoryStateBackend(), Types.INT, new ReaderFunction());
		KeyGroupRangeInputSplit[] splits = format.createInputSplits(4);
		Assert.assertEquals("Failed to properly partition operator state into input splits", 4, splits.length);
	}

	@Test
	public void testMaxParallelismRespected() throws Exception {
		OperatorID operatorID = OperatorIDGenerator.fromUid("uid");

		OperatorSubtaskState state = createOperatorSubtaskState(new StreamFlatMap<>(new StatefulFunction()));
		OperatorState operatorState = new OperatorState(operatorID, 1, 128);
		operatorState.putState(0, state);

		KeyedStateInputFormat<?, ?> format = new KeyedStateInputFormat<>(operatorState, new MemoryStateBackend(), Types.INT, new ReaderFunction());
		KeyGroupRangeInputSplit[] splits = format.createInputSplits(129);
		Assert.assertEquals("Failed to properly partition operator state into input splits", 128, splits.length);
	}

	@Test
	public void testReadState() throws Exception {
		OperatorID operatorID = OperatorIDGenerator.fromUid("uid");

		OperatorSubtaskState state = createOperatorSubtaskState(new StreamFlatMap<>(new StatefulFunction()));
		OperatorState operatorState = new OperatorState(operatorID, 1, 128);
		operatorState.putState(0, state);

		KeyedStateInputFormat<?, ?> format = new KeyedStateInputFormat<>(operatorState, new MemoryStateBackend(), Types.INT, new ReaderFunction());
		KeyGroupRangeInputSplit split = format.createInputSplits(1)[0];

		KeyedStateReaderFunction<Integer, Integer> userFunction = new ReaderFunction();

		List<Integer> data = readInputSplit(split, userFunction);

		Assert.assertEquals("Incorrect data read from input split", Arrays.asList(1, 2, 3), data);
	}

	@Test
	public void testReadMultipleOutputPerKey() throws Exception {
		OperatorID operatorID = OperatorIDGenerator.fromUid("uid");

		OperatorSubtaskState state = createOperatorSubtaskState(new StreamFlatMap<>(new StatefulFunction()));
		OperatorState operatorState = new OperatorState(operatorID, 1, 128);
		operatorState.putState(0, state);

		KeyedStateInputFormat<?, ?> format = new KeyedStateInputFormat<>(operatorState, new MemoryStateBackend(), Types.INT, new ReaderFunction());
		KeyGroupRangeInputSplit split = format.createInputSplits(1)[0];

		KeyedStateReaderFunction<Integer, Integer> userFunction = new DoubleReaderFunction();

		List<Integer> data = readInputSplit(split, userFunction);

		Assert.assertEquals("Incorrect data read from input split", Arrays.asList(1, 1, 2, 2, 3, 3), data);
	}

	@Test(expected = IOException.class)
	public void testInvalidProcessReaderFunctionFails() throws Exception {
		OperatorID operatorID = OperatorIDGenerator.fromUid("uid");

		OperatorSubtaskState state = createOperatorSubtaskState(new StreamFlatMap<>(new StatefulFunction()));
		OperatorState operatorState = new OperatorState(operatorID, 1, 128);
		operatorState.putState(0, state);

		KeyedStateInputFormat<?, ?> format = new KeyedStateInputFormat<>(operatorState, new MemoryStateBackend(), Types.INT, new ReaderFunction());
		KeyGroupRangeInputSplit split = format.createInputSplits(1)[0];

		KeyedStateReaderFunction<Integer, Integer> userFunction = new InvalidReaderFunction();

		readInputSplit(split, userFunction);

		Assert.fail("KeyedStateReaderFunction did not fail on invalid RuntimeContext use");
	}

	@Test
	public void testReadTime() throws Exception {
		OperatorID operatorID = OperatorIDGenerator.fromUid("uid");

		OperatorSubtaskState state = createOperatorSubtaskState(new KeyedProcessOperator<>(new StatefulFunctionWithTime()));
		OperatorState operatorState = new OperatorState(operatorID, 1, 128);
		operatorState.putState(0, state);

		KeyedStateInputFormat<?, ?> format = new KeyedStateInputFormat<>(operatorState, new MemoryStateBackend(), Types.INT, new TimeReaderFunction());
		KeyGroupRangeInputSplit split = format.createInputSplits(1)[0];

		KeyedStateReaderFunction<Integer, Integer> userFunction = new TimeReaderFunction();

		List<Integer> data = readInputSplit(split, userFunction);

		Assert.assertEquals("Incorrect data read from input split", Arrays.asList(1, 1, 2, 2, 3, 3), data);
	}

	@Nonnull
	private List<Integer> readInputSplit(KeyGroupRangeInputSplit split, KeyedStateReaderFunction<Integer, Integer> userFunction) throws IOException {
		KeyedStateInputFormat<Integer, Integer> format = new KeyedStateInputFormat<>(
			new OperatorState(OperatorIDGenerator.fromUid("uid"), 1, 4),
			new MemoryStateBackend(),
			Types.INT,
			userFunction);

		List<Integer> data = new ArrayList<>();

		format.setRuntimeContext(new MockStreamingRuntimeContext(false, 1, 0));

		format.openInputFormat();
		format.open(split);

		while (!format.reachedEnd()) {
			data.add(format.nextRecord(0));
		}

		format.close();
		format.closeInputFormat();

		data.sort(Comparator.comparingInt(id -> id));
		return data;
	}

	private OperatorSubtaskState createOperatorSubtaskState(OneInputStreamOperator<Integer, Void> operator) throws Exception {
		try (KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Void> testHarness =
				new KeyedOneInputStreamOperatorTestHarness<>(operator, id -> id, Types.INT, 128, 1, 0)) {

			testHarness.setup(VoidSerializer.INSTANCE);
			testHarness.open();

			testHarness.processElement(1, 0);
			testHarness.processElement(2, 0);
			testHarness.processElement(3, 0);

			return testHarness.snapshot(0, 0);
		}
	}

	static class ReaderFunction extends KeyedStateReaderFunction<Integer, Integer> {
		ValueState<Integer> state;

		@Override
		public void open(Configuration parameters) {
			state = getRuntimeContext().getState(stateDescriptor);
		}

		@Override
		public void readKey(Integer key, KeyedStateReaderFunction.Context ctx, Collector<Integer> out) throws Exception {
			out.collect(state.value());
		}
	}

	static class DoubleReaderFunction extends KeyedStateReaderFunction<Integer, Integer> {
		ValueState<Integer> state;

		@Override
		public void open(Configuration parameters) {
			state = getRuntimeContext().getState(stateDescriptor);
		}

		@Override
		public void readKey(Integer key, KeyedStateReaderFunction.Context ctx, Collector<Integer> out) throws Exception {
			out.collect(state.value());
			out.collect(state.value());
		}
	}

	static class InvalidReaderFunction extends KeyedStateReaderFunction<Integer, Integer> {

		@Override
		public void open(Configuration parameters) {
			getRuntimeContext().getState(stateDescriptor);
		}

		@Override
		public void readKey(Integer key, KeyedStateReaderFunction.Context ctx, Collector<Integer> out) throws Exception {
			ValueState<Integer> state = getRuntimeContext().getState(stateDescriptor);
			out.collect(state.value());
		}
	}

	static class StatefulFunction extends RichFlatMapFunction<Integer, Void> {
		ValueState<Integer> state;

		@Override
		public void open(Configuration parameters) {
			state = getRuntimeContext().getState(stateDescriptor);
		}

		@Override
		public void flatMap(Integer value, Collector<Void> out) throws Exception {
			state.update(value);
		}
	}

	static class StatefulFunctionWithTime extends KeyedProcessFunction<Integer, Integer, Void> {
		ValueState<Integer> state;

		@Override
		public void open(Configuration parameters) {
			state = getRuntimeContext().getState(stateDescriptor);
		}

		@Override
		public void processElement(Integer value, Context ctx, Collector<Void> out) throws Exception {
			state.update(value);
			ctx.timerService().registerEventTimeTimer(value);
			ctx.timerService().registerProcessingTimeTimer(value);
		}
	}

	static class TimeReaderFunction extends KeyedStateReaderFunction<Integer, Integer> {
		ValueState<Integer> state;

		@Override
		public void open(Configuration parameters) {
			state = getRuntimeContext().getState(stateDescriptor);
		}

		@Override
		public void readKey(Integer key, KeyedStateReaderFunction.Context ctx, Collector<Integer> out) throws Exception {
			Set<Long> eventTimers = ctx.registeredEventTimeTimers();
			Assert.assertEquals("Each key should have exactly one event timer for key " + key, 1, eventTimers.size());

			out.collect(eventTimers.iterator().next().intValue());

			Set<Long> procTimers = ctx.registeredProcessingTimeTimers();
			Assert.assertEquals("Each key should have exactly one processing timer for key " + key, 1, procTimers.size());

			out.collect(procTimers.iterator().next().intValue());
		}
	}
}
