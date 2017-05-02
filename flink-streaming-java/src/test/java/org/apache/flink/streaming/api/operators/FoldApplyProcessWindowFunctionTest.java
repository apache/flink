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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.api.common.state.FoldingState;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.base.ByteSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.FoldApplyProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.FoldApplyProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.SourceTransformation;
import org.apache.flink.streaming.api.transformations.StreamTransformation;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.AccumulatingProcessingTimeWindowOperator;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalIterableProcessAllWindowFunction;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalIterableProcessWindowFunction;
import org.apache.flink.util.Collector;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class FoldApplyProcessWindowFunctionTest {

	/**
	 * Tests that the FoldWindowFunction gets the output type serializer set by the
	 * StreamGraphGenerator and checks that the FoldWindowFunction computes the correct result.
	 */
	@Test
	public void testFoldWindowFunctionOutputTypeConfigurable() throws Exception{
		StreamExecutionEnvironment env = new DummyStreamExecutionEnvironment();

		List<StreamTransformation<?>> transformations = new ArrayList<>();

		int initValue = 1;

		FoldApplyProcessWindowFunction<Integer, TimeWindow, Integer, Integer, Integer> foldWindowFunction = new FoldApplyProcessWindowFunction<>(
			initValue,
			new FoldFunction<Integer, Integer>() {
				@Override
				public Integer fold(Integer accumulator, Integer value) throws Exception {
					return accumulator + value;
				}

			},
			new ProcessWindowFunction<Integer, Integer, Integer, TimeWindow>() {
				@Override
				public void process(Integer integer,
									Context context,
									Iterable<Integer> input,
									Collector<Integer> out) throws Exception {
					for (Integer in: input) {
						out.collect(in);
					}
				}
			},
			BasicTypeInfo.INT_TYPE_INFO
		);

		AccumulatingProcessingTimeWindowOperator<Integer, Integer, Integer> windowOperator = new AccumulatingProcessingTimeWindowOperator<>(
			new InternalIterableProcessWindowFunction<>(foldWindowFunction),
			new KeySelector<Integer, Integer>() {
				private static final long serialVersionUID = -7951310554369722809L;

				@Override
				public Integer getKey(Integer value) throws Exception {
					return value;
				}
			},
			IntSerializer.INSTANCE,
			IntSerializer.INSTANCE,
			3000,
			3000
		);

		SourceFunction<Integer> sourceFunction = new SourceFunction<Integer>(){

			private static final long serialVersionUID = 8297735565464653028L;

			@Override
			public void run(SourceContext<Integer> ctx) throws Exception {

			}

			@Override
			public void cancel() {

			}
		};

		SourceTransformation<Integer> source = new SourceTransformation<>("", new StreamSource<>(sourceFunction), BasicTypeInfo.INT_TYPE_INFO, 1);

		transformations.add(new OneInputTransformation<>(source, "test", windowOperator, BasicTypeInfo.INT_TYPE_INFO, 1));

		StreamGraph streamGraph = StreamGraphGenerator.generate(env, transformations);

		List<Integer> result = new ArrayList<>();
		List<Integer> input = new ArrayList<>();
		List<Integer> expected = new ArrayList<>();

		input.add(1);
		input.add(2);
		input.add(3);

		for (int value : input) {
			initValue += value;
		}

		expected.add(initValue);

		FoldApplyProcessWindowFunction<Integer, TimeWindow, Integer, Integer, Integer>.Context ctx = foldWindowFunction.new Context() {
			@Override
			public TimeWindow window() {
				return new TimeWindow(0, 1);
			}

			@Override
			public long currentProcessingTime() {
				return 0;
			}

			@Override
			public long currentWatermark() {
				return 0;
			}

			@Override
			public KeyedStateStore windowState() {
				return new DummyKeyedStateStore();
			}

			@Override
			public KeyedStateStore globalState() {
				return new DummyKeyedStateStore();
			}
		};

		foldWindowFunction.open(new Configuration());

		foldWindowFunction.process(0, ctx, input, new ListCollector<>(result));

		Assert.assertEquals(expected, result);
	}

		/**
	 * Tests that the FoldWindowFunction gets the output type serializer set by the
	 * StreamGraphGenerator and checks that the FoldWindowFunction computes the correct result.
	 */
	@Test
	public void testFoldAllWindowFunctionOutputTypeConfigurable() throws Exception{
		StreamExecutionEnvironment env = new DummyStreamExecutionEnvironment();

		List<StreamTransformation<?>> transformations = new ArrayList<>();

		int initValue = 1;

		FoldApplyProcessAllWindowFunction<TimeWindow, Integer, Integer, Integer> foldWindowFunction = new FoldApplyProcessAllWindowFunction<>(
			initValue,
			new FoldFunction<Integer, Integer>() {
				@Override
				public Integer fold(Integer accumulator, Integer value) throws Exception {
					return accumulator + value;
				}

			},
			new ProcessAllWindowFunction<Integer, Integer, TimeWindow>() {
				@Override
				public void process(Context context,
									Iterable<Integer> input,
									Collector<Integer> out) throws Exception {
					for (Integer in: input) {
						out.collect(in);
					}
				}
			},
			BasicTypeInfo.INT_TYPE_INFO
		);

		AccumulatingProcessingTimeWindowOperator<Byte, Integer, Integer> windowOperator = new AccumulatingProcessingTimeWindowOperator<>(
			new InternalIterableProcessAllWindowFunction<>(foldWindowFunction),
			new KeySelector<Integer, Byte>() {
				private static final long serialVersionUID = -7951310554369722809L;

				@Override
				public Byte getKey(Integer value) throws Exception {
					return 0;
				}
			},
			ByteSerializer.INSTANCE,
			IntSerializer.INSTANCE,
			3000,
			3000
		);

		SourceFunction<Integer> sourceFunction = new SourceFunction<Integer>(){

			private static final long serialVersionUID = 8297735565464653028L;

			@Override
			public void run(SourceContext<Integer> ctx) throws Exception {

			}

			@Override
			public void cancel() {

			}
		};

		SourceTransformation<Integer> source = new SourceTransformation<>("", new StreamSource<>(sourceFunction), BasicTypeInfo.INT_TYPE_INFO, 1);

		transformations.add(new OneInputTransformation<>(source, "test", windowOperator, BasicTypeInfo.INT_TYPE_INFO, 1));

		StreamGraph streamGraph = StreamGraphGenerator.generate(env, transformations);

		List<Integer> result = new ArrayList<>();
		List<Integer> input = new ArrayList<>();
		List<Integer> expected = new ArrayList<>();

		input.add(1);
		input.add(2);
		input.add(3);

		for (int value : input) {
			initValue += value;
		}

		expected.add(initValue);

		FoldApplyProcessAllWindowFunction<TimeWindow, Integer, Integer, Integer>.Context ctx = foldWindowFunction.new Context() {
			@Override
			public TimeWindow window() {
				return new TimeWindow(0, 1);
			}

			@Override
			public KeyedStateStore windowState() {
				return new DummyKeyedStateStore();
			}

			@Override
			public KeyedStateStore globalState() {
				return new DummyKeyedStateStore();
			}
		};

		foldWindowFunction.open(new Configuration());

		foldWindowFunction.process(ctx, input, new ListCollector<>(result));

		Assert.assertEquals(expected, result);
	}

	public static class DummyKeyedStateStore implements KeyedStateStore {

		@Override
		public <T> ValueState<T> getState(ValueStateDescriptor<T> stateProperties) {
			return null;
		}

		@Override
		public <T> ListState<T> getListState(ListStateDescriptor<T> stateProperties) {
			return null;
		}

		@Override
		public <T> ReducingState<T> getReducingState(ReducingStateDescriptor<T> stateProperties) {
			return null;
		}

		@Override
		public <T, ACC> FoldingState<T, ACC> getFoldingState(FoldingStateDescriptor<T, ACC> stateProperties) {
			return null;
		}

		@Override
		public <UK, UV> MapState<UK, UV> getMapState(MapStateDescriptor<UK, UV> stateProperties) {
			return null;
		}
	}

	public static class DummyStreamExecutionEnvironment extends StreamExecutionEnvironment {

		@Override
		public JobExecutionResult execute(String jobName) throws Exception {
			return null;
		}
	}
}
