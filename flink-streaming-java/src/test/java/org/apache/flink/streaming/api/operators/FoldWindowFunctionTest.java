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
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.FoldWindowFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.SourceTransformation;
import org.apache.flink.streaming.api.transformations.StreamTransformation;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.AccumulatingProcessingTimeWindowOperator;
import org.junit.Test;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.List;

public class FoldWindowFunctionTest {

	/**
	 * Tests that the FoldWindowFunction gets the output type serializer set by the
	 * StreamGraphGenerator and checks that the FoldWindowFunction computes the correct result.
	 */
	@Test
	public void testFoldWindowFunctionOutputTypeConfigurable() throws Exception{
		StreamExecutionEnvironment env = new DummyStreamExecutionEnvironment();

		List<StreamTransformation<?>> transformations = new ArrayList<>();

		int initValue = 1;

		FoldWindowFunction<Integer, TimeWindow, Integer, Integer> foldWindowFunction = new FoldWindowFunction<>(
			initValue,
			new FoldFunction<Integer, Integer>() {
				private static final long serialVersionUID = -4849549768529720587L;

				@Override
				public Integer fold(Integer accumulator, Integer value) throws Exception {
					return accumulator + value;
				}
			}
		);

		AccumulatingProcessingTimeWindowOperator<Integer, Integer, Integer> windowOperator = new AccumulatingProcessingTimeWindowOperator<>(
			foldWindowFunction,
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

		SourceTransformation<Integer> source = new SourceTransformation<>("", new StreamSource<Integer>(sourceFunction), BasicTypeInfo.INT_TYPE_INFO, 1);

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

		foldWindowFunction.apply(0, new TimeWindow(0, 1), input, new ListCollector<Integer>(result));

		Assert.assertEquals(expected, result);
	}

	public static class DummyStreamExecutionEnvironment extends StreamExecutionEnvironment {

		@Override
		public JobExecutionResult execute(String jobName) throws Exception {
			return null;
		}
	}
}
