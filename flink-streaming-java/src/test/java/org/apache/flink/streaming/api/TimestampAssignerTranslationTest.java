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
package org.apache.flink.streaming.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import javax.annotation.Nullable;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OutputTypeConfigurable;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.operators.TimestampsAndPeriodicWatermarksOperator;
import org.apache.flink.streaming.runtime.operators.TimestampsAndPunctuatedWatermarksOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.Assert;
import org.junit.Test;

/**
 * These tests verify that the api calls on {@link DataStream} correctly instantiate
 * timestamp/watermark assignment operators.
 *
 * <p>We also create a test harness and push one element into the operator to verify
 * that we get some output.
 */
@SuppressWarnings("serial")
public class TimestampAssignerTranslationTest {

	/**
	 * When the upstream operator has the default parallelism it has parallelism {@code -1}. This
	 * test makes sure that code API code can deal with that.
	 */
	@Test
	public void testPunctuatedAssignerWorksWithDefaultParallelism() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(ExecutionConfig.PARALLELISM_DEFAULT);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		DataStream<Tuple2<String, Integer>> source =
				env.fromElements(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

		SingleOutputStreamOperator<Tuple2<String, Integer>> assigner = source
				.map(new IdentityMap())
				.setParallelism(ExecutionConfig.PARALLELISM_DEFAULT)
				.assignTimestampsAndWatermarks(new DummyPunctuatedAssigner());

		OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>> transform =
				(OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>>) assigner.getTransformation();

		assertEquals(ExecutionConfig.PARALLELISM_DEFAULT, transform.getParallelism());

		OneInputStreamOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> operator = transform.getOperator();
		assertTrue(operator instanceof TimestampsAndPunctuatedWatermarksOperator);
		TimestampsAndPunctuatedWatermarksOperator<Tuple2<String, Integer>> assignerOperator =
				(TimestampsAndPunctuatedWatermarksOperator<Tuple2<String, Integer>>) operator;

		processElementAndEnsureOutput(assignerOperator, new Tuple2<>("hello", 1));
	}

	/**
	 * When the upstream operator has the default parallelism it has parallelism {@code -1}. This
	 * test makes sure that code API code can deal with that.
	 */
	@Test
	public void testPeriodicAssignerWorksWithDefaultParallelism() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(ExecutionConfig.PARALLELISM_DEFAULT);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		DataStream<Tuple2<String, Integer>> source =
				env.fromElements(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

		SingleOutputStreamOperator<Tuple2<String, Integer>> assigner = source
				.map(new IdentityMap())
				.setParallelism(ExecutionConfig.PARALLELISM_DEFAULT)
				.assignTimestampsAndWatermarks(new DummyPeriodicAssigner());

		OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>> transform =
				(OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>>) assigner.getTransformation();

		assertEquals(ExecutionConfig.PARALLELISM_DEFAULT, transform.getParallelism());

		OneInputStreamOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> operator = transform.getOperator();
		assertTrue(operator instanceof TimestampsAndPeriodicWatermarksOperator);
		TimestampsAndPeriodicWatermarksOperator<Tuple2<String, Integer>> assignerOperator =
				(TimestampsAndPeriodicWatermarksOperator<Tuple2<String, Integer>>) operator;

		processElementAndEnsureOutput(assignerOperator, new Tuple2<>("hello", 1));
	}

	@Test
	public void testPunctuatedAssignerPicksUpUpstreamParallelism() throws Exception {
		final int parallelism = 13;

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		DataStream<Tuple2<String, Integer>> source =
				env.fromElements(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

		SingleOutputStreamOperator<Tuple2<String, Integer>> assigner = source
				.map(new IdentityMap())
				.setParallelism(parallelism)
				.assignTimestampsAndWatermarks(new DummyPunctuatedAssigner());

		OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>> transform =
				(OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>>) assigner.getTransformation();

		assertEquals(parallelism, transform.getParallelism());

		OneInputStreamOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> operator = transform.getOperator();
		Assert.assertTrue(operator instanceof TimestampsAndPunctuatedWatermarksOperator);
		TimestampsAndPunctuatedWatermarksOperator<Tuple2<String, Integer>> assignerOperator =
				(TimestampsAndPunctuatedWatermarksOperator<Tuple2<String, Integer>>) operator;

		processElementAndEnsureOutput(assignerOperator, new Tuple2<>("hello", 1));
	}

	@Test
	public void testPeriodicAssignerPicksUpUpstreamParallelism() throws Exception {
		final int parallelism = 13;

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		DataStream<Tuple2<String, Integer>> source =
				env.fromElements(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

		SingleOutputStreamOperator<Tuple2<String, Integer>> assigner = source
				.map(new IdentityMap())
				.setParallelism(parallelism)
				.assignTimestampsAndWatermarks(new DummyPeriodicAssigner());

		OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>> transform =
				(OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>>) assigner.getTransformation();

		assertEquals(parallelism, transform.getParallelism());

		OneInputStreamOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> operator = transform.getOperator();
		Assert.assertTrue(operator instanceof TimestampsAndPeriodicWatermarksOperator);
		TimestampsAndPeriodicWatermarksOperator<Tuple2<String, Integer>> assignerOperator =
				(TimestampsAndPeriodicWatermarksOperator<Tuple2<String, Integer>>) operator;

		processElementAndEnsureOutput(assignerOperator, new Tuple2<>("hello", 1));
	}

	/**
	 * Ensure that we get some output from the given operator when pushing in an element and
	 * setting watermark and processing time to {@code Long.MAX_VALUE}.
	 */
	private static <IN, OUT> void processElementAndEnsureOutput(
			OneInputStreamOperator<IN, OUT> operator,
			IN element) throws Exception {

		OneInputStreamOperatorTestHarness<IN, OUT> testHarness =
				new OneInputStreamOperatorTestHarness<>(operator);

		if (operator instanceof OutputTypeConfigurable) {
			// use a dummy type since window functions just need the ExecutionConfig
			// this is also only needed for Fold, which we're getting rid off soon.
			((OutputTypeConfigurable) operator).setOutputType(BasicTypeInfo.STRING_TYPE_INFO, new ExecutionConfig());
		}

		testHarness.open();

		testHarness.setProcessingTime(0);
		testHarness.processWatermark(Long.MIN_VALUE);

		testHarness.processElement(new StreamRecord<>(element, 0));

		// provoke any processing-time/event-time triggers
		testHarness.setProcessingTime(Long.MAX_VALUE);
		testHarness.processWatermark(Long.MAX_VALUE);

		// we at least get the record and the passed-through Long.MAX_VALUE watermark
		assertTrue(testHarness.getOutput().size() >= 2);

		testHarness.close();
	}


	private static class IdentityMap implements MapFunction<Tuple2<String,Integer>, Tuple2<String, Integer>> {
		@Override
		public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
			return value;
		}
	}

	private static class DummyPunctuatedAssigner implements AssignerWithPunctuatedWatermarks<Tuple2<String, Integer>> {
		@Nullable
		@Override
		public Watermark checkAndGetNextWatermark(
				Tuple2<String, Integer> lastElement, long extractedTimestamp) {
			return null;
		}

		@Override
		public long extractTimestamp(
				Tuple2<String, Integer> element,
				long previousElementTimestamp) {
			return 0;
		}
	}

	private static class DummyPeriodicAssigner implements AssignerWithPeriodicWatermarks<Tuple2<String, Integer>> {
		@Override
		public long extractTimestamp(Tuple2<String, Integer> element,
				long previousElementTimestamp) {
			return 0;
		}

		@Nullable
		@Override
		public Watermark getCurrentWatermark() {
			return null;
		}
	}
}
