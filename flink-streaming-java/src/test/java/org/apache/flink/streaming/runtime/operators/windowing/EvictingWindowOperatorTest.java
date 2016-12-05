/**
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
package org.apache.flink.streaming.runtime.operators.windowing;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeInfoParser;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ReduceIterableWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalIterableWindowFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecordSerializer;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.util.Collector;
import org.junit.Assert;
import org.junit.Test;

import java.util.Comparator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class EvictingWindowOperatorTest {

	// For counting if close() is called the correct number of times on the SumReducer

	@Test
	@SuppressWarnings("unchecked")
	public void testCountTrigger() throws Exception {

		final int WINDOW_SIZE = 4;
		final int WINDOW_SLIDE = 2;

		TypeInformation<Tuple2<String, Integer>> inputType = TypeInfoParser.parse("Tuple2<String, Integer>");


		ListStateDescriptor<StreamRecord<Tuple2<String, Integer>>> stateDesc = new ListStateDescriptor<>("window-contents",
			new StreamRecordSerializer<>(inputType.createSerializer(new ExecutionConfig())));


		EvictingWindowOperator<String, Tuple2<String, Integer>, Tuple2<String, Integer>, GlobalWindow> operator = new EvictingWindowOperator<>(
				GlobalWindows.create(),
				new GlobalWindow.Serializer(),
				new TupleKeySelector(),
				BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
				stateDesc,
				new InternalIterableWindowFunction<>(new ReduceIterableWindowFunction<String, GlobalWindow, Tuple2<String, Integer>>(new SumReducer())),
				CountTrigger.of(WINDOW_SLIDE),
				CountEvictor.of(WINDOW_SIZE),
				0);


		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>> testHarness =
				new KeyedOneInputStreamOperatorTestHarness<>(operator, new TupleKeySelector(), BasicTypeInfo.STRING_TYPE_INFO);

		long initialTime = 0L;
		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.open();

		// The global window actually ignores these timestamps...

		// add elements out-of-order
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), initialTime + 3000));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), initialTime + 3999));

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), initialTime + 20));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), initialTime));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), initialTime + 999));

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), initialTime + 1998));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), initialTime + 1999));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), initialTime + 1000));



		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 2), Long.MAX_VALUE));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 4), Long.MAX_VALUE));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 2), Long.MAX_VALUE));

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new ResultSortComparator());

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), initialTime + 10999));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), initialTime + 1000));

		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 4), Long.MAX_VALUE));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 4), Long.MAX_VALUE));

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new ResultSortComparator());

		testHarness.close();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testCountTriggerWithApply() throws Exception {
		AtomicInteger closeCalled = new AtomicInteger(0);

		final int WINDOW_SIZE = 4;
		final int WINDOW_SLIDE = 2;

		TypeInformation<Tuple2<String, Integer>> inputType = TypeInfoParser.parse("Tuple2<String, Integer>");


		ListStateDescriptor<StreamRecord<Tuple2<String, Integer>>> stateDesc = new ListStateDescriptor<>("window-contents",
			new StreamRecordSerializer<>(inputType.createSerializer(new ExecutionConfig())));


		EvictingWindowOperator<String, Tuple2<String, Integer>, Tuple2<String, Integer>, GlobalWindow> operator = new EvictingWindowOperator<>(
			GlobalWindows.create(),
			new GlobalWindow.Serializer(),
			new TupleKeySelector(),
			BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
			stateDesc,
			new InternalIterableWindowFunction<>(new RichSumReducer<GlobalWindow>(closeCalled)),
			CountTrigger.of(WINDOW_SLIDE),
			CountEvictor.of(WINDOW_SIZE),
			0);


		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>> testHarness =
				new KeyedOneInputStreamOperatorTestHarness<>(operator, new TupleKeySelector(), BasicTypeInfo.STRING_TYPE_INFO);

		long initialTime = 0L;
		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.open();

		// The global window actually ignores these timestamps...

		// add elements out-of-order
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), initialTime + 3000));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), initialTime + 3999));

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), initialTime + 20));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), initialTime));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), initialTime + 999));

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), initialTime + 1998));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), initialTime + 1999));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), initialTime + 1000));



		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 2), Long.MAX_VALUE));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 4), Long.MAX_VALUE));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 2), Long.MAX_VALUE));

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new ResultSortComparator());

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), initialTime + 10999));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), initialTime + 1000));

		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 4), Long.MAX_VALUE));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 4), Long.MAX_VALUE));

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new ResultSortComparator());

		testHarness.close();

		Assert.assertEquals("Close was not called.", 1, closeCalled.get());
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testTumblingWindowWithApply() throws Exception {
		AtomicInteger closeCalled = new AtomicInteger(0);

		final int WINDOW_SIZE = 4;

		TypeInformation<Tuple2<String, Integer>> inputType = TypeInfoParser.parse("Tuple2<String, Integer>");

		ListStateDescriptor<StreamRecord<Tuple2<String, Integer>>> stateDesc = new ListStateDescriptor<>("window-contents",
			new StreamRecordSerializer<>(inputType.createSerializer(new ExecutionConfig())));

		EvictingWindowOperator<String, Tuple2<String, Integer>, Tuple2<String, Integer>, TimeWindow> operator = new EvictingWindowOperator<>(
			TumblingEventTimeWindows.of(Time.of(WINDOW_SIZE, TimeUnit.SECONDS)),
			new TimeWindow.Serializer(),
			new TupleKeySelector(),
			BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
			stateDesc,
			new InternalIterableWindowFunction<>(new RichSumReducer<TimeWindow>(closeCalled)),
			EventTimeTrigger.create(),
			CountEvictor.of(WINDOW_SIZE),
			0);

		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>> testHarness =
				new KeyedOneInputStreamOperatorTestHarness<>(operator, new TupleKeySelector(), BasicTypeInfo.STRING_TYPE_INFO);

		long initialTime = 0L;

		testHarness.open();

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), initialTime + 10));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), initialTime + 100));

		testHarness.processWatermark(new Watermark(1999));

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), initialTime + 1997));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), initialTime + 1998));

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), initialTime + 2310)); // not late but more than 4
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), initialTime + 2310));

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), initialTime + 2310));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), initialTime + 2310));

		testHarness.processWatermark(new Watermark(3999));											 // now is the evictor

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
		expectedOutput.add(new Watermark(1999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 4), 3999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 2), 3999));
		expectedOutput.add(new Watermark(3999));


		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(),
			new EvictingWindowOperatorTest.ResultSortComparator());
		testHarness.close();
	}

		// ------------------------------------------------------------------------
	//  UDFs
	// ------------------------------------------------------------------------

	public static class SumReducer implements ReduceFunction<Tuple2<String, Integer>> {
		private static final long serialVersionUID = 1L;


		@Override
		public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1,
				Tuple2<String, Integer> value2) throws Exception {
			return new Tuple2<>(value2.f0, value1.f1 + value2.f1);
		}
	}

	public static class RichSumReducer<W extends Window> extends RichWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, W> {
		private static final long serialVersionUID = 1L;

		private boolean openCalled = false;

		private AtomicInteger closeCalled = new AtomicInteger(0);

		public RichSumReducer(AtomicInteger closeCalled) {
			this.closeCalled = closeCalled;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			openCalled = true;
		}

		@Override
		public void close() throws Exception {
			super.close();
			closeCalled.incrementAndGet();
		}

		@Override
		public void apply(String key,
			W window,
			Iterable<Tuple2<String, Integer>> input,
			Collector<Tuple2<String, Integer>> out) throws Exception {

			if (!openCalled) {
				Assert.fail("Open was not called");
			}
			int sum = 0;

			for (Tuple2<String, Integer> t: input) {
				sum += t.f1;
			}
			out.collect(new Tuple2<>(key, sum));

		}

	}

	@SuppressWarnings("unchecked")
	private static class ResultSortComparator implements Comparator<Object> {
		@Override
		public int compare(Object o1, Object o2) {
			if (o1 instanceof Watermark || o2 instanceof Watermark) {
				return 0;
			} else {
				StreamRecord<Tuple2<String, Integer>> sr0 = (StreamRecord<Tuple2<String, Integer>>) o1;
				StreamRecord<Tuple2<String, Integer>> sr1 = (StreamRecord<Tuple2<String, Integer>>) o2;
				if (sr0.getTimestamp() != sr1.getTimestamp()) {
					return (int) (sr0.getTimestamp() - sr1.getTimestamp());
				}
				int comparison = sr0.getValue().f0.compareTo(sr1.getValue().f0);
				if (comparison != 0) {
					return comparison;
				} else {
					return sr0.getValue().f1 - sr1.getValue().f1;
				}
			}
		}
	}

	private static class TupleKeySelector implements KeySelector<Tuple2<String, Integer>, String> {
		private static final long serialVersionUID = 1L;

		@Override
		public String getKey(Tuple2<String, Integer> value) throws Exception {
			return value.f0;
		}
	}

}
