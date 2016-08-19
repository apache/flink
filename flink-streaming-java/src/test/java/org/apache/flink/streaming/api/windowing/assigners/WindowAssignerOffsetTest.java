/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.windowing.assigners;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.array.StringArraySerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeInfoParser;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.WindowingTestHarnessTest;
import org.apache.flink.streaming.util.WindowingTestHarness;
import org.junit.Test;

import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

/**
 * Created by aditi on 01/08/16.
 */
public class WindowAssignerOffsetTest {


	@Test
	public void testProcessingTimeTumblingWindowsPositiveOffset() throws Exception {
		final int WINDOW_SIZE = 100;
		final long offset = 20;

		TypeInformation<Tuple2<String, Integer>> inputType = TypeInfoParser.parse("Tuple2<String, Integer>");

		TumblingProcessingTimeWindows windowAssigner = TumblingProcessingTimeWindows.of(Time.milliseconds(WINDOW_SIZE), offset);

		WindowingTestHarness<String, Tuple2<String, Integer>, TimeWindow> testHarness = new WindowingTestHarness<>(
			new ExecutionConfig(),
			windowAssigner,
			BasicTypeInfo.STRING_TYPE_INFO,
			inputType,
			new WindowAssignerOffsetTest.TupleKeySelector(),
			ProcessingTimeTrigger.create(),
			0);

		testHarness.setProcessingTime(3);

		// timestamp is ignored in processing time
		testHarness.processElement(new Tuple2<>("key2", 1), Long.MAX_VALUE);
		testHarness.processElement(new Tuple2<>("key2", 1), 7000);
		testHarness.processElement(new Tuple2<>("key2", 1), 7000);

		testHarness.setProcessingTime(100);

		testHarness.addExpectedElement(new Tuple2<>("key2", 1), 119);
		testHarness.addExpectedElement(new Tuple2<>("key2", 1), 119);
		testHarness.addExpectedElement(new Tuple2<>("key2", 1), 119);

		testHarness.compareActualToExpectedOutput("Output was not correct.");

		testHarness.processElement(new Tuple2<>("key1", 1), 8000);
		testHarness.processElement(new Tuple2<>("key1", 1), 7000);

		testHarness.setProcessingTime(200);

		testHarness.addExpectedElement(new Tuple2<>("key1", 1), 219);
		testHarness.addExpectedElement(new Tuple2<>("key1", 1), 219);

		testHarness.compareActualToExpectedOutput("Output was not correct.");

		testHarness.close();
	}

	@Test
	public void testProcessingTimeTumblingWindowsNegativeOffset() throws Exception {
		final int WINDOW_SIZE = 100;
		final long offset = -20;

		TypeInformation<Tuple2<String, Integer>> inputType = TypeInfoParser.parse("Tuple2<String, Integer>");

		TumblingProcessingTimeWindows windowAssigner = TumblingProcessingTimeWindows.of(Time.milliseconds(WINDOW_SIZE), offset);

		WindowingTestHarness<String, Tuple2<String, Integer>, TimeWindow> testHarness = new WindowingTestHarness<>(
			new ExecutionConfig(),
			windowAssigner,
			BasicTypeInfo.STRING_TYPE_INFO,
			inputType,
			new WindowAssignerOffsetTest.TupleKeySelector(),
			ProcessingTimeTrigger.create(),
			0);

		testHarness.setProcessingTime(3);

		// timestamp is ignored in processing time
		testHarness.processElement(new Tuple2<>("key2", 1), Long.MAX_VALUE);
		testHarness.processElement(new Tuple2<>("key2", 1), 7000);
		testHarness.processElement(new Tuple2<>("key2", 1), 7000);

		testHarness.setProcessingTime(100);

		testHarness.addExpectedElement(new Tuple2<>("key2", 1), 79);
		testHarness.addExpectedElement(new Tuple2<>("key2", 1), 79);
		testHarness.addExpectedElement(new Tuple2<>("key2", 1), 79);

		testHarness.compareActualToExpectedOutput("Output was not correct.");

		testHarness.processElement(new Tuple2<>("key1", 1), 8000);
		testHarness.processElement(new Tuple2<>("key1", 1), 7000);

		testHarness.setProcessingTime(200);

		testHarness.addExpectedElement(new Tuple2<>("key1", 1), 179);
		testHarness.addExpectedElement(new Tuple2<>("key1", 1), 179);

		testHarness.compareActualToExpectedOutput("Output was not correct.");

		testHarness.close();
	}


	@Test
	public void testProcessingTimeTumblingWindowsTimeZoneOffset() throws Exception {
		final int WINDOW_SIZE = 1;
		final TimeZone offset = TimeZone.getTimeZone("Asia/Kolkata");

		TypeInformation<Tuple2<String, Integer>> inputType = TypeInfoParser.parse("Tuple2<String, Integer>");

		TumblingProcessingTimeWindows windowAssigner = TumblingProcessingTimeWindows.of(Time.days(WINDOW_SIZE), offset);

		WindowingTestHarness<String, Tuple2<String, Integer>, TimeWindow> testHarness = new WindowingTestHarness<>(
			new ExecutionConfig(),
			windowAssigner,
			BasicTypeInfo.STRING_TYPE_INFO,
			inputType,
			new WindowAssignerOffsetTest.TupleKeySelector(),
			ProcessingTimeTrigger.create(),
			0);

		testHarness.setProcessingTime(3);

		// timestamp is ignored in processing time
		testHarness.processElement(new Tuple2<>("key2", 1), Long.MAX_VALUE);
		testHarness.processElement(new Tuple2<>("key2", 1), 7000);
		testHarness.processElement(new Tuple2<>("key2", 1), 7000);

		testHarness.setProcessingTime(86400000);

		testHarness.addExpectedElement(new Tuple2<>("key2", 1), 106199999);
		testHarness.addExpectedElement(new Tuple2<>("key2", 1), 106199999);
		testHarness.addExpectedElement(new Tuple2<>("key2", 1), 106199999);

		testHarness.compareActualToExpectedOutput("Output was not correct.");

		testHarness.processElement(new Tuple2<>("key1", 1), 8000);
		testHarness.processElement(new Tuple2<>("key1", 1), 7000);

		testHarness.setProcessingTime(172800000);

		testHarness.addExpectedElement(new Tuple2<>("key1", 1), 192599999);
		testHarness.addExpectedElement(new Tuple2<>("key1", 1), 192599999);

		testHarness.compareActualToExpectedOutput("Output was not correct.");

		testHarness.close();
	}

	@Test
	public void testProcessingTimeSlidingWindowsPositiveOffset() throws Exception {
		final int WINDOW_SIZE = 3;
		final int WINDOW_SLIDE = 1;
		final long offset = 200;

		TypeInformation<Tuple2<String, Integer>> inputType = TypeInfoParser.parse("Tuple2<String, Integer>");

		SlidingProcessingTimeWindows windowAssigner = SlidingProcessingTimeWindows.of(Time.of(WINDOW_SIZE, TimeUnit.SECONDS), Time.of(WINDOW_SLIDE, TimeUnit.SECONDS), offset);

		WindowingTestHarness<String, Tuple2<String, Integer>, TimeWindow> testHarness = new WindowingTestHarness<>(
			new ExecutionConfig(),
			windowAssigner,
			BasicTypeInfo.STRING_TYPE_INFO,
			inputType,
			new WindowAssignerOffsetTest.TupleKeySelector(),
			ProcessingTimeTrigger.create(),
			0);

		testHarness.setProcessingTime(3);

		// timestamp is ignored in processing time
		testHarness.processElement(new Tuple2<>("key2", 1), Long.MAX_VALUE);

		testHarness.setProcessingTime(1000);

		testHarness.addExpectedElement(new Tuple2<>("key2", 1), 1199);

		testHarness.compareActualToExpectedOutput("Output was not correct.");


		testHarness.processElement(new Tuple2<>("key2", 1), Long.MAX_VALUE);
		testHarness.processElement(new Tuple2<>("key2", 1), Long.MAX_VALUE);

		testHarness.setProcessingTime(2000);

		testHarness.addExpectedElement(new Tuple2<>("key2", 1), 2199);
		testHarness.addExpectedElement(new Tuple2<>("key2", 1), 2199);
		testHarness.addExpectedElement(new Tuple2<>("key2", 1), 2199);


		testHarness.compareActualToExpectedOutput("Output was not correct.");

		testHarness.processElement(new Tuple2<>("key1", 1), Long.MAX_VALUE);
		testHarness.processElement(new Tuple2<>("key1", 1), Long.MAX_VALUE);

		testHarness.setProcessingTime(3000);

		testHarness.addExpectedElement(new Tuple2<>("key2", 1), 3199);
		testHarness.addExpectedElement(new Tuple2<>("key2", 1), 3199);
		testHarness.addExpectedElement(new Tuple2<>("key2", 1), 3199);
		testHarness.addExpectedElement(new Tuple2<>("key1", 1), 3199);
		testHarness.addExpectedElement(new Tuple2<>("key1", 1), 3199);

		testHarness.compareActualToExpectedOutput("Output was not correct.");

		testHarness.processElement(new Tuple2<>("key1", 1), Long.MAX_VALUE);
		testHarness.processElement(new Tuple2<>("key1", 1), Long.MAX_VALUE);
		testHarness.processElement(new Tuple2<>("key1", 1), Long.MAX_VALUE);

		testHarness.setProcessingTime(7000);

		testHarness.addExpectedElement(new Tuple2<>("key2", 1), 4199);
		testHarness.addExpectedElement(new Tuple2<>("key2", 1), 4199);
		testHarness.addExpectedElement(new Tuple2<>("key1", 1), 4199);
		testHarness.addExpectedElement(new Tuple2<>("key1", 1), 4199);
		testHarness.addExpectedElement(new Tuple2<>("key1", 1), 4199);
		testHarness.addExpectedElement(new Tuple2<>("key1", 1), 4199);
		testHarness.addExpectedElement(new Tuple2<>("key1", 1), 4199);

		testHarness.addExpectedElement(new Tuple2<>("key1", 1), 5199);
		testHarness.addExpectedElement(new Tuple2<>("key1", 1), 5199);
		testHarness.addExpectedElement(new Tuple2<>("key1", 1), 5199);
		testHarness.addExpectedElement(new Tuple2<>("key1", 1), 5199);
		testHarness.addExpectedElement(new Tuple2<>("key1", 1), 5199);

		testHarness.addExpectedElement(new Tuple2<>("key1", 1), 6199);
		testHarness.addExpectedElement(new Tuple2<>("key1", 1), 6199);
		testHarness.addExpectedElement(new Tuple2<>("key1", 1), 6199);

		testHarness.compareActualToExpectedOutput("Output was not correct.");
		testHarness.close();

	}

	@Test
	public void testProcessingTimeSlidingWindowsNegativeOffset() throws Exception {
		final int WINDOW_SIZE = 3;
		final int WINDOW_SLIDE = 1;
		final long offset = -200;

		TypeInformation<Tuple2<String, Integer>> inputType = TypeInfoParser.parse("Tuple2<String, Integer>");

		SlidingProcessingTimeWindows windowAssigner = SlidingProcessingTimeWindows.of(Time.of(WINDOW_SIZE, TimeUnit.SECONDS), Time.of(WINDOW_SLIDE, TimeUnit.SECONDS), offset);

		WindowingTestHarness<String, Tuple2<String, Integer>, TimeWindow> testHarness = new WindowingTestHarness<>(
			new ExecutionConfig(),
			windowAssigner,
			BasicTypeInfo.STRING_TYPE_INFO,
			inputType,
			new WindowAssignerOffsetTest.TupleKeySelector(),
			ProcessingTimeTrigger.create(),
			0);

		testHarness.setProcessingTime(3);

		// timestamp is ignored in processing time
		testHarness.processElement(new Tuple2<>("key2", 1), Long.MAX_VALUE);

		testHarness.setProcessingTime(1000);

		testHarness.addExpectedElement(new Tuple2<>("key2", 1), 799);

		testHarness.compareActualToExpectedOutput("Output was not correct.");


		testHarness.processElement(new Tuple2<>("key2", 1), Long.MAX_VALUE);
		testHarness.processElement(new Tuple2<>("key2", 1), Long.MAX_VALUE);

		testHarness.setProcessingTime(2000);

		testHarness.addExpectedElement(new Tuple2<>("key2", 1), 1799);
		testHarness.addExpectedElement(new Tuple2<>("key2", 1), 1799);
		testHarness.addExpectedElement(new Tuple2<>("key2", 1), 1799);


		testHarness.compareActualToExpectedOutput("Output was not correct.");

		testHarness.processElement(new Tuple2<>("key1", 1), Long.MAX_VALUE);
		testHarness.processElement(new Tuple2<>("key1", 1), Long.MAX_VALUE);

		testHarness.setProcessingTime(3000);

		testHarness.addExpectedElement(new Tuple2<>("key2", 1), 2799);
		testHarness.addExpectedElement(new Tuple2<>("key2", 1), 2799);
		testHarness.addExpectedElement(new Tuple2<>("key2", 1), 2799);
		testHarness.addExpectedElement(new Tuple2<>("key1", 1), 2799);
		testHarness.addExpectedElement(new Tuple2<>("key1", 1), 2799);

		testHarness.compareActualToExpectedOutput("Output was not correct.");

		testHarness.processElement(new Tuple2<>("key1", 1), Long.MAX_VALUE);
		testHarness.processElement(new Tuple2<>("key1", 1), Long.MAX_VALUE);
		testHarness.processElement(new Tuple2<>("key1", 1), Long.MAX_VALUE);

		testHarness.setProcessingTime(7000);

		testHarness.addExpectedElement(new Tuple2<>("key2", 1), 3799);
		testHarness.addExpectedElement(new Tuple2<>("key2", 1), 3799);
		testHarness.addExpectedElement(new Tuple2<>("key1", 1), 3799);
		testHarness.addExpectedElement(new Tuple2<>("key1", 1), 3799);
		testHarness.addExpectedElement(new Tuple2<>("key1", 1), 3799);
		testHarness.addExpectedElement(new Tuple2<>("key1", 1), 3799);
		testHarness.addExpectedElement(new Tuple2<>("key1", 1), 3799);

		testHarness.addExpectedElement(new Tuple2<>("key1", 1), 4799);
		testHarness.addExpectedElement(new Tuple2<>("key1", 1), 4799);
		testHarness.addExpectedElement(new Tuple2<>("key1", 1), 4799);
		testHarness.addExpectedElement(new Tuple2<>("key1", 1), 4799);
		testHarness.addExpectedElement(new Tuple2<>("key1", 1), 4799);

		testHarness.addExpectedElement(new Tuple2<>("key1", 1), 5799);
		testHarness.addExpectedElement(new Tuple2<>("key1", 1), 5799);
		testHarness.addExpectedElement(new Tuple2<>("key1", 1), 5799);

		testHarness.compareActualToExpectedOutput("Output was not correct.");
		testHarness.close();

	}

	@Test
	public void testProcessingTimeSlidingWindowsTimeZoneOffset() throws Exception {
		final int WINDOW_SIZE = 1; //day
		final int WINDOW_SLIDE = 1; //hour
		final TimeZone offset = TimeZone.getTimeZone("Asia/Kolkata");

		TypeInformation<Tuple2<String, Integer>> inputType = TypeInfoParser.parse("Tuple2<String, Integer>");

		SlidingProcessingTimeWindows windowAssigner = SlidingProcessingTimeWindows.of(Time.of(WINDOW_SIZE, TimeUnit.DAYS), Time.of(WINDOW_SLIDE, TimeUnit.HOURS), offset);

		WindowingTestHarness<String, Tuple2<String, Integer>, TimeWindow> testHarness = new WindowingTestHarness<>(
			new ExecutionConfig(),
			windowAssigner,
			BasicTypeInfo.STRING_TYPE_INFO,
			inputType,
			new WindowAssignerOffsetTest.TupleKeySelector(),
			ProcessingTimeTrigger.create(),
			0);

		testHarness.setProcessingTime(3);

		// timestamp is ignored in processing time
		testHarness.processElement(new Tuple2<>("key2", 1), Long.MAX_VALUE);

		testHarness.setProcessingTime(3600000);

		testHarness.addExpectedElement(new Tuple2<>("key2", 1), 23399999);

		testHarness.compareActualToExpectedOutput("Output was not correct.");


		testHarness.processElement(new Tuple2<>("key2", 1), Long.MAX_VALUE);
		testHarness.processElement(new Tuple2<>("key2", 1), Long.MAX_VALUE);

		testHarness.setProcessingTime(7200000);

		testHarness.addExpectedElement(new Tuple2<>("key2", 1), 26999999);
		testHarness.addExpectedElement(new Tuple2<>("key2", 1), 26999999);
		testHarness.addExpectedElement(new Tuple2<>("key2", 1), 26999999);


		testHarness.compareActualToExpectedOutput("Output was not correct.");

		testHarness.close();

	}


	@Test
	public void testProcessingTimeSessionWindowsPositiveOffset() throws Exception {
		final int WINDOW_GAP = 3;
		final long offset = 1000;

		TypeInformation<Tuple2<String, Integer>> inputType = TypeInfoParser.parse("Tuple2<String, Integer>");

		ProcessingTimeSessionWindows windowAssigner = ProcessingTimeSessionWindows.withGap(Time.of(WINDOW_GAP, TimeUnit.SECONDS), offset);

		WindowingTestHarness<String, Tuple2<String, Integer>, TimeWindow> testHarness = new WindowingTestHarness<>(
			new ExecutionConfig(),
			windowAssigner,
			BasicTypeInfo.STRING_TYPE_INFO,
			inputType,
			new WindowAssignerOffsetTest.TupleKeySelector(),
			ProcessingTimeTrigger.create(),
			0);

		testHarness.setProcessingTime(3);

		// timestamp is ignored in processing time
		testHarness.processElement(new Tuple2<>("key2", 1), Long.MAX_VALUE);

		testHarness.setProcessingTime(1000);

		testHarness.processElement(new Tuple2<>("key2", 1), 1002);

		testHarness.setProcessingTime(5000);

		testHarness.addExpectedElement(new Tuple2<>("key2", 1), 4999);
		testHarness.addExpectedElement(new Tuple2<>("key2", 1), 4999);

		testHarness.compareActualToExpectedOutput("Output was not correct.");

		testHarness.processElement(new Tuple2<>("key2", 1), 5000);
		testHarness.processElement(new Tuple2<>("key2", 1), 5000);
		testHarness.processElement(new Tuple2<>("key1", 1), 5000);
		testHarness.processElement(new Tuple2<>("key1", 1), 5000);
		testHarness.processElement(new Tuple2<>("key1", 1), 5000);

		testHarness.setProcessingTime(10000);

		testHarness.addExpectedElement(new Tuple2<>("key2", 1), 8999);
		testHarness.addExpectedElement(new Tuple2<>("key2", 1), 8999);
		testHarness.addExpectedElement(new Tuple2<>("key1", 1), 8999);
		testHarness.addExpectedElement(new Tuple2<>("key1", 1), 8999);
		testHarness.addExpectedElement(new Tuple2<>("key1", 1), 8999);

		testHarness.compareActualToExpectedOutput("Output was not correct.");
		testHarness.close();

	}

	@Test
	public void testProcessingTimeSessionWindowsNegativeOffset() throws Exception {
		final int WINDOW_GAP = 3;
		final long offset = -1000;

		TypeInformation<Tuple2<String, Integer>> inputType = TypeInfoParser.parse("Tuple2<String, Integer>");

		ProcessingTimeSessionWindows windowAssigner = ProcessingTimeSessionWindows.withGap(Time.of(WINDOW_GAP, TimeUnit.SECONDS), offset);

		WindowingTestHarness<String, Tuple2<String, Integer>, TimeWindow> testHarness = new WindowingTestHarness<>(
			new ExecutionConfig(),
			windowAssigner,
			BasicTypeInfo.STRING_TYPE_INFO,
			inputType,
			new WindowAssignerOffsetTest.TupleKeySelector(),
			ProcessingTimeTrigger.create(),
			0);

		testHarness.setProcessingTime(3);

		// timestamp is ignored in processing time
		testHarness.processElement(new Tuple2<>("key2", 1), Long.MAX_VALUE);

		testHarness.setProcessingTime(1000);

		testHarness.processElement(new Tuple2<>("key2", 1), 1002);

		testHarness.setProcessingTime(5000);

		testHarness.addExpectedElement(new Tuple2<>("key2", 1), 2999);
		testHarness.addExpectedElement(new Tuple2<>("key2", 1), 2999);

		testHarness.compareActualToExpectedOutput("Output was not correct.");

		testHarness.processElement(new Tuple2<>("key2", 1), 5000);
		testHarness.processElement(new Tuple2<>("key2", 1), 5000);
		testHarness.processElement(new Tuple2<>("key1", 1), 5000);
		testHarness.processElement(new Tuple2<>("key1", 1), 5000);
		testHarness.processElement(new Tuple2<>("key1", 1), 5000);

		testHarness.setProcessingTime(10000);

		testHarness.addExpectedElement(new Tuple2<>("key2", 1), 6999);
		testHarness.addExpectedElement(new Tuple2<>("key2", 1), 6999);
		testHarness.addExpectedElement(new Tuple2<>("key1", 1), 6999);
		testHarness.addExpectedElement(new Tuple2<>("key1", 1), 6999);
		testHarness.addExpectedElement(new Tuple2<>("key1", 1), 6999);

		testHarness.compareActualToExpectedOutput("Output was not correct.");
		testHarness.close();

	}


	@Test
	public void testProcessingTimeSessionWindowsTimeZoneOffset() throws Exception {
		final int WINDOW_GAP = 1;
		final TimeZone offset = TimeZone.getTimeZone("Asia/Kolkata");

		TypeInformation<Tuple2<String, Integer>> inputType = TypeInfoParser.parse("Tuple2<String, Integer>");

		ProcessingTimeSessionWindows windowAssigner = ProcessingTimeSessionWindows.withGap(Time.of(WINDOW_GAP, TimeUnit.HOURS), offset);

		WindowingTestHarness<String, Tuple2<String, Integer>, TimeWindow> testHarness = new WindowingTestHarness<>(
			new ExecutionConfig(),
			windowAssigner,
			BasicTypeInfo.STRING_TYPE_INFO,
			inputType,
			new WindowAssignerOffsetTest.TupleKeySelector(),
			ProcessingTimeTrigger.create(),
			0);

		testHarness.setProcessingTime(3);

		// timestamp is ignored in processing time
		testHarness.processElement(new Tuple2<>("key2", 1), Long.MAX_VALUE);

		testHarness.setProcessingTime(1000);

		testHarness.processElement(new Tuple2<>("key2", 1), 1002);

		testHarness.setProcessingTime(3700000);

		testHarness.addExpectedElement(new Tuple2<>("key2", 1), 23400999);
		testHarness.addExpectedElement(new Tuple2<>("key2", 1), 23400999);

		testHarness.compareActualToExpectedOutput("Output was not correct.");

		testHarness.close();

	}


	private static class TupleKeySelector implements KeySelector<Tuple2<String, Integer>, String> {
		private static final long serialVersionUID = 1L;

		@Override
		public String getKey(Tuple2<String, Integer> value) throws Exception {
			return value.f0;
		}
	}


}
