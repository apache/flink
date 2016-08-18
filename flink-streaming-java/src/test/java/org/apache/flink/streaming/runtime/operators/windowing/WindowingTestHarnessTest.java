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
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeInfoParser;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.tasks.StreamTaskState;
import org.apache.flink.streaming.util.WindowingTestHarness;
import org.junit.Test;

public class WindowingTestHarnessTest {

	@Test
	public void testEventTimeTumblingWindows() throws Exception {
		final int WINDOW_SIZE = 2000;

		TypeInformation<Tuple2<String, Integer>> inputType = TypeInfoParser.parse("Tuple2<String, Integer>");

		TumblingEventTimeWindows windowAssigner = TumblingEventTimeWindows.of(Time.milliseconds(WINDOW_SIZE));

		WindowingTestHarness<String, Tuple2<String, Integer>, TimeWindow> testHarness = new WindowingTestHarness<>(
			new ExecutionConfig(),
			windowAssigner,
			BasicTypeInfo.STRING_TYPE_INFO,
			inputType,
			new TupleKeySelector(),
			EventTimeTrigger.create(),
			0);

		// normal element
		testHarness.processElement(new Tuple2<>("key2", 1), 1000);
		testHarness.processWatermark(1985);

		testHarness.addExpectedWatermark(1985);

		// this will not be dropped because window.maxTimestamp() + allowedLateness > currentWatermark
		testHarness.processElement(new Tuple2<>("key2", 1), 1980);

		// dropped as late
		testHarness.processElement(new Tuple2<>("key2", 1), 1998);

		testHarness.processElement(new Tuple2<>("key2", 1), 2001);
		testHarness.processWatermark(2999);

		testHarness.addExpectedElement(new Tuple2<>("key2", 1), 1999);
		testHarness.addExpectedElement(new Tuple2<>("key2", 1), 1999);
		testHarness.addExpectedElement(new Tuple2<>("key2", 1), 1999);
		testHarness.addExpectedWatermark(2999);

		testHarness.addExpectedElement(new Tuple2<>("key2", 1), 3999);

		testHarness.processWatermark(3999);
		testHarness.addExpectedWatermark(3999);

		testHarness.compareActualToExpectedOutput("Output is not correct");

		testHarness.close();
	}

	@Test
	public void testProcessingTimeTumblingWindows() throws Exception {
		final int WINDOW_SIZE = 3000;

		TypeInformation<Tuple2<String, Integer>> inputType = TypeInfoParser.parse("Tuple2<String, Integer>");

		TumblingProcessingTimeWindows windowAssigner = TumblingProcessingTimeWindows.of(Time.milliseconds(WINDOW_SIZE));

		WindowingTestHarness<String, Tuple2<String, Integer>, TimeWindow> testHarness = new WindowingTestHarness<>(
			new ExecutionConfig(),
			windowAssigner,
			BasicTypeInfo.STRING_TYPE_INFO,
			inputType,
			new TupleKeySelector(),
			ProcessingTimeTrigger.create(),
			0);

		testHarness.setProcessingTime(3);

		// timestamp is ignored in processing time
		testHarness.processElement(new Tuple2<>("key2", 1), Long.MAX_VALUE);
		testHarness.processElement(new Tuple2<>("key2", 1), 7000);
		testHarness.processElement(new Tuple2<>("key2", 1), 7000);

		testHarness.processElement(new Tuple2<>("key1", 1), 7000);
		testHarness.processElement(new Tuple2<>("key1", 1), 7000);

		testHarness.setProcessingTime(5000);

		testHarness.addExpectedElement(new Tuple2<>("key2", 1), 2999);
		testHarness.addExpectedElement(new Tuple2<>("key2", 1), 2999);
		testHarness.addExpectedElement(new Tuple2<>("key2", 1), 2999);
		testHarness.addExpectedElement(new Tuple2<>("key1", 1), 2999);
		testHarness.addExpectedElement(new Tuple2<>("key1", 1), 2999);

		testHarness.compareActualToExpectedOutput("Output was not correct.");

		testHarness.processElement(new Tuple2<>("key1", 1), 7000);
		testHarness.processElement(new Tuple2<>("key1", 1), 7000);
		testHarness.processElement(new Tuple2<>("key1", 1), 7000);

		testHarness.setProcessingTime(7000);

		testHarness.addExpectedElement(new Tuple2<>("key1", 1), 5999);
		testHarness.addExpectedElement(new Tuple2<>("key1", 1), 5999);
		testHarness.addExpectedElement(new Tuple2<>("key1", 1), 5999);

		testHarness.compareActualToExpectedOutput("Output was not correct.");

		testHarness.close();
	}

	@Test
	public void testSnapshotingAndRecovery() throws Exception {

		final int WINDOW_SIZE = 3000;

		TypeInformation<Tuple2<String, Integer>> inputType = TypeInfoParser.parse("Tuple2<String, Integer>");

		TumblingEventTimeWindows windowAssigner = TumblingEventTimeWindows.of(Time.milliseconds(WINDOW_SIZE));

		WindowingTestHarness<String, Tuple2<String, Integer>, TimeWindow> testHarness = new WindowingTestHarness<>(
			new ExecutionConfig(),
			windowAssigner,
			BasicTypeInfo.STRING_TYPE_INFO,
			inputType,
			new TupleKeySelector(),
			EventTimeTrigger.create(),
			0);

		// add elements out-of-order
		testHarness.processElement(new Tuple2<>("key2", 1), 3999);
		testHarness.processElement(new Tuple2<>("key2", 1), 3000);

		testHarness.processElement(new Tuple2<>("key1", 1), 20);
		testHarness.processElement(new Tuple2<>("key1", 1), 0);
		testHarness.processElement(new Tuple2<>("key1", 1), 999);

		testHarness.processElement(new Tuple2<>("key2", 1), 1998);
		testHarness.processElement(new Tuple2<>("key2", 1), 1999);
		testHarness.processElement(new Tuple2<>("key2", 1), 1000);

		testHarness.processWatermark(999);
		testHarness.addExpectedWatermark(999);

		testHarness.compareActualToExpectedOutput("Output was not correct.");

		testHarness.processWatermark(1999);
		testHarness.addExpectedWatermark(1999);
		testHarness.compareActualToExpectedOutput("Output was not correct.");

		// do a snapshot, close and restore again
		StreamTaskState snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot, 10L);

		testHarness.processWatermark(2999);

		testHarness.addExpectedElement(new Tuple2<>("key1", 1), 2999);
		testHarness.addExpectedElement(new Tuple2<>("key1", 1), 2999);
		testHarness.addExpectedElement(new Tuple2<>("key1", 1), 2999);

		testHarness.addExpectedElement(new Tuple2<>("key2", 1), 2999);
		testHarness.addExpectedElement(new Tuple2<>("key2", 1), 2999);
		testHarness.addExpectedElement(new Tuple2<>("key2", 1), 2999);

		testHarness.addExpectedWatermark(2999);

		testHarness.compareActualToExpectedOutput("Output was not correct.");

		testHarness.processWatermark(3999);
		testHarness.addExpectedWatermark(3999);

		testHarness.compareActualToExpectedOutput("Output was not correct.");

		testHarness.processWatermark(4999);
		testHarness.addExpectedWatermark(4999);

		testHarness.compareActualToExpectedOutput("Output was not correct.");

		testHarness.processWatermark(5999);

		testHarness.addExpectedElement(new Tuple2<>("key2", 1), 5999);
		testHarness.addExpectedElement(new Tuple2<>("key2", 1), 5999);
		testHarness.addExpectedWatermark(5999);

		testHarness.compareActualToExpectedOutput("Output was not correct.");


		// those don't have any effect...
		testHarness.processWatermark(6999);
		testHarness.processWatermark(7999);

		testHarness.addExpectedWatermark(6999);
		testHarness.addExpectedWatermark(7999);

		testHarness.compareActualToExpectedOutput("Output was not correct.");
	}

	private static class TupleKeySelector implements KeySelector<Tuple2<String, Integer>, String> {
		private static final long serialVersionUID = 1L;

		@Override
		public String getKey(Tuple2<String, Integer> value) throws Exception {
			return value.f0;
		}
	}
}
