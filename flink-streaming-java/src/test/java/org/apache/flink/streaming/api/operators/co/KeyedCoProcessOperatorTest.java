/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators.co;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.streaming.util.TwoInputStreamOperatorTestHarness;
import org.apache.flink.util.Collector;

import org.junit.Test;

import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.Assert.assertEquals;

/**
 * Tests {@link KeyedCoProcessOperator}.
 */
public class KeyedCoProcessOperatorTest extends LegacyKeyedCoProcessOperatorTest {

	@Test
	public void testGetCurrentKeyFromContext() throws Exception {
		KeyedCoProcessOperator<String, Integer, String, String> operator =
			new KeyedCoProcessOperator<>(new RepeatKeyProcessFunction());

		TwoInputStreamOperatorTestHarness<Integer, String, String> testHarness =
			new KeyedTwoInputStreamOperatorTestHarness<>(
				operator,
				new IntToStringKeySelector<>(),
				new IdentityKeySelector<String>(),
				BasicTypeInfo.STRING_TYPE_INFO);

		testHarness.setup();
		testHarness.open();

		testHarness.processElement1(new StreamRecord<>(5));
		testHarness.processElement1(new StreamRecord<>(6));
		testHarness.processElement2(new StreamRecord<>("hello"));
		testHarness.processElement2(new StreamRecord<>("world"));

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		expectedOutput.add(new StreamRecord<>("5,5"));
		expectedOutput.add(new StreamRecord<>("6,6"));
		expectedOutput.add(new StreamRecord<>("hello,hello"));
		expectedOutput.add(new StreamRecord<>("world,world"));

		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.close();
	}

	@Test
	public void testGetCurrentKeyFromTimers() throws Exception {
		KeyedCoProcessOperator<String, Integer, String, String> operator =
			new KeyedCoProcessOperator<>(new EventTimeTriggeringProcessFunction());

		TwoInputStreamOperatorTestHarness<Integer, String, String> testHarness =
			new KeyedTwoInputStreamOperatorTestHarness<>(
				operator,
				new IntToStringKeySelector<>(),
				new IdentityKeySelector<String>(),
				BasicTypeInfo.STRING_TYPE_INFO);

		testHarness.setup();
		testHarness.open();

		testHarness.processElement1(new StreamRecord<>(17, 42L));
		testHarness.processElement2(new StreamRecord<>("18", 42L));

		testHarness.processWatermark1(new Watermark(5));
		testHarness.processWatermark2(new Watermark(5));

		testHarness.processWatermark1(new Watermark(6));
		testHarness.processWatermark2(new Watermark(6));

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		expectedOutput.add(new StreamRecord<>("INPUT1:17", 42L));
		expectedOutput.add(new StreamRecord<>("INPUT2:18", 42L));
		expectedOutput.add(new StreamRecord<>("17:1777", 5L));
		expectedOutput.add(new Watermark(5L));
		expectedOutput.add(new StreamRecord<>("18:1777", 6L));
		expectedOutput.add(new Watermark(6L));

		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.close();
	}

	private static class RepeatKeyProcessFunction extends KeyedCoProcessFunction<String, Integer, String, String> {

		@Override
		public void processElement1(Integer value, KeyedCoProcessFunction.Context ctx, Collector<String> out) throws Exception {
			out.collect(value + "," + ctx.getCurrentKey());
		}

		@Override
		public void processElement2(String value, KeyedCoProcessFunction.Context ctx, Collector<String> out) throws Exception {
			out.collect(value + "," + ctx.getCurrentKey());
		}
	}

	private static class EventTimeTriggeringProcessFunction extends KeyedCoProcessFunction<String, Integer, String, String> {

		private static final long serialVersionUID = 1L;

		@Override
		public void processElement1(Integer value, KeyedCoProcessFunction.Context ctx, Collector<String> out) throws Exception {
			out.collect("INPUT1:" + value);
			ctx.timerService().registerEventTimeTimer(5);
		}

		@Override
		public void processElement2(String value, KeyedCoProcessFunction.Context ctx, Collector<String> out) throws Exception {
			out.collect("INPUT2:" + value);
			ctx.timerService().registerEventTimeTimer(6);
		}

		@Override
		public void onTimer(
			long timestamp,
			KeyedCoProcessFunction.OnTimerContext ctx,
			Collector<String> out) throws Exception {

			assertEquals(TimeDomain.EVENT_TIME, ctx.timeDomain());
			out.collect(ctx.getCurrentKey() + ":" + 1777);
		}
	}

}
