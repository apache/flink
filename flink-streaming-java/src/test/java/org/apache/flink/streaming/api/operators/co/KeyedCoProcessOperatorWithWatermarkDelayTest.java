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

package org.apache.flink.streaming.api.operators.co;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperatorWithWatermarkDelay;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.streaming.util.TwoInputStreamOperatorTestHarness;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Tests {@link KeyedProcessOperatorWithWatermarkDelay}.
 */
public class KeyedCoProcessOperatorWithWatermarkDelayTest extends TestLogger {

	@Test
	public void testHoldingBackWatermarks() throws Exception {

		KeyedCoProcessOperatorWithWatermarkDelay<String, Integer, String, String> operator =
				new KeyedCoProcessOperatorWithWatermarkDelay<>(new EmptyProcessFunction(), 100);

		TwoInputStreamOperatorTestHarness<Integer, String, String> testHarness =
				new KeyedTwoInputStreamOperatorTestHarness<>(
						operator,
						new IntToStringKeySelector<>(),
						new IdentityKeySelector<>(),
						BasicTypeInfo.STRING_TYPE_INFO);

		testHarness.setup();
		testHarness.open();

		testHarness.processWatermark1(new Watermark(101));
		testHarness.processWatermark2(new Watermark(202));
		testHarness.processWatermark1(new Watermark(103));
		testHarness.processWatermark2(new Watermark(204));

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		expectedOutput.add(new Watermark(1));
		expectedOutput.add(new Watermark(3));

		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.close();
	}

	@Test
	public void testDelayParameter() throws Exception {
		try {
			new KeyedCoProcessOperatorWithWatermarkDelay<>(new EmptyProcessFunction(), -1);
		} catch (Exception ex) {
			Assert.assertTrue(ex instanceof IllegalArgumentException);
		}
	}

	private static class EmptyProcessFunction extends CoProcessFunction<Integer, String, String> {
		@Override
		public void processElement1(Integer value, Context ctx, Collector<String> out) throws Exception {
			// do nothing
		}

		@Override
		public void processElement2(String value, Context ctx, Collector<String> out) throws Exception {
			// do nothing
		}
	}

	private static class IntToStringKeySelector<T> implements KeySelector<Integer, String> {
		private static final long serialVersionUID = 1L;

		@Override
		public String getKey(Integer value) throws Exception {
			return "" + value;
		}
	}

	private static class IdentityKeySelector<T> implements KeySelector<T, T> {
		private static final long serialVersionUID = 1L;

		@Override
		public T getKey(T value) throws Exception {
			return value;
		}
	}
}
