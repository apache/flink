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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Tests {@link KeyedProcessOperatorWithWatermarkDelay}.
 */
public class KeyedProcessOperatorWithWatermarkDelayTest extends TestLogger{
	@Test
	public void testHoldingBackWatermarks() throws Exception {

		KeyedProcessOperatorWithWatermarkDelay<Integer, Integer, String> operator =
				new KeyedProcessOperatorWithWatermarkDelay<>(new EmptyProcessFunction(), 100);

		OneInputStreamOperatorTestHarness<Integer, String> testHarness =
				new KeyedOneInputStreamOperatorTestHarness<>(operator, new IdentityKeySelector<>(), BasicTypeInfo.INT_TYPE_INFO);

		testHarness.setup();
		testHarness.open();

		testHarness.processWatermark(new Watermark(101));

		testHarness.processWatermark(new Watermark(102));

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		expectedOutput.add(new Watermark(1));
		expectedOutput.add(new Watermark(2));

		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.close();
	}

	@Test
	public void testDelayParameter() throws Exception {
		try {
			new KeyedProcessOperatorWithWatermarkDelay<>(new EmptyProcessFunction(), -1);
		} catch (Exception ex) {
			Assert.assertTrue(ex instanceof IllegalArgumentException);
		}
	}

	private static class EmptyProcessFunction extends ProcessFunction<Integer, String> {
		@Override
		public void processElement(Integer value, Context ctx, Collector<String> out) throws Exception {
			// do nothing
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
