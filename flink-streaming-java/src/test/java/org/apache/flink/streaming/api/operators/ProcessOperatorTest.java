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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Tests {@link ProcessOperator}.
 */
public class ProcessOperatorTest extends TestLogger {

	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	@Test
	public void testTimestampAndWatermarkQuerying() throws Exception {

		ProcessOperator<Integer, String> operator =
				new ProcessOperator<>(new QueryingProcessFunction(TimeDomain.EVENT_TIME));

		OneInputStreamOperatorTestHarness<Integer, String> testHarness =
				new OneInputStreamOperatorTestHarness<>(operator);

		testHarness.setup();
		testHarness.open();

		testHarness.processWatermark(new Watermark(17));
		testHarness.processElement(new StreamRecord<>(5, 12L));

		testHarness.processWatermark(new Watermark(42));
		testHarness.processElement(new StreamRecord<>(6, 13L));

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		expectedOutput.add(new Watermark(17L));
		expectedOutput.add(new StreamRecord<>("5TIME:17 TS:12", 12L));
		expectedOutput.add(new Watermark(42L));
		expectedOutput.add(new StreamRecord<>("6TIME:42 TS:13", 13L));

		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.close();
	}

	@Test
	public void testTimestampAndProcessingTimeQuerying() throws Exception {

		ProcessOperator<Integer, String> operator =
				new ProcessOperator<>(new QueryingProcessFunction(TimeDomain.PROCESSING_TIME));

		OneInputStreamOperatorTestHarness<Integer, String> testHarness =
				new OneInputStreamOperatorTestHarness<>(operator);

		testHarness.setup();
		testHarness.open();

		testHarness.setProcessingTime(17);
		testHarness.processElement(new StreamRecord<>(5));

		testHarness.setProcessingTime(42);
		testHarness.processElement(new StreamRecord<>(6));

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		expectedOutput.add(new StreamRecord<>("5TIME:17 TS:null"));
		expectedOutput.add(new StreamRecord<>("6TIME:42 TS:null"));

		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.close();
	}

	@Test
	public void testNullOutputTagRefusal() throws Exception {
		ProcessOperator<Integer, String> operator = new ProcessOperator<>(new NullOutputTagEmittingProcessFunction());

		OneInputStreamOperatorTestHarness<Integer, String> testHarness = new OneInputStreamOperatorTestHarness<>(operator);

		testHarness.setup();
		testHarness.open();

		testHarness.setProcessingTime(17);
		try {
			expectedException.expect(IllegalArgumentException.class);
			testHarness.processElement(new StreamRecord<>(5));
		} finally {
			testHarness.close();
		}
	}

	private static class NullOutputTagEmittingProcessFunction extends ProcessFunction<Integer, String> {

		@Override
		public void processElement(Integer value, Context ctx, Collector<String> out) throws Exception {
			ctx.output(null, value);
		}
	}

	private static class QueryingProcessFunction extends ProcessFunction<Integer, String> {

		private static final long serialVersionUID = 1L;

		private final TimeDomain timeDomain;

		public QueryingProcessFunction(TimeDomain timeDomain) {
			this.timeDomain = timeDomain;
		}

		@Override
		public void processElement(Integer value, Context ctx, Collector<String> out) throws Exception {
			if (timeDomain.equals(TimeDomain.EVENT_TIME)) {
				out.collect(value + "TIME:" + ctx.timerService().currentWatermark() + " TS:" + ctx.timestamp());
			} else {
				out.collect(value + "TIME:" + ctx.timerService().currentProcessingTime() + " TS:" + ctx.timestamp());
			}
		}

		@Override
		public void onTimer(
				long timestamp,
				OnTimerContext ctx,
				Collector<String> out) throws Exception {
		}
	}
}
