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

package org.apache.flink.streaming.runtime.metrics;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TwoInputStreamOperatorTestHarness;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests to verify that {@link AbstractStreamOperator} properly updates the {@link WatermarkGauge}.
 */
public class StreamOperatorWatermarkMetricsTest {

	@Test
	public void testOneInputWatermarkReporting() throws Exception {
		TestOneInputStreamOperator operator = new TestOneInputStreamOperator();
		try (OneInputStreamOperatorTestHarness<Integer, Integer> harness = new OneInputStreamOperatorTestHarness<>(operator)) {
			harness.setup();
			WatermarkGauge inputWatermarkGauge = operator.getInputWatermarkGauge();
			WatermarkGauge outputWatermarkGauge = operator.getOutputWatermarkGauge();

			Assert.assertEquals(Long.MIN_VALUE, outputWatermarkGauge.getValue().longValue());

			harness.processWatermark(new Watermark(64L));
			Assert.assertEquals(64L, inputWatermarkGauge.getValue().longValue());
			Assert.assertEquals(64L, outputWatermarkGauge.getValue().longValue());
			harness.processWatermark(new Watermark(128L));
			Assert.assertEquals(128L, inputWatermarkGauge.getValue().longValue());
			Assert.assertEquals(128L, outputWatermarkGauge.getValue().longValue());
			harness.processWatermark(Watermark.MAX_WATERMARK);
			Assert.assertEquals(Watermark.MAX_WATERMARK.getTimestamp(), inputWatermarkGauge.getValue().longValue());
			Assert.assertEquals(Watermark.MAX_WATERMARK.getTimestamp(), outputWatermarkGauge.getValue().longValue());
		}
	}

	@Test
	public void testTwoInputWatermarkReporting() throws Exception {
		TestTwoInputStreamOperator operator = new TestTwoInputStreamOperator();
		try (TwoInputStreamOperatorTestHarness<Integer, Integer, Integer> harness = new TwoInputStreamOperatorTestHarness<>(operator)) {
			harness.setup();
			WatermarkGauge inputWatermarkGauge = operator.getInputWatermarkGauge();
			WatermarkGauge outputWatermarkGauge = operator.getOutputWatermarkGauge();

			Assert.assertEquals(Long.MIN_VALUE, outputWatermarkGauge.getValue().longValue());

			harness.processWatermark1(new Watermark(64L));
			Assert.assertEquals(Long.MIN_VALUE, inputWatermarkGauge.getValue().longValue());
			Assert.assertEquals(Long.MIN_VALUE, outputWatermarkGauge.getValue().longValue());

			harness.processWatermark2(new Watermark(128L));
			Assert.assertEquals(64L, inputWatermarkGauge.getValue().longValue());
			Assert.assertEquals(64L, outputWatermarkGauge.getValue().longValue());

			harness.processWatermark1(new Watermark(128L));
			Assert.assertEquals(128L, inputWatermarkGauge.getValue().longValue());
			Assert.assertEquals(128L, outputWatermarkGauge.getValue().longValue());

			harness.processWatermark1(new Watermark(256L));
			Assert.assertEquals(128L, inputWatermarkGauge.getValue().longValue());
			Assert.assertEquals(128L, outputWatermarkGauge.getValue().longValue());

			harness.processWatermark1(Watermark.MAX_WATERMARK);
			Assert.assertEquals(128L, inputWatermarkGauge.getValue().longValue());
			Assert.assertEquals(128L, outputWatermarkGauge.getValue().longValue());

			harness.processWatermark2(Watermark.MAX_WATERMARK);
			Assert.assertEquals(Watermark.MAX_WATERMARK.getTimestamp(), inputWatermarkGauge.getValue().longValue());
			Assert.assertEquals(Watermark.MAX_WATERMARK.getTimestamp(), outputWatermarkGauge.getValue().longValue());
		}
	}

	private static class TestOneInputStreamOperator extends AbstractStreamOperator<Integer> implements OneInputStreamOperator<Integer, Integer> {

		@Override
		public void processElement(StreamRecord<Integer> element) throws Exception {
			output.collect(element);
		}
	}

	private static class TestTwoInputStreamOperator extends AbstractStreamOperator<Integer> implements TwoInputStreamOperator<Integer, Integer, Integer> {

		@Override
		public void processElement1(StreamRecord<Integer> element) throws Exception {
			output.collect(element);
		}

		@Override
		public void processElement2(StreamRecord<Integer> element) throws Exception {
			output.collect(element);
		}
	}
}
