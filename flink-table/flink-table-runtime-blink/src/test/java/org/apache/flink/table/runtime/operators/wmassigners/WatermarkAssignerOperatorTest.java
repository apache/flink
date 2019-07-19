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

package org.apache.flink.table.runtime.operators.wmassigners;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.GenericRow;

import org.junit.Test;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Tests of {@link WatermarkAssignerOperator}.
 */
public class WatermarkAssignerOperatorTest extends WatermarkAssignerOperatorTestBase {

	@Test
	public void testWatermarkAssignerWithIdleSource() throws Exception {
		// with timeout 1000 ms
		final WatermarkAssignerOperator operator = new WatermarkAssignerOperator(0, 1, 1000);
		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness =
			new OneInputStreamOperatorTestHarness<>(operator);
		testHarness.getExecutionConfig().setAutoWatermarkInterval(50);
		testHarness.open();

		testHarness.processElement(new StreamRecord<>(GenericRow.of(1L)));
		testHarness.processElement(new StreamRecord<>(GenericRow.of(2L)));
		testHarness.processWatermark(new Watermark(2)); // this watermark should be ignored
		testHarness.processElement(new StreamRecord<>(GenericRow.of(3L)));
		testHarness.processElement(new StreamRecord<>(GenericRow.of(4L)));

		// trigger watermark emit
		testHarness.setProcessingTime(51);
		ConcurrentLinkedQueue<Object> output = testHarness.getOutput();
		List<Watermark> watermarks = extractWatermarks(output);
		assertEquals(1, watermarks.size());
		assertEquals(new Watermark(3), watermarks.get(0));
		assertEquals(StreamStatus.ACTIVE, testHarness.getStreamStatus());
		output.clear();

		testHarness.setProcessingTime(1001);
		assertEquals(StreamStatus.IDLE, testHarness.getStreamStatus());

		testHarness.processElement(new StreamRecord<>(GenericRow.of(4L)));
		testHarness.processElement(new StreamRecord<>(GenericRow.of(5L)));
		testHarness.processElement(new StreamRecord<>(GenericRow.of(6L)));
		testHarness.processElement(new StreamRecord<>(GenericRow.of(7L)));
		testHarness.processElement(new StreamRecord<>(GenericRow.of(8L)));

		assertEquals(StreamStatus.ACTIVE, testHarness.getStreamStatus());
		testHarness.setProcessingTime(1060);
		output = testHarness.getOutput();
		watermarks = extractWatermarks(output);
		assertEquals(1, watermarks.size());
		assertEquals(new Watermark(7), watermarks.get(0));
	}

	@Test
	public void testWatermarkAssignerOperator() throws Exception {
		final WatermarkAssignerOperator operator = new WatermarkAssignerOperator(0, 1, -1);

		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness =
			new OneInputStreamOperatorTestHarness<>(operator);

		testHarness.getExecutionConfig().setAutoWatermarkInterval(50);

		long currentTime = 0;

		testHarness.open();

		testHarness.processElement(new StreamRecord<>(GenericRow.of(1L)));
		testHarness.processElement(new StreamRecord<>(GenericRow.of(2L)));
		testHarness.processWatermark(new Watermark(2)); // this watermark should be ignored
		testHarness.processElement(new StreamRecord<>(GenericRow.of(3L)));
		testHarness.processElement(new StreamRecord<>(GenericRow.of(4L)));

		// validate first part of the sequence. we poll elements until our
		// watermark updates to "3", which must be the result of the "4" element.
		{
			ConcurrentLinkedQueue<Object> output = testHarness.getOutput();
			long nextElementValue = 1L;
			long lastWatermark = -1L;

			while (lastWatermark < 3) {
				if (output.size() > 0) {
					Object next = output.poll();
					assertNotNull(next);
					Tuple2<Long, Long> update = validateElement(next, nextElementValue, lastWatermark);
					nextElementValue = update.f0;
					lastWatermark = update.f1;

					// check the invariant
					assertTrue(lastWatermark < nextElementValue);
				} else {
					currentTime = currentTime + 10;
					testHarness.setProcessingTime(currentTime);
				}
			}

			output.clear();
		}

		testHarness.processElement(new StreamRecord<>(GenericRow.of(4L)));
		testHarness.processElement(new StreamRecord<>(GenericRow.of(5L)));
		testHarness.processElement(new StreamRecord<>(GenericRow.of(6L)));
		testHarness.processElement(new StreamRecord<>(GenericRow.of(7L)));
		testHarness.processElement(new StreamRecord<>(GenericRow.of(8L)));

		// validate the next part of the sequence. we poll elements until our
		// watermark updates to "7", which must be the result of the "8" element.
		{
			ConcurrentLinkedQueue<Object> output = testHarness.getOutput();
			long nextElementValue = 4L;
			long lastWatermark = 2L;

			while (lastWatermark < 7) {
				if (output.size() > 0) {
					Object next = output.poll();
					assertNotNull(next);
					Tuple2<Long, Long> update = validateElement(next, nextElementValue, lastWatermark);
					nextElementValue = update.f0;
					lastWatermark = update.f1;

					// check the invariant
					assertTrue(lastWatermark < nextElementValue);
				} else {
					currentTime = currentTime + 10;
					testHarness.setProcessingTime(currentTime);
				}
			}

			output.clear();
		}

		testHarness.processWatermark(new Watermark(Long.MAX_VALUE));
		assertEquals(Long.MAX_VALUE, ((Watermark) testHarness.getOutput().poll()).getTimestamp());
	}

}
