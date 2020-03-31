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

/**
 * Tests of {@link MiniBatchedWatermarkAssignerOperator}.
 */
public class MiniBatchedWatermarkAssignerOperatorTest extends WatermarkAssignerOperatorTestBase {

	@Test
	public void testMiniBatchedWatermarkAssignerWithIdleSource() throws Exception {
		// with timeout 1000 ms
		final MiniBatchedWatermarkAssignerOperator operator = new MiniBatchedWatermarkAssignerOperator(
				0, 1, 0, 1000, 50);
		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness =
				new OneInputStreamOperatorTestHarness<>(operator);
		testHarness.open();

		testHarness.processElement(new StreamRecord<>(GenericRow.of(1L)));
		testHarness.processElement(new StreamRecord<>(GenericRow.of(2L)));
		testHarness.processWatermark(new Watermark(2)); // this watermark should be ignored
		testHarness.processElement(new StreamRecord<>(GenericRow.of(3L)));
		testHarness.processElement(new StreamRecord<>(GenericRow.of(4L)));
		// this watermark excess expected watermark, should emit a watermark of 49
		testHarness.processElement(new StreamRecord<>(GenericRow.of(50L)));

		ConcurrentLinkedQueue<Object> output = testHarness.getOutput();
		List<Watermark> watermarks = extractWatermarks(output);
		assertEquals(1, watermarks.size());
		assertEquals(new Watermark(49), watermarks.get(0));
		assertEquals(StreamStatus.ACTIVE, testHarness.getStreamStatus());
		output.clear();

		testHarness.setProcessingTime(1001);
		assertEquals(StreamStatus.IDLE, testHarness.getStreamStatus());

		testHarness.processElement(new StreamRecord<>(GenericRow.of(51L)));
		assertEquals(StreamStatus.ACTIVE, testHarness.getStreamStatus());

		// process time will not trigger to emit watermark
		testHarness.setProcessingTime(1060);
		output = testHarness.getOutput();
		watermarks = extractWatermarks(output);
		assertTrue(watermarks.isEmpty());
		output.clear();

		testHarness.processElement(new StreamRecord<>(GenericRow.of(100L)));
		output = testHarness.getOutput();
		watermarks = extractWatermarks(output);
		assertEquals(1, watermarks.size());
		assertEquals(new Watermark(99), watermarks.get(0));
	}

	@Test
	public void testMiniBatchedWatermarkAssignerOperator() throws Exception {
		final MiniBatchedWatermarkAssignerOperator operator = new MiniBatchedWatermarkAssignerOperator(0, 1, 0, -1, 50);

		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness =
				new OneInputStreamOperatorTestHarness<>(operator);

		testHarness.open();

		testHarness.processElement(new StreamRecord<>(GenericRow.of(1L)));
		testHarness.processElement(new StreamRecord<>(GenericRow.of(2L)));
		testHarness.processWatermark(new Watermark(2)); // this watermark should be ignored
		testHarness.processElement(new StreamRecord<>(GenericRow.of(3L)));
		testHarness.processElement(new StreamRecord<>(GenericRow.of(4L)));
		// this watermark excess expected watermark, should emit a watermark of 49
		testHarness.processElement(new StreamRecord<>(GenericRow.of(50L)));

		ConcurrentLinkedQueue<Object> output = testHarness.getOutput();
		List<Watermark> watermarks = extractWatermarks(output);
		assertEquals(1, watermarks.size());
		assertEquals(new Watermark(49), watermarks.get(0));
		output.clear();

		testHarness.setProcessingTime(1001);
		output = testHarness.getOutput();
		watermarks = extractWatermarks(output);
		assertTrue(watermarks.isEmpty());
		output.clear();

		testHarness.processElement(new StreamRecord<>(GenericRow.of(99L)));
		output = testHarness.getOutput();
		watermarks = extractWatermarks(output);
		assertTrue(watermarks.isEmpty());
		output.clear();

		testHarness.processElement(new StreamRecord<>(GenericRow.of(100L)));
		output = testHarness.getOutput();
		watermarks = extractWatermarks(output);
		assertEquals(1, watermarks.size());
		assertEquals(new Watermark(99), watermarks.get(0));
		output.clear();
	}
}
