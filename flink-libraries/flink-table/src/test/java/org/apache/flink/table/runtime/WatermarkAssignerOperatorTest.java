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

package org.apache.flink.table.runtime;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.GenericRow;

import org.junit.Test;

import java.util.concurrent.ConcurrentLinkedQueue;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Tests of WatermarkAssignerOperator.
 */
public class WatermarkAssignerOperatorTest {

	@Test
	public void testWatermarkAssingerOperator() throws Exception {

		final WatermarkAssignerOperator operator = new WatermarkAssignerOperator(0, 1);

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

	// -------------------------------------------------------------------------

	private Tuple2<Long, Long> validateElement(Object element, long nextElementValue, long currentWatermark) {
		if (element instanceof StreamRecord) {
			@SuppressWarnings("unchecked")
			StreamRecord<BaseRow> record = (StreamRecord<BaseRow>) element;
			assertEquals(nextElementValue, record.getValue().getLong(0));
			return new Tuple2<>(nextElementValue + 1, currentWatermark);
		}
		else if (element instanceof Watermark) {
			long wt = ((Watermark) element).getTimestamp();
			assertTrue(wt > currentWatermark);
			return new Tuple2<>(nextElementValue, wt);
		}
		else {
			throw new IllegalArgumentException("unrecognized element: " + element);
		}
	}
}
