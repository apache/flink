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
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.GenericRow;

import org.junit.Test;

import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Tests of {@link MiniBatchAssignerOperator}.
 */
public class MiniBatchAssignerOperatorTest extends WatermarkAssignerOperatorTestBase {

	@Test
	public void testMiniBatchAssignerOperator() throws Exception {
		final MiniBatchAssignerOperator operator = new MiniBatchAssignerOperator(100);

		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness =
				new OneInputStreamOperatorTestHarness<>(operator);

		long currentTime = 0;

		testHarness.open();

		testHarness.processElement(new StreamRecord<>(GenericRow.of(1L)));
		testHarness.processElement(new StreamRecord<>(GenericRow.of(2L)));
		testHarness.processWatermark(new Watermark(2)); // this watermark should be ignored
		testHarness.processElement(new StreamRecord<>(GenericRow.of(3L)));
		testHarness.processElement(new StreamRecord<>(GenericRow.of(4L)));

		{
			ConcurrentLinkedQueue<Object> output = testHarness.getOutput();
			long currentElement = 1L;
			long lastWatermark = 0L;

			while (true) {
				if (output.size() > 0) {
					Object next = output.poll();
					assertNotNull(next);
					Tuple2<Long, Long> update = validateElement(next, currentElement, lastWatermark);
					long nextElementValue = update.f0;
					lastWatermark = update.f1;
					if (next instanceof Watermark) {
						assertEquals(100, lastWatermark);
						break;
					} else {
						assertEquals(currentElement, nextElementValue - 1);
						currentElement += 1;
						assertEquals(0, lastWatermark);
					}
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

		{
			ConcurrentLinkedQueue<Object> output = testHarness.getOutput();
			long currentElement = 4L;
			long lastWatermark = 100L;

			while (true) {
				if (output.size() > 0) {
					Object next = output.poll();
					assertNotNull(next);
					Tuple2<Long, Long> update = validateElement(next, currentElement, lastWatermark);
					long nextElementValue = update.f0;
					lastWatermark = update.f1;
					if (next instanceof Watermark) {
						assertEquals(200, lastWatermark);
						break;
					} else {
						assertEquals(currentElement, nextElementValue - 1);
						currentElement += 1;
						assertEquals(100, lastWatermark);
					}
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
