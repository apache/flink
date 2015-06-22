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

import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.joda.time.Instant;
import org.junit.Test;

/**
 * Tests for {@link StreamCounter}. These test that:
 *
 * <ul>
 *     <li>Timestamps of processed elements match the input timestamp</li>
 *     <li>Watermarks are correctly forwarded</li>
 * </ul>
 */
public class StreamCounterTest {

	@Test
	public void testCount() throws Exception {
		StreamCounter<String> operator = new StreamCounter<String>();

		OneInputStreamOperatorTestHarness<String, Long> testHarness = new OneInputStreamOperatorTestHarness<String, Long>(operator);

		long initialTime = 0L;
		ConcurrentLinkedQueue expectedOutput = new ConcurrentLinkedQueue();

		testHarness.open();

		testHarness.processElement(new StreamRecord<String>("eins", initialTime + 1));
		testHarness.processElement(new StreamRecord<String>("zwei", initialTime + 2));
		testHarness.processWatermark(new Watermark(initialTime + 2));
		testHarness.processElement(new StreamRecord<String>("drei", initialTime + 3));

		expectedOutput.add(new StreamRecord<Long>(1L, initialTime + 1));
		expectedOutput.add(new StreamRecord<Long>(2L, initialTime + 2));
		expectedOutput.add(new Watermark(initialTime + 2));
		expectedOutput.add(new StreamRecord<Long>(3L, initialTime + 3));

		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());
	}
}
