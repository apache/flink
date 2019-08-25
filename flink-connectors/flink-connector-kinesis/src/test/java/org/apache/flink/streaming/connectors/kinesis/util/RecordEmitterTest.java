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

package org.apache.flink.streaming.connectors.kinesis.util;

import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** Test for {@link RecordEmitter}. */
public class RecordEmitterTest {

	static List<TimestampedValue> results = Collections.synchronizedList(new ArrayList<>());

	private class TestRecordEmitter extends RecordEmitter<TimestampedValue> {

		private TestRecordEmitter() {
			super(DEFAULT_QUEUE_CAPACITY);
		}

		@Override
		public void emit(TimestampedValue record, RecordQueue<TimestampedValue> queue) {
			results.add(record);
		}
	}

	@Test
	public void test() throws Exception {

		TestRecordEmitter emitter = new TestRecordEmitter();

		final TimestampedValue<String> one = new TimestampedValue<>("one", 1);
		final TimestampedValue<String> two = new TimestampedValue<>("two", 2);
		final TimestampedValue<String> five = new TimestampedValue<>("five", 5);
		final TimestampedValue<String> ten = new TimestampedValue<>("ten", 10);

		final RecordEmitter.RecordQueue<TimestampedValue> queue0 = emitter.getQueue(0);
		final RecordEmitter.RecordQueue<TimestampedValue> queue1 = emitter.getQueue(1);

		queue0.put(one);
		queue0.put(five);
		queue0.put(ten);

		queue1.put(two);

		ExecutorService executor = Executors.newSingleThreadExecutor();
		executor.submit(emitter);

		long timeout = System.currentTimeMillis() + 10_000;
		while (results.size() != 4 && System.currentTimeMillis() < timeout) {
			Thread.sleep(100);
		}
		emitter.stop();
		executor.shutdownNow();

		Assert.assertThat(results, Matchers.contains(one, five, two, ten));
	}

}
