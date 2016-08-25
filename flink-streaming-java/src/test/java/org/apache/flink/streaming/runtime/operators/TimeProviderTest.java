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
package org.apache.flink.streaming.runtime.operators;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.runtime.tasks.DefaultTimeServiceProvider;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTaskTestHarness;
import org.apache.flink.streaming.runtime.tasks.TestTimeServiceProvider;
import org.apache.flink.streaming.runtime.tasks.TimeServiceProvider;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(PowerMockRunner.class)
@PrepareForTest(ResultPartitionWriter.class)
@PowerMockIgnore({"javax.management.*", "com.sun.jndi.*"})
public class TimeProviderTest {

	@Test
	public void testDefaultTimeProvider() throws InterruptedException {
		final Object lock = new Object();
		TimeServiceProvider timeServiceProvider = DefaultTimeServiceProvider
			.createForTesting(Executors.newSingleThreadScheduledExecutor(), lock);

		final List<Long> timestamps = new ArrayList<>();

		long start = System.currentTimeMillis();
		long interval = 50L;

		long noOfTimers = 5;
		for (int i = 0; i < noOfTimers; i++) {
			double nextTimer = start + i * interval;

			timeServiceProvider.registerTimer((long) nextTimer, new Triggerable() {
				@Override
				public void trigger(long timestamp) throws Exception {
					timestamps.add(timestamp);
				}
			});

			// add also out-of-order tasks to verify that eventually
			// they will be executed in the correct order.

			if (i > 0) {
				timeServiceProvider.registerTimer((long) (nextTimer - 10), new Triggerable() {
					@Override
					public void trigger(long timestamp) throws Exception {
						timestamps.add(timestamp);
					}
				});
			}
		}

		Thread.sleep(1000);

		// verify that the tasks are executed
		// in ascending timestamp order

		int counter = 0;
		long lastTs = Long.MIN_VALUE;
		for (long timestamp: timestamps) {
			Assert.assertTrue(timestamp >= lastTs);
			lastTs = timestamp;

			long expectedTs = start + (counter/2) * interval;
			Assert.assertEquals(timestamp, (expectedTs + ((counter % 2 == 0) ? 0 : 40)));
			counter++;
		}
		Assert.assertEquals(counter, timestamps.size());
		Assert.assertEquals(counter, 9);
	}

	@Test
	public void testTimerSorting() throws Exception {

		final List<Long> result = new ArrayList<>();

		TestTimeServiceProvider provider = new TestTimeServiceProvider();

		provider.registerTimer(45, new Triggerable() {
			@Override
			public void trigger(long timestamp) {
				result.add(timestamp);
			}
		});

		provider.registerTimer(50, new Triggerable() {
			@Override
			public void trigger(long timestamp) {
				result.add(timestamp);
			}
		});

		provider.registerTimer(30, new Triggerable() {
			@Override
			public void trigger(long timestamp) {
				result.add(timestamp);
			}
		});

		provider.registerTimer(50, new Triggerable() {
			@Override
			public void trigger(long timestamp) {
				result.add(timestamp);
			}
		});

		Assert.assertTrue(provider.getNoOfRegisteredTimers() == 4);

		provider.setCurrentTime(100);
		long seen = 0;
		for (Long l: result) {
			Assert.assertTrue(l >= seen);
			seen = l;
		}
	}

	@Test
	public void testCustomTimeServiceProvider() throws Throwable {
		TestTimeServiceProvider tp = new TestTimeServiceProvider();

		final OneInputStreamTask<String, String> mapTask = new OneInputStreamTask<>();
		mapTask.setTimeService(tp);

		final OneInputStreamTaskTestHarness<String, String> testHarness = new OneInputStreamTaskTestHarness<>(
			mapTask, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);

		StreamConfig streamConfig = testHarness.getStreamConfig();

		StreamMap<String, String> mapOperator = new StreamMap<>(new StreamTaskTimerTest.DummyMapFunction<String>());
		streamConfig.setStreamOperator(mapOperator);

		testHarness.invoke();

		assertTrue(testHarness.getCurrentProcessingTime() == 0);

		tp.setCurrentTime(11);
		assertTrue(testHarness.getCurrentProcessingTime() == 11);

		tp.setCurrentTime(15);
		tp.setCurrentTime(16);
		assertTrue(testHarness.getCurrentProcessingTime() == 16);

		// register 2 tasks
		mapTask.registerTimer(30, new Triggerable() {
			@Override
			public void trigger(long timestamp) {

			}
		});

		mapTask.registerTimer(40, new Triggerable() {
			@Override
			public void trigger(long timestamp) {

			}
		});

		assertEquals(2, tp.getNoOfRegisteredTimers());

		tp.setCurrentTime(35);
		assertEquals(1, tp.getNoOfRegisteredTimers());

		tp.setCurrentTime(40);
		assertEquals(0, tp.getNoOfRegisteredTimers());

		tp.shutdownService();
	}
}
