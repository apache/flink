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
import org.apache.flink.streaming.runtime.tasks.AsyncExceptionHandler;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTaskTestHarness;

import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ResultPartitionWriter.class})
@PowerMockIgnore({"javax.management.*", "com.sun.jndi.*"})
public class TestProcessingTimeServiceTest {

	@Test
	public void testCustomTimeServiceProvider() throws Throwable {
		TestProcessingTimeService tp = new TestProcessingTimeService();

		final OneInputStreamTask<String, String> mapTask = new OneInputStreamTask<>();
		mapTask.setProcessingTimeService(tp);

		final OneInputStreamTaskTestHarness<String, String> testHarness = new OneInputStreamTaskTestHarness<>(
			mapTask, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);

		StreamConfig streamConfig = testHarness.getStreamConfig();

		StreamMap<String, String> mapOperator = new StreamMap<>(new StreamTaskTimerTest.DummyMapFunction<String>());
		streamConfig.setStreamOperator(mapOperator);

		testHarness.invoke();

		assertEquals(testHarness.getProcessingTimeService().getCurrentProcessingTime(), 0);

		tp.setCurrentTime(11);
		assertEquals(testHarness.getProcessingTimeService().getCurrentProcessingTime(), 11);

		tp.setCurrentTime(15);
		tp.setCurrentTime(16);
		assertEquals(testHarness.getProcessingTimeService().getCurrentProcessingTime(), 16);

		// register 2 tasks
		mapTask.getProcessingTimeService().registerTimer(30, new Triggerable() {
			@Override
			public void trigger(long timestamp) {

			}
		});

		mapTask.getProcessingTimeService().registerTimer(40, new Triggerable() {
			@Override
			public void trigger(long timestamp) {

			}
		});

		assertEquals(2, tp.getNumRegisteredTimers());

		tp.setCurrentTime(35);
		assertEquals(1, tp.getNumRegisteredTimers());

		tp.setCurrentTime(40);
		assertEquals(0, tp.getNumRegisteredTimers());

		tp.shutdownService();
	}

	// ------------------------------------------------------------------------

	public static class ReferenceSettingExceptionHandler implements AsyncExceptionHandler {

		private final AtomicReference<Throwable> errorReference;

		public ReferenceSettingExceptionHandler(AtomicReference<Throwable> errorReference) {
			this.errorReference = errorReference;
		}

		@Override
		public void handleAsyncException(String message, Throwable exception) {
			errorReference.compareAndSet(null, exception);
		}
	}
}
