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
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.runtime.tasks.AsyncExceptionHandler;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTaskTestHarness;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link TestProcessingTimeService}.
 */
public class TestProcessingTimeServiceTest {

	@Test
	public void testCustomTimeServiceProvider() throws Throwable {
		TestProcessingTimeService tp = new TestProcessingTimeService();

		final OneInputStreamTask<String, String> mapTask = new OneInputStreamTask<>();
		mapTask.setProcessingTimeService(tp);

		final OneInputStreamTaskTestHarness<String, String> testHarness = new OneInputStreamTaskTestHarness<>(
			mapTask, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);
		testHarness.setupOutputForSingletonOperatorChain();

		StreamConfig streamConfig = testHarness.getStreamConfig();

		StreamMap<String, String> mapOperator = new StreamMap<>(new StreamTaskTimerTest.DummyMapFunction<String>());
		streamConfig.setStreamOperator(mapOperator);

		testHarness.invoke();

		assertEquals(Long.MIN_VALUE, testHarness.getProcessingTimeService().getCurrentProcessingTime());

		tp.setCurrentTime(11);
		assertEquals(testHarness.getProcessingTimeService().getCurrentProcessingTime(), 11);

		tp.setCurrentTime(15);
		tp.setCurrentTime(16);
		assertEquals(testHarness.getProcessingTimeService().getCurrentProcessingTime(), 16);

		// register 2 tasks
		mapTask.getProcessingTimeService().registerTimer(30, new ProcessingTimeCallback() {
			@Override
			public void onProcessingTime(long timestamp) {

			}
		});

		mapTask.getProcessingTimeService().registerTimer(40, new ProcessingTimeCallback() {
			@Override
			public void onProcessingTime(long timestamp) {

			}
		});

		assertEquals(2, tp.getNumActiveTimers());

		tp.setCurrentTime(35);
		assertEquals(1, tp.getNumActiveTimers());

		tp.setCurrentTime(40);
		assertEquals(0, tp.getNumActiveTimers());

		tp.shutdownService();
	}

	// ------------------------------------------------------------------------

	/**
	 * An {@link AsyncExceptionHandler} storing the handled exception.
	 */
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
