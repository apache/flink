/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.operators;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.streaming.util.CollectorOutput;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link StreamSource} operators.
 */
@SuppressWarnings("serial")
public class StreamSourceOperatorLatencyMetricsTest {

	/**
	 * Test that latency marks are emitted.
	 */
	@Test
	public void testLatencyMarkEmission() throws Exception {
		final List<StreamElement> output = new ArrayList<>();

		final long maxProcessingTime = 100L;
		final long latencyMarkInterval = 10L;

		final TestProcessingTimeService testProcessingTimeService = new TestProcessingTimeService();
		testProcessingTimeService.setCurrentTime(0L);
		final List<Long> processingTimes = Arrays.asList(1L, 10L, 11L, 21L, maxProcessingTime);

		// regular stream source operator
		final StreamSource<Long, ProcessingTimeServiceSource> operator =
			new StreamSource<>(new ProcessingTimeServiceSource(testProcessingTimeService, processingTimes));

		// emit latency marks every 10 milliseconds.
		setupSourceOperator(operator, TimeCharacteristic.EventTime, latencyMarkInterval, testProcessingTimeService);

		// run and wait to be stopped
		operator.run(new Object(), mock(StreamStatusMaintainer.class), new CollectorOutput<Long>(output));

		int numberLatencyMarkers = (int) (maxProcessingTime / latencyMarkInterval) + 1;

		assertEquals(
			numberLatencyMarkers + 1, // + 1 is the final watermark element
			output.size());

		long timestamp = 0L;

		int i = 0;
		// and that its only latency markers + a final watermark
		for (; i < output.size() - 1; i++) {
			StreamElement se = output.get(i);
			Assert.assertTrue(se.isLatencyMarker());
			Assert.assertEquals(operator.getOperatorID(), se.asLatencyMarker().getOperatorId());
			Assert.assertEquals(0, se.asLatencyMarker().getSubtaskIndex());
			Assert.assertTrue(se.asLatencyMarker().getMarkedTime() == timestamp);

			timestamp += latencyMarkInterval;
		}

		Assert.assertTrue(output.get(i).isWatermark());
	}

	// ------------------------------------------------------------------------

	@SuppressWarnings("unchecked")
	private static <T> void setupSourceOperator(StreamSource<T, ?> operator,
	                                            TimeCharacteristic timeChar,
	                                            long latencyMarkInterval,
	                                            final ProcessingTimeService timeProvider) {

		ExecutionConfig executionConfig = new ExecutionConfig();
		executionConfig.setLatencyTrackingInterval(latencyMarkInterval);

		StreamConfig cfg = new StreamConfig(new Configuration());
		cfg.setStateBackend(new MemoryStateBackend());

		cfg.setTimeCharacteristic(timeChar);
		cfg.setOperatorID(new OperatorID());

		Environment env = new DummyEnvironment("MockTwoInputTask", 1, 0);

		StreamStatusMaintainer streamStatusMaintainer = mock(StreamStatusMaintainer.class);
		when(streamStatusMaintainer.getStreamStatus()).thenReturn(StreamStatus.ACTIVE);

		StreamTask<?, ?> mockTask = mock(StreamTask.class);
		when(mockTask.getName()).thenReturn("Mock Task");
		when(mockTask.getCheckpointLock()).thenReturn(new Object());
		when(mockTask.getConfiguration()).thenReturn(cfg);
		when(mockTask.getEnvironment()).thenReturn(env);
		when(mockTask.getExecutionConfig()).thenReturn(executionConfig);
		when(mockTask.getAccumulatorMap()).thenReturn(Collections.<String, Accumulator<?, ?>>emptyMap());
		when(mockTask.getStreamStatusMaintainer()).thenReturn(streamStatusMaintainer);

		doAnswer(new Answer<ProcessingTimeService>() {
			@Override
			public ProcessingTimeService answer(InvocationOnMock invocation) throws Throwable {
				if (timeProvider == null) {
					throw new RuntimeException("The time provider is null.");
				}
				return timeProvider;
			}
		}).when(mockTask).getProcessingTimeService();

		operator.setup(mockTask, cfg, (Output<StreamRecord<T>>) mock(Output.class));
	}

	// ------------------------------------------------------------------------

	private static final class ProcessingTimeServiceSource implements SourceFunction<Long> {

		private final TestProcessingTimeService processingTimeService;
		private final List<Long> processingTimes;

		private boolean cancelled = false;

		private ProcessingTimeServiceSource(TestProcessingTimeService processingTimeService, List<Long> processingTimes) {
			this.processingTimeService = processingTimeService;
			this.processingTimes = processingTimes;
		}

		@Override
		public void run(SourceContext<Long> ctx) throws Exception {
			for (Long processingTime : processingTimes) {
				if (cancelled) {
					break;
				}

				processingTimeService.setCurrentTime(processingTime);
			}
		}

		@Override
		public void cancel() {
			cancelled = true;
		}
	}
}
