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
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.util.TestingTaskManagerRuntimeInfo;
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
import org.apache.flink.util.TestLogger;

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
 * Tests for the emission of latency markers by {@link StreamSource} operators.
 */
public class StreamSourceOperatorLatencyMetricsTest extends TestLogger {

	private static final long maxProcessingTime = 100L;
	private static final long latencyMarkInterval = 10L;

	/**
	 * Verifies that latency metrics can be enabled via the {@link ExecutionConfig}.
	 */
	@Test
	public void testLatencyMarkEmissionEnabledViaExecutionConfig() throws Exception {
		testLatencyMarkEmission((int) (maxProcessingTime / latencyMarkInterval) + 1, (operator, timeProvider) -> {
			ExecutionConfig executionConfig = new ExecutionConfig();
			executionConfig.setLatencyTrackingInterval(latencyMarkInterval);

			setupSourceOperator(operator, executionConfig, MockEnvironment.builder().build(), timeProvider);
		});
	}

	/**
	 * Verifies that latency metrics can be enabled via the configuration.
	 */
	@Test
	public void testLatencyMarkEmissionEnabledViaFlinkConfig() throws Exception {
		testLatencyMarkEmission((int) (maxProcessingTime / latencyMarkInterval) + 1, (operator, timeProvider) -> {
			Configuration tmConfig = new Configuration();
			tmConfig.setLong(MetricOptions.LATENCY_INTERVAL, latencyMarkInterval);

			Environment env = MockEnvironment.builder()
				.setTaskManagerRuntimeInfo(new TestingTaskManagerRuntimeInfo(tmConfig))
				.build();

			setupSourceOperator(operator, new ExecutionConfig(), env, timeProvider);
		});
	}

	/**
	 * Verifies that latency metrics can be enabled via the {@link ExecutionConfig} even if they are disabled via
	 * the configuration.
	 */
	@Test
	public void testLatencyMarkEmissionEnabledOverrideViaExecutionConfig() throws Exception {
		testLatencyMarkEmission((int) (maxProcessingTime / latencyMarkInterval) + 1, (operator, timeProvider) -> {
			ExecutionConfig executionConfig = new ExecutionConfig();
			executionConfig.setLatencyTrackingInterval(latencyMarkInterval);

			Configuration tmConfig = new Configuration();
			tmConfig.setLong(MetricOptions.LATENCY_INTERVAL, 0L);

			Environment env = MockEnvironment.builder()
				.setTaskManagerRuntimeInfo(new TestingTaskManagerRuntimeInfo(tmConfig))
				.build();

			setupSourceOperator(operator, executionConfig, env, timeProvider);
		});
	}

	/**
	 * Verifies that latency metrics can be disabled via the {@link ExecutionConfig} even if they are enabled via
	 * the configuration.
	 */
	@Test
	public void testLatencyMarkEmissionDisabledOverrideViaExecutionConfig() throws Exception {
		testLatencyMarkEmission(0, (operator, timeProvider) -> {
			Configuration tmConfig = new Configuration();
			tmConfig.setLong(MetricOptions.LATENCY_INTERVAL, latencyMarkInterval);

			Environment env = MockEnvironment.builder()
				.setTaskManagerRuntimeInfo(new TestingTaskManagerRuntimeInfo(tmConfig))
				.build();

			ExecutionConfig executionConfig = new ExecutionConfig();
			executionConfig.setLatencyTrackingInterval(0);

			setupSourceOperator(operator, executionConfig, env, timeProvider);
		});
	}

	private interface OperatorSetupOperation {
		void setupSourceOperator(
			StreamSource<Long, ?> operator,
			TestProcessingTimeService testProcessingTimeService
		);
	}

	private void testLatencyMarkEmission(int numberLatencyMarkers, OperatorSetupOperation operatorSetup) throws Exception {
		final List<StreamElement> output = new ArrayList<>();

		final TestProcessingTimeService testProcessingTimeService = new TestProcessingTimeService();
		testProcessingTimeService.setCurrentTime(0L);
		final List<Long> processingTimes = Arrays.asList(1L, 10L, 11L, 21L, maxProcessingTime);

		// regular stream source operator
		final StreamSource<Long, ProcessingTimeServiceSource> operator =
			new StreamSource<>(new ProcessingTimeServiceSource(testProcessingTimeService, processingTimes));

		operatorSetup.setupSourceOperator(operator, testProcessingTimeService);

		// run and wait to be stopped
		operator.run(new Object(), mock(StreamStatusMaintainer.class), new CollectorOutput<Long>(output));

		assertEquals(
			numberLatencyMarkers + 1, // + 1 is the final watermark element
			output.size());

		long timestamp = 0L;

		int i = 0;
		// verify that its only latency markers + a final watermark
		for (; i < numberLatencyMarkers; i++) {
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

	private static <T> void setupSourceOperator(
			StreamSource<T, ?> operator,
			ExecutionConfig executionConfig,
			Environment env,
			ProcessingTimeService timeProvider) {

		StreamConfig cfg = new StreamConfig(new Configuration());
		cfg.setStateBackend(new MemoryStateBackend());

		cfg.setTimeCharacteristic(TimeCharacteristic.EventTime);
		cfg.setOperatorID(new OperatorID());

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
