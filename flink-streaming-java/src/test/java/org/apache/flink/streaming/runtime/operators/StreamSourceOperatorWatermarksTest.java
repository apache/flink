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
import org.apache.flink.api.common.functions.StoppableFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StoppableStreamSource;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.operators.StreamSourceContexts;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.streaming.util.CollectorOutput;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link StreamSource} operators.
 */
@SuppressWarnings("serial")
public class StreamSourceOperatorWatermarksTest {

	@Test
	public void testEmitMaxWatermarkForFiniteSource() throws Exception {

		// regular stream source operator
		StreamSource<String, FiniteSource<String>> operator =
				new StreamSource<>(new FiniteSource<String>());

		final List<StreamElement> output = new ArrayList<>();

		setupSourceOperator(operator, TimeCharacteristic.EventTime, 0);
		operator.run(new Object(), mock(StreamStatusMaintainer.class), new CollectorOutput<String>(output));

		assertEquals(1, output.size());
		assertEquals(Watermark.MAX_WATERMARK, output.get(0));
	}

	@Test
	public void testNoMaxWatermarkOnImmediateCancel() throws Exception {

		final List<StreamElement> output = new ArrayList<>();

		// regular stream source operator
		final StreamSource<String, InfiniteSource<String>> operator =
				new StreamSource<>(new InfiniteSource<String>());

		setupSourceOperator(operator, TimeCharacteristic.EventTime, 0);
		operator.cancel();

		// run and exit
		operator.run(new Object(), mock(StreamStatusMaintainer.class), new CollectorOutput<String>(output));

		assertTrue(output.isEmpty());
	}

	@Test
	public void testNoMaxWatermarkOnAsyncCancel() throws Exception {

		final List<StreamElement> output = new ArrayList<>();
		final Thread runner = Thread.currentThread();

		// regular stream source operator
		final StreamSource<String, InfiniteSource<String>> operator =
				new StreamSource<>(new InfiniteSource<String>());

		setupSourceOperator(operator, TimeCharacteristic.EventTime, 0);

		// trigger an async cancel in a bit
		new Thread("canceler") {
			@Override
			public void run() {
				try {
					Thread.sleep(200);
				} catch (InterruptedException ignored) {}
				operator.cancel();
				runner.interrupt();
			}
		}.start();

		// run and wait to be canceled
		try {
			operator.run(new Object(), mock(StreamStatusMaintainer.class), new CollectorOutput<String>(output));
		}
		catch (InterruptedException ignored) {}

		assertTrue(output.isEmpty());
	}

	@Test
	public void testNoMaxWatermarkOnImmediateStop() throws Exception {

		final List<StreamElement> output = new ArrayList<>();

		// regular stream source operator
		final StoppableStreamSource<String, InfiniteSource<String>> operator =
				new StoppableStreamSource<>(new InfiniteSource<String>());

		setupSourceOperator(operator, TimeCharacteristic.EventTime, 0);
		operator.stop();

		// run and stop
		operator.run(new Object(), mock(StreamStatusMaintainer.class), new CollectorOutput<String>(output));

		assertTrue(output.isEmpty());
	}

	@Test
	public void testNoMaxWatermarkOnAsyncStop() throws Exception {

		final List<StreamElement> output = new ArrayList<>();

		// regular stream source operator
		final StoppableStreamSource<String, InfiniteSource<String>> operator =
				new StoppableStreamSource<>(new InfiniteSource<String>());

		setupSourceOperator(operator, TimeCharacteristic.EventTime, 0);

		// trigger an async cancel in a bit
		new Thread("canceler") {
			@Override
			public void run() {
				try {
					Thread.sleep(200);
				} catch (InterruptedException ignored) {}
				operator.stop();
			}
		}.start();

		// run and wait to be stopped
		operator.run(new Object(), mock(StreamStatusMaintainer.class), new CollectorOutput<String>(output));

		assertTrue(output.isEmpty());
	}

	@Test
	public void testAutomaticWatermarkContext() throws Exception {

		// regular stream source operator
		final StoppableStreamSource<String, InfiniteSource<String>> operator =
			new StoppableStreamSource<>(new InfiniteSource<String>());

		long watermarkInterval = 10;
		TestProcessingTimeService processingTimeService = new TestProcessingTimeService();
		processingTimeService.setCurrentTime(0);

		setupSourceOperator(operator, TimeCharacteristic.IngestionTime, watermarkInterval, processingTimeService);

		final List<StreamElement> output = new ArrayList<>();

		StreamSourceContexts.getSourceContext(TimeCharacteristic.IngestionTime,
			operator.getContainingTask().getProcessingTimeService(),
			operator.getContainingTask().getCheckpointLock(),
			operator.getContainingTask().getStreamStatusMaintainer(),
			new CollectorOutput<String>(output),
			operator.getExecutionConfig().getAutoWatermarkInterval(),
			-1);

		// periodically emit the watermarks
		// even though we start from 1 the watermark are still
		// going to be aligned with the watermark interval.

		for (long i = 1; i < 100; i += watermarkInterval)  {
			processingTimeService.setCurrentTime(i);
		}

		assertTrue(output.size() == 9);

		long nextWatermark = 0;
		for (StreamElement el : output) {
			nextWatermark += watermarkInterval;
			Watermark wm = (Watermark) el;
			assertTrue(wm.getTimestamp() == nextWatermark);
		}
	}

	// ------------------------------------------------------------------------

	@SuppressWarnings("unchecked")
	private static <T> void setupSourceOperator(StreamSource<T, ?> operator,
			TimeCharacteristic timeChar,
			long watermarkInterval) {
		setupSourceOperator(operator, timeChar, watermarkInterval, new TestProcessingTimeService());
	}

	@SuppressWarnings("unchecked")
	private static <T> void setupSourceOperator(StreamSource<T, ?> operator,
												TimeCharacteristic timeChar,
												long watermarkInterval,
												final ProcessingTimeService timeProvider) {

		ExecutionConfig executionConfig = new ExecutionConfig();
		executionConfig.setAutoWatermarkInterval(watermarkInterval);

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

	private static final class FiniteSource<T> implements SourceFunction<T>, StoppableFunction {

		@Override
		public void run(SourceContext<T> ctx) {}

		@Override
		public void cancel() {}

		@Override
		public void stop() {}
	}

	private static final class InfiniteSource<T> implements SourceFunction<T>, StoppableFunction {

		private volatile boolean running = true;

		@Override
		public void run(SourceContext<T> ctx) throws Exception {
			while (running) {
				Thread.sleep(20);
			}
		}

		@Override
		public void cancel() {
			running = false;
		}

		@Override
		public void stop() {
			running = false;
		}
	}
}
