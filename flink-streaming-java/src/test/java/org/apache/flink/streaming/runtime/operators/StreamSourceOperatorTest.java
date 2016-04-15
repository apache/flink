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
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StoppableStreamSource;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("serial")
public class StreamSourceOperatorTest {
	
	@Test
	public void testEmitMaxWatermarkForFiniteSource() throws Exception {

		// regular stream source operator
		StreamSource<String, FiniteSource<String>> operator = 
				new StreamSource<>(new FiniteSource<String>());
		
		final List<StreamElement> output = new ArrayList<>();
		
		setupSourceOperator(operator);
		operator.run(new Object(), new CollectorOutput<String>(output));
		
		assertEquals(1, output.size());
		assertEquals(Watermark.MAX_WATERMARK, output.get(0));
	}

	@Test
	public void testNoMaxWatermarkOnImmediateCancel() throws Exception {

		final List<StreamElement> output = new ArrayList<>();

		// regular stream source operator
		final StreamSource<String, InfiniteSource<String>> operator =
				new StreamSource<>(new InfiniteSource<String>());


		setupSourceOperator(operator);
		operator.cancel();

		// run and exit
		operator.run(new Object(), new CollectorOutput<String>(output));
		
		assertTrue(output.isEmpty());
	}
	
	@Test
	public void testNoMaxWatermarkOnAsyncCancel() throws Exception {

		final List<StreamElement> output = new ArrayList<>();
		final Thread runner = Thread.currentThread();
		
		// regular stream source operator
		final StreamSource<String, InfiniteSource<String>> operator =
				new StreamSource<>(new InfiniteSource<String>());

		
		setupSourceOperator(operator);
		
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
			operator.run(new Object(), new CollectorOutput<String>(output));
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


		setupSourceOperator(operator);
		operator.stop();

		// run and stop
		operator.run(new Object(), new CollectorOutput<String>(output));

		assertTrue(output.isEmpty());
	}

	@Test
	public void testNoMaxWatermarkOnAsyncStop() throws Exception {

		final List<StreamElement> output = new ArrayList<>();

		// regular stream source operator
		final StoppableStreamSource<String, InfiniteSource<String>> operator =
				new StoppableStreamSource<>(new InfiniteSource<String>());


		setupSourceOperator(operator);

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
		operator.run(new Object(), new CollectorOutput<String>(output));

		assertTrue(output.isEmpty());
	}
	
	
	// ------------------------------------------------------------------------
	
	@SuppressWarnings("unchecked")
	private static <T> void setupSourceOperator(StreamSource<T, ?> operator) {
		ExecutionConfig executionConfig = new ExecutionConfig();
		StreamConfig cfg = new StreamConfig(new Configuration());
		
		cfg.setTimeCharacteristic(TimeCharacteristic.EventTime);

		Environment env = new DummyEnvironment("MockTwoInputTask", 1, 0);
		
		StreamTask<?, ?> mockTask = mock(StreamTask.class);
		when(mockTask.getName()).thenReturn("Mock Task");
		when(mockTask.getCheckpointLock()).thenReturn(new Object());
		when(mockTask.getConfiguration()).thenReturn(cfg);
		when(mockTask.getEnvironment()).thenReturn(env);
		when(mockTask.getExecutionConfig()).thenReturn(executionConfig);
		when(mockTask.getAccumulatorMap()).thenReturn(Collections.<String, Accumulator<?, ?>>emptyMap());

		operator.setup(mockTask, cfg, (Output< StreamRecord<T>>) mock(Output.class));
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

	// ------------------------------------------------------------------------
	
	private static class CollectorOutput<T> implements Output<StreamRecord<T>> {

		private final List<StreamElement> list;

		private CollectorOutput(List<StreamElement> list) {
			this.list = list;
		}

		@Override
		public void emitWatermark(Watermark mark) {
			list.add(mark);
		}

		@Override
		public void collect(StreamRecord<T> record) {
			list.add(record);
		}

		@Override
		public void close() {}
	}
}
