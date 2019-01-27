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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.metrics.SimpleHistogram;
import org.apache.flink.runtime.metrics.SumAndCount;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.streaming.util.CollectorOutput;

import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link StreamSource}.
 */
public class StreamSourceTest {

	@Test
	public void testTaskLatencyMetric() throws Exception {
		SourceFunction sourceFunction = new ExampleCountSource();
		StreamSource<Long, ExampleCountSource> streamSource = new StreamSource(sourceFunction);

		TestProcessingTimeService processingTimeService = new TestProcessingTimeService();
		processingTimeService.setCurrentTime(20);

		MockStreamStatusMaintainer mockStreamStatusMaintainer = new MockStreamStatusMaintainer();

		SumAndCount testSumAndCount = new SumAndCount("test");
		assertEquals(0, testSumAndCount.getCounter().getCount());
		assertEquals(0, testSumAndCount.getSum(), 0.000001);

		SourceFunction.SourceContext<Long> ctx = streamSource.getSourceContext(
			TimeCharacteristic.IngestionTime,
			processingTimeService,
			new Object(),
			mockStreamStatusMaintainer,
			new CollectorOutput<>(new ArrayList<>()),
			true,
			1,
			testSumAndCount,
			new SimpleHistogram(),
			40L
		);

		sourceFunction.run(ctx);

		assertEquals(1000, testSumAndCount.getCounter().getCount());
		assertTrue(testSumAndCount.getSum() > 0);
	}

	private class ExampleCountSource implements SourceFunction<Long>, CheckpointedFunction {
		private long count = 0L;
		private volatile boolean isRunning = true;

		private transient ListState<Long> checkpointedCount;

		@Override
		public void run(SourceContext<Long> ctx) {
			while (isRunning && count < 1000) {
				// this synchronized block ensures that state checkpointing,
				// internal state updates and emission of elements are an atomic operation
				synchronized (ctx.getCheckpointLock()) {
					ctx.collect(count);
					count++;
				}
			}
		}

		public void cancel() {
			isRunning = false;
		}

		public void initializeState(FunctionInitializationContext context) throws Exception {
			this.checkpointedCount = context
					.getOperatorStateStore()
					.getListState(new ListStateDescriptor<>("count", Long.class));

			if (context.isRestored()) {
				for (Long count : this.checkpointedCount.get()) {
					this.count = count;
				}
			}
		}

		public void snapshotState(FunctionSnapshotContext context) throws Exception {
			this.checkpointedCount.clear();
			this.checkpointedCount.add(count);
		}
	}

	private static class MockStreamStatusMaintainer implements StreamStatusMaintainer {
		StreamStatus currentStreamStatus = StreamStatus.ACTIVE;

		@Override
		public void toggleStreamStatus(StreamStatus streamStatus) {
			if (!currentStreamStatus.equals(streamStatus)) {
				currentStreamStatus = streamStatus;
			}
		}

		@Override
		public StreamStatus getStreamStatus() {
			return currentStreamStatus;
		}
	}
}
