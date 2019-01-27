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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunctionV2;
import org.apache.flink.streaming.api.functions.source.SourceRecord;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamSourceV2;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusSubMaintainer;
import org.apache.flink.streaming.runtime.tasks.InputSelector;

import org.junit.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Test for {@link SourceFetcher}.
 */
public class SourceFetcherTest {
	@Test
	public void testFetchAndProcess() throws Exception {
		final FakeSourceFunctionV2 sourceFunctionV2 = new FakeSourceFunctionV2();
		final FakeSourceContext context = new FakeSourceContext();
		final FakeSourceInputProcessor processor = new FakeSourceInputProcessor(
			mock(StreamStatusSubMaintainer.class),
			mock(OneInputStreamOperator.class),
			this,
			mock(TaskMetricGroup.class),
			1);

		{
			final SourceFetcher fetcher = new SourceFetcher(
				mock(InputSelector.InputSelection.class),
				new FakeStreamSourceV2<>(sourceFunctionV2),
				context,
				processor);

			sourceFunctionV2.isFinished.add(true);
			assertFalse(fetcher.fetchAndProcess());
		}

		{
			final SourceFetcher fetcher = new SourceFetcher(
				mock(InputSelector.InputSelection.class),
				new FakeStreamSourceV2<>(sourceFunctionV2),
				context,
				processor);
			sourceFunctionV2.reset();
			context.reset();
			processor.reset();
			sourceFunctionV2.isFinished.add(false);
			sourceFunctionV2.isFinished.add(false);
			sourceFunctionV2.next = null;
			assertFalse(fetcher.fetchAndProcess());
			assertTrue(context.idleMark);
		}

		{
			final SourceFetcher fetcher = new SourceFetcher(
				mock(InputSelector.InputSelection.class),
				new FakeStreamSourceV2<>(sourceFunctionV2),
				context,
				processor);
			sourceFunctionV2.reset();
			context.reset();
			processor.reset();
			sourceFunctionV2.isFinished.add(false);
			sourceFunctionV2.isFinished.add(false);
			sourceFunctionV2.next = SourceRecord.create(1024);
			assertTrue(fetcher.fetchAndProcess());
			assertEquals(1, context.collected.size());
			assertEquals(1024, context.collected.get(0).intValue());
		}

		{
			final SourceFetcher fetcher = new SourceFetcher(
				mock(InputSelector.InputSelection.class),
				new FakeStreamSourceV2<>(sourceFunctionV2),
				context,
				processor);
			sourceFunctionV2.reset();
			context.reset();
			processor.reset();
			sourceFunctionV2.isFinished.add(false);
			sourceFunctionV2.isFinished.add(true);
			sourceFunctionV2.next = SourceRecord.create(9527, 123L);
			assertFalse(fetcher.fetchAndProcess());
			assertEquals(1, context.collected.size());
			assertEquals(9527, context.collected.get(0).intValue());
			assertEquals(1, context.timestamps.size());
			assertEquals(123L, context.timestamps.get(0).longValue());
			assertEquals(1, context.watermarks.size());
			assertEquals(Watermark.MAX_WATERMARK, context.watermarks.get(0));
			assertTrue(processor.endInput);
			assertTrue(processor.released);
		}

		{
			final SourceFetcher fetcher = new SourceFetcher(
				mock(InputSelector.InputSelection.class),
				new FakeStreamSourceV2<>(sourceFunctionV2),
				context,
				processor);
			sourceFunctionV2.reset();
			context.reset();
			processor.reset();
			sourceFunctionV2.isFinished.add(false);
			sourceFunctionV2.isFinished.add(true);
			sourceFunctionV2.next = SourceRecord.create(new Watermark(456L));
			assertFalse(fetcher.fetchAndProcess());
			assertTrue(context.collected.isEmpty());
			assertEquals(2, context.watermarks.size());
			assertEquals(456L, context.watermarks.get(0).getTimestamp());
			assertEquals(Watermark.MAX_WATERMARK, context.watermarks.get(1));
			assertTrue(processor.endInput);
			assertTrue(processor.released);
		}
	}

	class FakeStreamSourceV2<SRC extends SourceFunctionV2<Integer>> extends StreamSourceV2<Integer, SRC> {

		public FakeStreamSourceV2(SRC sourceFunctionV2) {
			super(sourceFunctionV2);
		}
	}

	class FakeSourceFunctionV2 implements SourceFunctionV2<Integer> {

		public Queue<Boolean> isFinished = new ArrayDeque<>();

		public SourceRecord<Integer> next = null;

		@Override
		public boolean isFinished() {
			final Boolean finished = isFinished.poll();
			return finished != null ? finished : false;
		}

		@Override
		public SourceRecord<Integer> next() throws Exception {
			return next;
		}

		@Override
		public void cancel() {

		}

		public void reset() {
			next = null;
			isFinished.clear();
		}
	}

	class FakeSourceContext implements SourceFunction.SourceContext<Integer> {

		public List<Integer> collected = new ArrayList<>();

		public List<Long> timestamps = new ArrayList<>();

		public List<Watermark> watermarks = new ArrayList<>();

		public boolean idleMark = false;

		public void reset() {
			collected.clear();
			timestamps.clear();
			watermarks.clear();
			idleMark = false;
		}

		@Override
		public void collect(Integer element) {
			collected.add(element);
		}

		@Override
		public void collectWithTimestamp(Integer element, long timestamp) {
			collected.add(element);
			timestamps.add(timestamp);
		}

		@Override
		public void emitWatermark(Watermark mark) {
			watermarks.add(mark);
		}

		@Override
		public void markAsTemporarilyIdle() {
			idleMark = true;
		}

		@Override
		public Object getCheckpointLock() {
			return this;
		}

		@Override
		public void close() {

		}
	}

	class FakeSourceInputProcessor extends SourceInputProcessor {

		public boolean endInput = false;

		public boolean released = false;

		public FakeSourceInputProcessor(
			StreamStatusSubMaintainer streamStatusSubMaintainer,
			OneInputStreamOperator sourceOperatorProxy, Object checkpointLock,
			TaskMetricGroup taskMetricGroup, int channelCount) {
			super(streamStatusSubMaintainer, sourceOperatorProxy, checkpointLock, taskMetricGroup,
				channelCount);
		}

		public void reset() {
			endInput = false;
			released = false;
		}

		@Override
		public void endInput() throws Exception {
			endInput = true;
		}

		@Override
		public void release() {
			released = true;
		}
	}
}
