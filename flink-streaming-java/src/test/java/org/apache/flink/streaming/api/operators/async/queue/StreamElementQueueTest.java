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

package org.apache.flink.streaming.api.operators.async.queue;

import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.apache.flink.streaming.api.operators.async.queue.QueueUtil.popCompleted;
import static org.apache.flink.streaming.api.operators.async.queue.QueueUtil.putSuccessfully;
import static org.apache.flink.streaming.api.operators.async.queue.QueueUtil.putUnsuccessfully;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the basic functionality of {@link StreamElementQueue}. The basic operations consist
 * of putting and polling elements from the queue.
 */
@RunWith(Parameterized.class)
public class StreamElementQueueTest extends TestLogger {
	@Parameterized.Parameters
	public static Collection<AsyncDataStream.OutputMode> outputModes() {
		return Arrays.asList(AsyncDataStream.OutputMode.ORDERED, AsyncDataStream.OutputMode.UNORDERED);
	}

	private final AsyncDataStream.OutputMode outputMode;

	public StreamElementQueueTest(AsyncDataStream.OutputMode outputMode) {
		this.outputMode = Preconditions.checkNotNull(outputMode);
	}

	private StreamElementQueue<Integer> createStreamElementQueue(int capacity) {
		switch (outputMode) {
			case ORDERED:
				return new OrderedStreamElementQueue<>(capacity);
			case UNORDERED:
				return new UnorderedStreamElementQueue<>(capacity);
			default:
				throw new IllegalStateException("Unknown output mode: " + outputMode);
		}
	}

	@Test
	public void testPut() {
		StreamElementQueue<Integer> queue = createStreamElementQueue(2);

		Watermark watermark = new Watermark(0L);
		StreamRecord<Integer> streamRecord = new StreamRecord<>(42, 1L);

		// add two elements to reach capacity
		assertTrue(queue.tryPut(watermark).isPresent());
		assertTrue(queue.tryPut(streamRecord).isPresent());

		assertEquals(2, queue.size());

		// queue full, cannot add new element
		assertFalse(queue.tryPut(new Watermark(2L)).isPresent());

		// check if expected values are returned (for checkpointing)
		assertEquals(Arrays.asList(watermark, streamRecord), queue.values());
	}

	@Test
	public void testPop() {
		StreamElementQueue<Integer> queue = createStreamElementQueue(2);

		// add two elements to reach capacity
		putSuccessfully(queue, new Watermark(0L));
		ResultFuture<Integer> recordResult = putSuccessfully(queue, new StreamRecord<>(42, 1L));

		assertEquals(2, queue.size());

		// remove completed elements (watermarks are always completed)
		assertEquals(Arrays.asList(new Watermark(0L)), popCompleted(queue));
		assertEquals(1, queue.size());

		// now complete the stream record
		recordResult.complete(Collections.singleton(43));

		assertEquals(Arrays.asList(new StreamRecord<>(43, 1L)), popCompleted(queue));
		assertEquals(0, queue.size());
		assertTrue(queue.isEmpty());
	}

	/**
	 * Tests that a put operation fails if the queue is full.
	 */
	@Test
	public void testPutOnFull() throws Exception {
		final StreamElementQueue<Integer> queue = createStreamElementQueue(1);

		// fill up queue
		ResultFuture<Integer> resultFuture = putSuccessfully(queue, new StreamRecord<>(42, 0L));
		assertEquals(1, queue.size());

		// cannot add more
		putUnsuccessfully(queue, new StreamRecord<>(43, 1L));

		// popping the completed element frees the queue again
		resultFuture.complete(Collections.singleton(42 * 42));
		assertEquals(Arrays.asList(new StreamRecord<Integer>(42 * 42, 0L)), popCompleted(queue));

		// now the put operation should complete
		putSuccessfully(queue, new StreamRecord<>(43, 1L));
	}

	/**
	 * Tests two adjacent watermarks can be processed successfully.
	 */
	@Test
	public void testWatermarkOnly() {
		final StreamElementQueue<Integer> queue = createStreamElementQueue(2);

		putSuccessfully(queue, new Watermark(2L));
		putSuccessfully(queue, new Watermark(5L));

		Assert.assertEquals(2, queue.size());
		Assert.assertFalse(queue.isEmpty());

		Assert.assertEquals(Arrays.asList(
				new Watermark(2L),
			new Watermark(5L)),
			popCompleted(queue));
		Assert.assertEquals(0, queue.size());
		Assert.assertTrue(queue.isEmpty());
		Assert.assertEquals(Collections.emptyList(), popCompleted(queue));
	}
}
