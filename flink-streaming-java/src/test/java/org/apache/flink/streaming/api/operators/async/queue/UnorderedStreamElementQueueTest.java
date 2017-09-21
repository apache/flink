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

import org.apache.flink.streaming.api.operators.async.OperatorActions;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.TestLogger;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * {@link UnorderedStreamElementQueue} specific tests.
 */
public class UnorderedStreamElementQueueTest extends TestLogger {
	private static final long timeout = 10000L;
	private static ExecutorService executor;

	@BeforeClass
	public static void setup() {
		executor = Executors.newFixedThreadPool(3);
	}

	@AfterClass
	public static void shutdown() {
		executor.shutdown();

		try {
			if (!executor.awaitTermination(timeout, TimeUnit.MILLISECONDS)) {
				executor.shutdownNow();
			}
		} catch (InterruptedException interrupted) {
			executor.shutdownNow();

			Thread.currentThread().interrupt();
		}
	}

	/**
	 * Tests that only elements before the oldest watermark are returned if they are completed.
	 */
	@Test
	public void testCompletionOrder() throws Exception {
		OperatorActions operatorActions = mock(OperatorActions.class);

		final UnorderedStreamElementQueue queue = new UnorderedStreamElementQueue(8, executor, operatorActions);

		StreamRecordQueueEntry<Integer> record1 = new StreamRecordQueueEntry<>(new StreamRecord<>(1, 0L));
		StreamRecordQueueEntry<Integer> record2 = new StreamRecordQueueEntry<>(new StreamRecord<>(2, 1L));
		WatermarkQueueEntry watermark1 = new WatermarkQueueEntry(new Watermark(2L));
		StreamRecordQueueEntry<Integer> record3 = new StreamRecordQueueEntry<>(new StreamRecord<>(3, 3L));
		StreamRecordQueueEntry<Integer> record4 = new StreamRecordQueueEntry<>(new StreamRecord<>(4, 4L));
		WatermarkQueueEntry watermark2 = new WatermarkQueueEntry(new Watermark(5L));
		StreamRecordQueueEntry<Integer> record5 = new StreamRecordQueueEntry<>(new StreamRecord<>(5, 6L));
		StreamRecordQueueEntry<Integer> record6 = new StreamRecordQueueEntry<>(new StreamRecord<>(6, 7L));

		List<StreamElementQueueEntry<?>> entries = Arrays.asList(record1, record2, watermark1, record3,
			record4, watermark2, record5, record6);

		// The queue should look like R1, R2, W1, R3, R4, W2, R5, R6
		for (StreamElementQueueEntry<?> entry : entries) {
			queue.put(entry);
		}

		Assert.assertTrue(8 == queue.size());

		CompletableFuture<AsyncResult> firstPoll = CompletableFuture.supplyAsync(
			() -> {
				try {
					return queue.poll();
				} catch (InterruptedException e) {
					throw new CompletionException(e);
				}
			},
			executor);

		// this should not fulfill the poll, because R3 is behind W1
		record3.complete(Collections.<Integer>emptyList());

		Thread.sleep(10L);

		Assert.assertFalse(firstPoll.isDone());

		record2.complete(Collections.<Integer>emptyList());

		Assert.assertEquals(record2, firstPoll.get());

		CompletableFuture<AsyncResult> secondPoll = CompletableFuture.supplyAsync(
			() -> {
				try {
					return queue.poll();
				} catch (InterruptedException e) {
					throw new CompletionException(e);
				}
			},
			executor);

		record6.complete(Collections.<Integer>emptyList());
		record4.complete(Collections.<Integer>emptyList());

		Thread.sleep(10L);

		// The future should not be completed because R1 has not been completed yet
		Assert.assertFalse(secondPoll.isDone());

		record1.complete(Collections.<Integer>emptyList());

		Assert.assertEquals(record1, secondPoll.get());

		// Now W1, R3, R4 and W2 are completed and should be pollable
		Assert.assertEquals(watermark1, queue.poll());

		// The order of R3 and R4 is not specified
		Set<AsyncResult> expected = new HashSet<>(2);
		expected.add(record3);
		expected.add(record4);

		Set<AsyncResult> actual = new HashSet<>(2);

		actual.add(queue.poll());
		actual.add(queue.poll());

		Assert.assertEquals(expected, actual);

		Assert.assertEquals(watermark2, queue.poll());

		// since R6 has been completed before and W2 has been consumed, we should be able to poll
		// this record as well
		Assert.assertEquals(record6, queue.poll());

		// only R5 left in the queue
		Assert.assertTrue(1 == queue.size());

		CompletableFuture<AsyncResult> thirdPoll = CompletableFuture.supplyAsync(
			() -> {
				try {
					return queue.poll();
				} catch (InterruptedException e) {
					throw new CompletionException(e);
				}
			},
			executor);

		Thread.sleep(10L);

		Assert.assertFalse(thirdPoll.isDone());

		record5.complete(Collections.<Integer>emptyList());

		Assert.assertEquals(record5, thirdPoll.get());

		Assert.assertTrue(queue.isEmpty());

		verify(operatorActions, never()).failOperator(any(Exception.class));
	}
}
