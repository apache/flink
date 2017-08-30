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

package org.apache.flink.streaming.api.operators.async;

import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.async.queue.OrderedStreamElementQueue;
import org.apache.flink.streaming.api.operators.async.queue.StreamElementQueue;
import org.apache.flink.streaming.api.operators.async.queue.StreamRecordQueueEntry;
import org.apache.flink.streaming.api.operators.async.queue.WatermarkQueueEntry;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.CollectorOutput;
import org.apache.flink.util.TestLogger;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Tests for {@link Emitter}.
 */
public class EmitterTest extends TestLogger {

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
	 * Tests that the emitter outputs completed stream element queue entries.
	 */
	@Test
	public void testEmitterWithOrderedQueue() throws Exception {
		Object lock = new Object();
		List<StreamElement> list = new ArrayList<>();
		Output<StreamRecord<Integer>> output = new CollectorOutput<>(list);

		List<StreamElement> expected = Arrays.asList(
			new StreamRecord<>(1, 0L),
			new StreamRecord<>(2, 0L),
			new StreamRecord<>(3, 1L),
			new StreamRecord<>(4, 1L),
			new Watermark(3L),
			new StreamRecord<>(5, 4L),
			new StreamRecord<>(6, 4L));

		OperatorActions operatorActions = mock(OperatorActions.class);

		final int capacity = 5;

		StreamElementQueue queue = new OrderedStreamElementQueue(capacity, executor, operatorActions);

		final Emitter<Integer> emitter = new Emitter<>(lock, output, queue, operatorActions);

		final Thread emitterThread = new Thread(emitter);
		emitterThread.start();

		try {
			StreamRecordQueueEntry<Integer> record1 = new StreamRecordQueueEntry<>(new StreamRecord<>(1, 0L));
			StreamRecordQueueEntry<Integer> record2 = new StreamRecordQueueEntry<>(new StreamRecord<>(2, 1L));
			WatermarkQueueEntry watermark1 = new WatermarkQueueEntry(new Watermark(3L));
			StreamRecordQueueEntry<Integer> record3 = new StreamRecordQueueEntry<>(new StreamRecord<>(3, 4L));

			queue.put(record1);
			queue.put(record2);
			queue.put(watermark1);
			queue.put(record3);

			record2.complete(Arrays.asList(3, 4));
			record1.complete(Arrays.asList(1, 2));
			record3.complete(Arrays.asList(5, 6));

			synchronized (lock) {
				while (!queue.isEmpty()) {
					lock.wait();
				}
			}

			Assert.assertEquals(expected, list);
		} finally {
			emitter.stop();
			emitterThread.interrupt();
		}
	}

	/**
	 * Tests that the emitter handles exceptions occurring in the {@link ResultFuture} correctly.
	 */
	@Test
	public void testEmitterWithExceptions() throws Exception {
		Object lock = new Object();
		List<StreamElement> list = new ArrayList<>();
		Output<StreamRecord<Integer>> output = new CollectorOutput<>(list);

		List<StreamElement> expected = Arrays.asList(
			new StreamRecord<>(1, 0L),
			new Watermark(3L));

		OperatorActions operatorActions = mock(OperatorActions.class);

		final int capacity = 3;

		StreamElementQueue queue = new OrderedStreamElementQueue(capacity, executor, operatorActions);

		final Emitter<Integer> emitter = new Emitter<>(lock, output, queue, operatorActions);

		final Thread emitterThread = new Thread(emitter);
		emitterThread.start();

		final Exception testException = new Exception("Test exception");

		try {
			StreamRecordQueueEntry<Integer> record1 = new StreamRecordQueueEntry<>(new StreamRecord<>(1, 0L));
			StreamRecordQueueEntry<Integer> record2 = new StreamRecordQueueEntry<>(new StreamRecord<>(2, 1L));
			WatermarkQueueEntry watermark1 = new WatermarkQueueEntry(new Watermark(3L));

			queue.put(record1);
			queue.put(record2);
			queue.put(watermark1);

			record2.completeExceptionally(testException);
			record1.complete(Arrays.asList(1));

			synchronized (lock) {
				while (!queue.isEmpty()) {
					lock.wait();
				}
			}

			Assert.assertEquals(expected, list);

			ArgumentCaptor<Throwable> argumentCaptor = ArgumentCaptor.forClass(Throwable.class);

			verify(operatorActions).failOperator(argumentCaptor.capture());

			Throwable failureCause = argumentCaptor.getValue();

			Assert.assertNotNull(failureCause.getCause());
			Assert.assertTrue(failureCause.getCause() instanceof ExecutionException);

			Assert.assertNotNull(failureCause.getCause().getCause());
			Assert.assertEquals(testException, failureCause.getCause().getCause());
		} finally {
			emitter.stop();
			emitterThread.interrupt();
		}
	}
}
