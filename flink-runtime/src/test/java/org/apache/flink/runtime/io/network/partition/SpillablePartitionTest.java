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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.MockProducer;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.SpillableSubpartition;
import org.junit.Before;
import org.junit.Test;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class SpillablePartitionTest {

	private static final IOManager ioManager = new IOManagerAsync();

	private static final int NUM_BUFFERS = 1024;

	private static final int BUFFER_SIZE = 32 * 1024;

	private static final NetworkBufferPool networkBuffers = new NetworkBufferPool(NUM_BUFFERS, BUFFER_SIZE);

	private SpillableSubpartition queue;

	@Before
	public void setUp() {
		this.queue = new SpillableSubpartition(0, mock(ResultPartition.class), ioManager);
	}

	@Test
	public void testConcurrentProduceAndSpill() throws Exception {
		final ExecutorService executor = Executors.newFixedThreadPool(2);

		final AtomicReference<Throwable> error = new AtomicReference<Throwable>();

		final int producerNumBuffersToProduce = 64;
		final BufferPool producerBufferPool = networkBuffers.createBufferPool(NUM_BUFFERS, true);
		final MockProducer producer = new MockProducer(queue, producerBufferPool, producerNumBuffersToProduce, true);
		boolean producerSuccess = false;

		try {
			final TimerTask memoryThief = new TimerTask() {
				@Override
				public void run() {
					try {
						queue.releaseMemory();
					}
					catch (Throwable t) {
						error.set(t);
					}
				}
			};

			// Delay the thief to have producer in produce phase
			int delay = MockProducer.SLEEP_TIME_MS * producerNumBuffersToProduce / 4;

			new Timer().schedule(memoryThief, delay);

			producerSuccess = executor.submit(producer).get(60, TimeUnit.SECONDS);
		}
		finally {
			executor.shutdownNow();

			if (!producerSuccess) {
				Throwable t = error.get();
				if (t != null && t.getMessage() != null) {
					fail("Error during produce: " + t.getMessage());
				}

				fail("Error during produce (but no Exception thrown).");
			}

			assertFalse("Supposed to test spilling behaviour, but queue is still in memory.", queue.isInMemory());

			producerBufferPool.lazyDestroy();

			assertEquals("Resource leak: did not return all buffers to network buffer pool.", NUM_BUFFERS, networkBuffers.getNumberOfAvailableMemorySegments());
		}
	}

	@Test
	public void testInMemoryProduceConsume () {

	}

	@Test
	public void testSpilledProduceConsume() throws Exception {

	}

	@Test
	public void testSpillingProduceConsume() throws Exception {

	}
}
