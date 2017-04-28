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

import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.disk.iomanager.AsynchronousBufferFileWriter;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.junit.AfterClass;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SpillableSubpartitionTest extends SubpartitionTestBase {

	/** Executor service for concurrent produce/consume tests */
	private final static ExecutorService executorService = Executors.newCachedThreadPool();

	/** Asynchronous I/O manager */
	private static final IOManager ioManager = new IOManagerAsync();

	@AfterClass
	public static void shutdown() {
		executorService.shutdownNow();
		ioManager.shutdown();
	}

	@Override
	SpillableSubpartition createSubpartition() {
		ResultPartition parent = mock(ResultPartition.class);
		BufferProvider bufferProvider = mock(BufferProvider.class);
		when(parent.getBufferProvider()).thenReturn(bufferProvider);
		when(bufferProvider.getMemorySegmentSize()).thenReturn(32 * 1024);
		return new SpillableSubpartition(0, parent, ioManager);
	}

	/**
	 * Tests a fix for FLINK-2384.
	 *
	 * @see <a href="https://issues.apache.org/jira/browse/FLINK-2384">FLINK-2384</a>
	 */
	@Test
	public void testConcurrentFinishAndReleaseMemory() throws Exception {
		// Latches to blocking
		final CountDownLatch doneLatch = new CountDownLatch(1);
		final CountDownLatch blockLatch = new CountDownLatch(1);

		// Blocking spill writer (blocks on the close call)
		AsynchronousBufferFileWriter spillWriter = mock(AsynchronousBufferFileWriter.class);
		doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				blockLatch.countDown();
				doneLatch.await();
				return null;
			}
		}).when(spillWriter).close();

		// Mock I/O manager returning the blocking spill writer
		IOManager ioManager = mock(IOManager.class);
		when(ioManager.createBufferFileWriter(any(FileIOChannel.ID.class)))
			.thenReturn(spillWriter);

		// The partition
		final SpillableSubpartition partition = new SpillableSubpartition(
			0, mock(ResultPartition.class), ioManager);

		// Spill the partition initially (creates the spill writer)
		assertEquals(0, partition.releaseMemory());

		ExecutorService executor = Executors.newSingleThreadExecutor();

		// Finish the partition (this blocks because of the mock blocking writer)
		Future<Void> blockingFinish = executor.submit(new Callable<Void>() {
			@Override
			public Void call() throws Exception {
				partition.finish();
				return null;
			}
		});

		// Ensure that the blocking call has been made
		blockLatch.await();

		// This call needs to go through. FLINK-2384 discovered a bug, in
		// which the finish call was holding a lock, which was leading to a
		// deadlock when another operation on the partition was happening.
		partition.releaseMemory();

		// Check that the finish call succeeded w/o problems as well to avoid
		// false test successes.
		doneLatch.countDown();
		blockingFinish.get();
	}

	/**
	 * Tests a fix for FLINK-2412.
	 *
	 * @see <a href="https://issues.apache.org/jira/browse/FLINK-2412">FLINK-2412</a>
	 */
	@Test
	public void testReleasePartitionAndGetNext() throws Exception {
		// Create partition and add some buffers
		SpillableSubpartition partition = createSubpartition();

		partition.finish();

		// Create the read view
		ResultSubpartitionView readView = spy(partition
			.createReadView(new BufferAvailabilityListener() {
				@Override
				public void notifyBuffersAvailable(long numBuffers) {

				}
			}));

		// The released state check (of the parent) needs to be independent
		// of the released state of the view.
		doNothing().when(readView).releaseAllResources();

		// Release the partition, but the view does not notice yet.
		partition.release();

		assertNull(readView.getNextBuffer());
	}

	/**
	 * Tests that a spilled partition is correctly read back in via a spilled
	 * read view.
	 */
	@Test
	public void testConsumeSpilledPartition() throws Exception {
		SpillableSubpartition partition = createSubpartition();

		Buffer buffer = new Buffer(MemorySegmentFactory.allocateUnpooledSegment(4096), FreeingBufferRecycler.INSTANCE);
		buffer.retain();
		buffer.retain();

		partition.add(buffer);
		partition.add(buffer);
		partition.add(buffer);

		assertEquals(3, partition.releaseMemory());

		partition.finish();

		BufferAvailabilityListener listener = mock(BufferAvailabilityListener.class);
		SpilledSubpartitionView reader = (SpilledSubpartitionView) partition.createReadView(listener);

		verify(listener, times(1)).notifyBuffersAvailable(eq(4L));

		Buffer read = reader.getNextBuffer();
		assertNotNull(read);
		read.recycle();

		read = reader.getNextBuffer();
		assertNotNull(read);
		read.recycle();

		read = reader.getNextBuffer();
		assertNotNull(read);
		read.recycle();

		// End of partition
		read = reader.getNextBuffer();
		assertNotNull(read);
		assertEquals(EndOfPartitionEvent.class, EventSerializer.fromBuffer(read, ClassLoader.getSystemClassLoader()).getClass());
		read.recycle();
	}

	/**
	 * Tests that a spilled partition is correctly read back in via a spilled
	 * read view.
	 */
	@Test
	public void testConsumeSpillablePartitionSpilledDuringConsume() throws Exception {
		SpillableSubpartition partition = createSubpartition();

		Buffer buffer = new Buffer(MemorySegmentFactory.allocateUnpooledSegment(4096), FreeingBufferRecycler.INSTANCE);
		buffer.retain();
		buffer.retain();

		partition.add(buffer);
		partition.add(buffer);
		partition.add(buffer);
		partition.finish();

		AwaitableBufferAvailablityListener listener = new AwaitableBufferAvailablityListener();
		SpillableSubpartitionView reader = (SpillableSubpartitionView) partition.createReadView(listener);

		// Initial notification
		assertEquals(1, listener.getNumNotifiedBuffers());

		Buffer read = reader.getNextBuffer();
		assertNotNull(read);
		read.recycle();
		assertEquals(2, listener.getNumNotifiedBuffers());

		// Spill now
		assertEquals(2, partition.releaseMemory());

		listener.awaitNotifications(4, 30_000);
		assertEquals(4, listener.getNumNotifiedBuffers());

		read = reader.getNextBuffer();
		assertNotNull(read);
		read.recycle();

		read = reader.getNextBuffer();
		assertNotNull(read);
		read.recycle();

		// End of partition
		read = reader.getNextBuffer();
		assertNotNull(read);
		assertEquals(EndOfPartitionEvent.class, EventSerializer.fromBuffer(read, ClassLoader.getSystemClassLoader()).getClass());
		read.recycle();
	}

	private static class AwaitableBufferAvailablityListener implements BufferAvailabilityListener {

		private long numNotifiedBuffers;

		@Override
		public void notifyBuffersAvailable(long numBuffers) {
			numNotifiedBuffers += numBuffers;
		}

		long getNumNotifiedBuffers() {
			return numNotifiedBuffers;
		}

		void awaitNotifications(long awaitedNumNotifiedBuffers, long timeoutMillis) throws InterruptedException {
			long deadline = System.currentTimeMillis() + timeoutMillis;
			while (numNotifiedBuffers < awaitedNumNotifiedBuffers && System.currentTimeMillis() < deadline) {
				Thread.sleep(1);
			}
		}
	}
}
