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

import org.apache.flink.runtime.io.disk.iomanager.AsynchronousBufferFileWriter;
import org.apache.flink.runtime.io.disk.iomanager.BufferFileWriter;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsyncWithNoOpBufferFileWriter;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.createFilledBufferConsumer;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link SpillableSubpartition}.
 */
public class SpillableSubpartitionTest extends SubpartitionTestBase {
	private static final int BUFFER_DATA_SIZE = 4096;

	@Rule
	public ExpectedException exception = ExpectedException.none();

	/** Executor service for concurrent produce/consume tests. */
	private static final ExecutorService executorService = Executors.newCachedThreadPool();

	/** Asynchronous I/O manager. */
	private static final IOManager ioManager = new IOManagerAsync();

	@AfterClass
	public static void shutdown() {
		executorService.shutdownNow();
		ioManager.shutdown();
	}

	@Override
	SpillableSubpartition createSubpartition() {
		return createSubpartition(ioManager);
	}

	private static SpillableSubpartition createSubpartition(IOManager ioManager) {
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
			.createReadView(new NoOpBufferAvailablityListener()));

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

		BufferConsumer bufferConsumer = createFilledBufferConsumer(BUFFER_DATA_SIZE, BUFFER_DATA_SIZE);

		partition.add(bufferConsumer.copy());
		partition.add(bufferConsumer.copy());
		partition.add(BufferBuilderTestUtils.createEventBufferConsumer(BUFFER_DATA_SIZE));
		partition.add(bufferConsumer);

		assertEquals(4, partition.getTotalNumberOfBuffers());
		assertEquals(3, partition.getBuffersInBacklog());
		//TODO: re-enable this?
//		assertEquals(BUFFER_DATA_SIZE * 4, partition.getTotalNumberOfBytes());

		assertFalse(bufferConsumer.isRecycled());
		assertEquals(4, partition.releaseMemory());
		// now the bufferConsumer may be freed, depending on the timing of the write operation
		// -> let's do this check at the end of the test (to save some time)
		// still same statistics
		assertEquals(4, partition.getTotalNumberOfBuffers());
		assertEquals(3, partition.getBuffersInBacklog());
		assertEquals(BUFFER_DATA_SIZE * 4, partition.getTotalNumberOfBytes());

		partition.finish();
		// + one EndOfPartitionEvent
		assertEquals(5, partition.getTotalNumberOfBuffers());
		assertEquals(3, partition.getBuffersInBacklog());
		assertEquals(BUFFER_DATA_SIZE * 4 + 4, partition.getTotalNumberOfBytes());

		AwaitableBufferAvailablityListener listener = new AwaitableBufferAvailablityListener();
		SpilledSubpartitionView reader = (SpilledSubpartitionView) partition.createReadView(listener);

		assertEquals(1, listener.getNumNotifications());

		assertFalse(reader.nextBufferIsEvent()); // buffer
		BufferAndBacklog read = reader.getNextBuffer();
		assertNotNull(read);
		assertTrue(read.buffer().isBuffer());
		assertEquals(2, partition.getBuffersInBacklog());
		assertEquals(partition.getBuffersInBacklog(), read.buffersInBacklog());
		assertNotSame(bufferConsumer, read);
		assertFalse(read.buffer().isRecycled());
		read.buffer().recycleBuffer();
		assertTrue(read.buffer().isRecycled());
		assertFalse(read.nextBufferIsEvent());

		assertFalse(reader.nextBufferIsEvent()); // buffer
		read = reader.getNextBuffer();
		assertNotNull(read);
		assertTrue(read.buffer().isBuffer());
		assertEquals(1, partition.getBuffersInBacklog());
		assertEquals(partition.getBuffersInBacklog(), read.buffersInBacklog());
		assertNotSame(bufferConsumer, read);
		assertFalse(read.buffer().isRecycled());
		read.buffer().recycleBuffer();
		assertTrue(read.buffer().isRecycled());
		assertTrue(read.nextBufferIsEvent());

		assertTrue(reader.nextBufferIsEvent()); // event
		read = reader.getNextBuffer();
		assertNotNull(read);
		assertFalse(read.buffer().isBuffer());
		assertEquals(1, partition.getBuffersInBacklog());
		assertEquals(partition.getBuffersInBacklog(), read.buffersInBacklog());
		read.buffer().recycleBuffer();
		assertFalse(read.nextBufferIsEvent());

		assertFalse(reader.nextBufferIsEvent()); // buffer
		read = reader.getNextBuffer();
		assertNotNull(read);
		assertTrue(read.buffer().isBuffer());
		assertEquals(0, partition.getBuffersInBacklog());
		assertEquals(partition.getBuffersInBacklog(), read.buffersInBacklog());
		assertFalse(read.buffer().isRecycled());
		read.buffer().recycleBuffer();
		assertTrue(read.buffer().isRecycled());
		assertTrue(read.nextBufferIsEvent());

		assertTrue(reader.nextBufferIsEvent()); // end of partition event
		read = reader.getNextBuffer();
		assertNotNull(read);
		assertFalse(read.buffer().isBuffer());
		assertEquals(0, partition.getBuffersInBacklog());
		assertEquals(partition.getBuffersInBacklog(), read.buffersInBacklog());
		assertEquals(EndOfPartitionEvent.class,
			EventSerializer.fromBuffer(read.buffer(), ClassLoader.getSystemClassLoader()).getClass());
		assertFalse(read.buffer().isRecycled());
		read.buffer().recycleBuffer();
		assertTrue(read.buffer().isRecycled());
		assertFalse(read.nextBufferIsEvent());

		// finally check that the bufferConsumer has been freed after a successful (or failed) write
		final long deadline = System.currentTimeMillis() + 30_000L; // 30 secs
		while (!bufferConsumer.isRecycled() && System.currentTimeMillis() < deadline) {
			Thread.sleep(1);
		}
		assertTrue(bufferConsumer.isRecycled());
	}

	/**
	 * Tests that a spilled partition is correctly read back in via a spilled
	 * read view.
	 */
	@Test
	public void testConsumeSpillablePartitionSpilledDuringConsume() throws Exception {
		SpillableSubpartition partition = createSubpartition();

		BufferConsumer bufferConsumer = createFilledBufferConsumer(BUFFER_DATA_SIZE, BUFFER_DATA_SIZE);

		partition.add(bufferConsumer.copy());
		partition.add(bufferConsumer.copy());
		partition.add(BufferBuilderTestUtils.createEventBufferConsumer(BUFFER_DATA_SIZE));
		partition.add(bufferConsumer);
		partition.finish();

		assertEquals(5, partition.getTotalNumberOfBuffers());
		assertEquals(3, partition.getBuffersInBacklog());
		//TODO: re-enable this?
//		assertEquals(BUFFER_DATA_SIZE * 4 + 4, partition.getTotalNumberOfBytes());

		AwaitableBufferAvailablityListener listener = new AwaitableBufferAvailablityListener();
		SpillableSubpartitionView reader = (SpillableSubpartitionView) partition.createReadView(listener);

		// Initial notification
		assertEquals(1, listener.getNumNotifications());
		assertFalse(bufferConsumer.isRecycled());

		assertFalse(reader.nextBufferIsEvent());
		BufferAndBacklog read = reader.getNextBuffer(); // first buffer (non-spilled)
		assertNotNull(read);
		assertTrue(read.buffer().isBuffer());
		assertEquals(2, partition.getBuffersInBacklog());
		assertEquals(partition.getBuffersInBacklog(), read.buffersInBacklog());
		read.buffer().recycleBuffer();
		assertEquals(2, listener.getNumNotifications());
		assertFalse(bufferConsumer.isRecycled());
		assertFalse(read.nextBufferIsEvent());

		// Spill now
		assertEquals(3, partition.releaseMemory());
		assertFalse(bufferConsumer.isRecycled()); // still one in the reader!
		// still same statistics:
		assertEquals(5, partition.getTotalNumberOfBuffers());
		assertEquals(2, partition.getBuffersInBacklog());
		//TODO: re-enable this?
//		assertEquals(BUFFER_DATA_SIZE * 4 + 4, partition.getTotalNumberOfBytes());

		listener.awaitNotifications(3, 30_000);
		assertEquals(3, listener.getNumNotifications());

		assertFalse(reader.nextBufferIsEvent()); // second buffer (retained in SpillableSubpartition#nextBuffer)
		read = reader.getNextBuffer();
		assertNotNull(read);
		assertTrue(read.buffer().isBuffer());
		assertEquals(1, partition.getBuffersInBacklog());
		assertEquals(partition.getBuffersInBacklog(), read.buffersInBacklog());
		read.buffer().recycleBuffer();
		// now the bufferConsumer may be freed, depending on the timing of the write operation
		// -> let's do this check at the end of the test (to save some time)
		assertTrue(read.nextBufferIsEvent());

		assertTrue(reader.nextBufferIsEvent()); // the event (spilled)
		read = reader.getNextBuffer();
		assertNotNull(read);
		assertFalse(read.buffer().isBuffer());
		assertEquals(1, partition.getBuffersInBacklog());
		assertEquals(partition.getBuffersInBacklog(), read.buffersInBacklog());
		read.buffer().recycleBuffer();
		assertFalse(read.nextBufferIsEvent());

		assertFalse(reader.nextBufferIsEvent()); // last buffer (spilled)
		read = reader.getNextBuffer();
		assertNotNull(read);
		assertTrue(read.buffer().isBuffer());
		assertEquals(0, partition.getBuffersInBacklog());
		assertEquals(partition.getBuffersInBacklog(), read.buffersInBacklog());
		assertFalse(read.buffer().isRecycled());
		read.buffer().recycleBuffer();
		assertTrue(read.buffer().isRecycled());
		assertTrue(read.nextBufferIsEvent());

		// End of partition
		assertTrue(reader.nextBufferIsEvent());
		read = reader.getNextBuffer();
		assertNotNull(read);
		assertEquals(0, partition.getBuffersInBacklog());
		assertEquals(partition.getBuffersInBacklog(), read.buffersInBacklog());
		assertEquals(EndOfPartitionEvent.class,
			EventSerializer.fromBuffer(read.buffer(), ClassLoader.getSystemClassLoader()).getClass());
		assertFalse(read.buffer().isRecycled());
		read.buffer().recycleBuffer();
		assertTrue(read.buffer().isRecycled());
		assertFalse(read.nextBufferIsEvent());

		// finally check that the bufferConsumer has been freed after a successful (or failed) write
		final long deadline = System.currentTimeMillis() + 30_000L; // 30 secs
		while (!bufferConsumer.isRecycled() && System.currentTimeMillis() < deadline) {
			Thread.sleep(1);
		}
		assertTrue(bufferConsumer.isRecycled());
	}

	/**
	 * Tests {@link SpillableSubpartition#add(BufferConsumer)} with a spillable finished partition.
	 */
	@Test
	public void testAddOnFinishedSpillablePartition() throws Exception {
		testAddOnFinishedPartition(false);
	}

	/**
	 * Tests {@link SpillableSubpartition#add(BufferConsumer)} with a spilled finished partition.
	 */
	@Test
	public void testAddOnFinishedSpilledPartition() throws Exception {
		testAddOnFinishedPartition(true);
	}

	/**
	 * Tests {@link SpillableSubpartition#add(BufferConsumer)} with a finished partition.
	 *
	 * @param spilled
	 * 		whether the partition should be spilled to disk (<tt>true</tt>) or not (<tt>false</tt>,
	 * 		spillable).
	 */
	private void testAddOnFinishedPartition(boolean spilled) throws Exception {
		SpillableSubpartition partition = createSubpartition();
		if (spilled) {
			assertEquals(0, partition.releaseMemory());
		}
		partition.finish();
		// finish adds an EndOfPartitionEvent
		assertEquals(1, partition.getTotalNumberOfBuffers());
		//TODO: re-enable this?
//		assertEquals(4, partition.getTotalNumberOfBytes());

		BufferConsumer buffer = createFilledBufferConsumer(BUFFER_DATA_SIZE, BUFFER_DATA_SIZE);
		try {
			partition.add(buffer);
		} finally {
			if (!buffer.isRecycled()) {
				buffer.close();
				Assert.fail("buffer not recycled");
			}
		}
		// still same statistics
		assertEquals(1, partition.getTotalNumberOfBuffers());
		//TODO: re-enable this?
//		assertEquals(4, partition.getTotalNumberOfBytes());
	}

	@Test
	public void testAddOnReleasedSpillablePartition() throws Exception {
		testAddOnReleasedPartition(false);
	}

	@Test
	public void testAddOnReleasedSpilledPartition() throws Exception {
		testAddOnReleasedPartition(true);
	}

	/**
	 * Tests {@link SpillableSubpartition#add(BufferConsumer)} with a released partition.
	 *
	 * @param spilled
	 * 		whether the partition should be spilled to disk (<tt>true</tt>) or not (<tt>false</tt>,
	 * 		spillable).
	 */
	private void testAddOnReleasedPartition(boolean spilled) throws Exception {
		SpillableSubpartition partition = createSubpartition();
		partition.release();
		if (spilled) {
			assertEquals(0, partition.releaseMemory());
		}

		BufferConsumer buffer = createFilledBufferConsumer(BUFFER_DATA_SIZE, BUFFER_DATA_SIZE);
		boolean bufferRecycled;
		try {
			partition.add(buffer);
		} finally {
			bufferRecycled = buffer.isRecycled();
			if (!bufferRecycled) {
				buffer.close();
			}
		}
		if (!bufferRecycled) {
			Assert.fail("buffer not recycled");
		}
		assertEquals(0, partition.getTotalNumberOfBuffers());
		assertEquals(0, partition.getTotalNumberOfBytes());
	}

	/**
	 * Tests {@link SpillableSubpartition#add(BufferConsumer)} with a spilled partition where adding the
	 * write request fails with an exception.
	 */
	@Test
	public void testAddOnSpilledPartitionWithSlowWriter() throws Exception {
		// simulate slow writer by a no-op write operation
		IOManager ioManager = new IOManagerAsyncWithNoOpBufferFileWriter();
		SpillableSubpartition partition = createSubpartition(ioManager);
		assertEquals(0, partition.releaseMemory());

		BufferConsumer buffer = createFilledBufferConsumer(BUFFER_DATA_SIZE, BUFFER_DATA_SIZE);
		boolean bufferRecycled;
		try {
			partition.add(buffer);
		} finally {
			ioManager.shutdown();
			bufferRecycled = buffer.isRecycled();
			if (!bufferRecycled) {
				buffer.close();
			}
		}
		if (bufferRecycled) {
			Assert.fail("buffer recycled before the write operation completed");
		}
		assertEquals(1, partition.getTotalNumberOfBuffers());
		assertEquals(BUFFER_DATA_SIZE, partition.getTotalNumberOfBytes());
	}

	/**
	 * Tests {@link SpillableSubpartition#releaseMemory()} with a spillable partition without a view
	 * but with a writer that does not do any write to check for correct buffer recycling.
	 */
	@Test
	public void testReleaseOnSpillablePartitionWithoutViewWithSlowWriter() throws Exception {
		testReleaseOnSpillablePartitionWithSlowWriter(false);
	}

	/**
	 * Tests {@link SpillableSubpartition#releaseMemory()} with a spillable partition which has a
	 * view associated with it and a writer that does not do any write to check for correct buffer
	 * recycling.
	 */
	@Test
	public void testReleaseOnSpillablePartitionWithViewWithSlowWriter() throws Exception {
		testReleaseOnSpillablePartitionWithSlowWriter(true);
	}

	/**
	 * Tests {@link SpillableSubpartition#releaseMemory()} with a spillable partition which has a a
	 * writer that does not do any write to check for correct buffer recycling.
	 */
	private void testReleaseOnSpillablePartitionWithSlowWriter(boolean createView) throws Exception {
		// simulate slow writer by a no-op write operation
		IOManager ioManager = new IOManagerAsyncWithNoOpBufferFileWriter();
		SpillableSubpartition partition = createSubpartition(ioManager);

		BufferConsumer buffer1 = createFilledBufferConsumer(BUFFER_DATA_SIZE, BUFFER_DATA_SIZE);
		BufferConsumer buffer2 = createFilledBufferConsumer(BUFFER_DATA_SIZE, BUFFER_DATA_SIZE);
		try {
			// we need two buffers because the view will use one of them and not release it
			partition.add(buffer1);
			partition.add(buffer2);
			assertFalse("buffer1 should not be recycled (still in the queue)", buffer1.isRecycled());
			assertFalse("buffer2 should not be recycled (still in the queue)", buffer2.isRecycled());
			assertEquals(2, partition.getTotalNumberOfBuffers());
			//TODO: re-enable this?
//			assertEquals(BUFFER_DATA_SIZE * 2, partition.getTotalNumberOfBytes());

			if (createView) {
				// Create a read view
				partition.finish();
				partition.createReadView(new NoOpBufferAvailablityListener());
			}

			// one instance of the buffers is placed in the view's nextBuffer and not released
			// (if there is no view, there will be no additional EndOfPartitionEvent)
			assertEquals(2, partition.releaseMemory());
			assertFalse("buffer1 should not be recycled (advertised as nextBuffer)", buffer1.isRecycled());
			assertFalse("buffer2 should not be recycled (not written yet)", buffer2.isRecycled());
		} finally {
			ioManager.shutdown();
			if (!buffer1.isRecycled()) {
				buffer1.close();
			}
			if (!buffer2.isRecycled()) {
				buffer2.close();
			}
		}
		// note: a view requires a finished partition which has an additional EndOfPartitionEvent
		assertEquals(2 + (createView ? 1 : 0), partition.getTotalNumberOfBuffers());
		//TODO: re-enable this?
//		assertEquals(BUFFER_DATA_SIZE * 2 + (createView ? 4 : 0), partition.getTotalNumberOfBytes());
	}

	/**
	 * Tests {@link SpillableSubpartition#add(BufferConsumer)} with a spilled partition where adding the
	 * write request fails with an exception.
	 */
	@Test
	public void testAddOnSpilledPartitionWithFailingWriter() throws Exception {
		IOManager ioManager = new IOManagerAsyncWithClosedBufferFileWriter();
		SpillableSubpartition partition = createSubpartition(ioManager);
		assertEquals(0, partition.releaseMemory());

		exception.expect(IOException.class);

		BufferConsumer buffer = createFilledBufferConsumer(BUFFER_DATA_SIZE, BUFFER_DATA_SIZE);
		boolean bufferRecycled;
		try {
			partition.add(buffer);
		} finally {
			ioManager.shutdown();
			bufferRecycled = buffer.isRecycled();
			if (!bufferRecycled) {
				buffer.close();
			}
		}
		if (!bufferRecycled) {
			Assert.fail("buffer not recycled");
		}
		assertEquals(0, partition.getTotalNumberOfBuffers());
		assertEquals(0, partition.getTotalNumberOfBytes());
	}

	/**
	 * Tests cleanup of {@link SpillableSubpartition#release()} with a spillable partition and no
	 * read view attached.
	 */
	@Test
	public void testCleanupReleasedSpillablePartitionNoView() throws Exception {
		testCleanupReleasedPartition(false, false);
	}

	/**
	 * Tests cleanup of {@link SpillableSubpartition#release()} with a spillable partition and a
	 * read view attached - [FLINK-8371].
	 */
	@Test
	public void testCleanupReleasedSpillablePartitionWithView() throws Exception {
		testCleanupReleasedPartition(false, true);
	}

	/**
	 * Tests cleanup of {@link SpillableSubpartition#release()} with a spilled partition and no
	 * read view attached.
	 */
	@Test
	public void testCleanupReleasedSpilledPartitionNoView() throws Exception {
		testCleanupReleasedPartition(true, false);
	}

	/**
	 * Tests cleanup of {@link SpillableSubpartition#release()} with a spilled partition and a
	 * read view attached.
	 */
	@Test
	public void testCleanupReleasedSpilledPartitionWithView() throws Exception {
		testCleanupReleasedPartition(true, true);
	}

	/**
	 * Tests cleanup of {@link SpillableSubpartition#release()}.
	 *
	 * @param spilled
	 * 		whether the partition should be spilled to disk (<tt>true</tt>) or not (<tt>false</tt>,
	 * 		spillable)
	 * @param createView
	 * 		whether the partition should have a view attached to it (<tt>true</tt>) or not (<tt>false</tt>)
	 */
	private void testCleanupReleasedPartition(boolean spilled, boolean createView) throws Exception {
		SpillableSubpartition partition = createSubpartition();

		BufferConsumer buffer1 = createFilledBufferConsumer(BUFFER_DATA_SIZE, BUFFER_DATA_SIZE);
		BufferConsumer buffer2 = createFilledBufferConsumer(BUFFER_DATA_SIZE, BUFFER_DATA_SIZE);
		boolean buffer1Recycled;
		boolean buffer2Recycled;
		try {
			partition.add(buffer1);
			partition.add(buffer2);
			// create the read view before spilling
			// (tests both code paths since this view may then contain the spilled view)
			ResultSubpartitionView view = null;
			if (createView) {
				partition.finish();
				view = partition.createReadView(new NoOpBufferAvailablityListener());
			}
			if (spilled) {
				// note: in case we create a view, one buffer will already reside in the view and
				//       one EndOfPartitionEvent will be added instead (so overall the number of
				//       buffers to spill is the same
				assertEquals(2, partition.releaseMemory());
			}

			partition.release();

			assertTrue(partition.isReleased());
			if (createView) {
				assertTrue(view.isReleased());
			}
			assertTrue(buffer1.isRecycled());
		} finally {
			buffer1Recycled = buffer1.isRecycled();
			if (!buffer1Recycled) {
				buffer1.close();
			}
			buffer2Recycled = buffer2.isRecycled();
			if (!buffer2Recycled) {
				buffer2.close();
			}
		}
		if (!buffer1Recycled) {
			Assert.fail("buffer 1 not recycled");
		}
		if (!buffer2Recycled) {
			Assert.fail("buffer 2 not recycled");
		}
		// note: in case we create a view, there will be an additional EndOfPartitionEvent
		assertEquals(createView ? 3 : 2, partition.getTotalNumberOfBuffers());
		//TODO: re-enable this?
//		assertEquals((createView ? 4 : 0) + 2 * BUFFER_DATA_SIZE, partition.getTotalNumberOfBytes());
	}

	/**
	 * An {@link IOManagerAsync} that creates closed {@link BufferFileWriter} instances in its
	 * {@link #createBufferFileWriter(FileIOChannel.ID)} method.
	 *
	 * <p>These {@link BufferFileWriter} objects will thus throw an exception when trying to add
	 * write requests, e.g. by calling {@link BufferFileWriter#writeBlock(Object)}.
	 */
	private static class IOManagerAsyncWithClosedBufferFileWriter extends IOManagerAsync {
		@Override
		public BufferFileWriter createBufferFileWriter(FileIOChannel.ID channelID)
				throws IOException {
			BufferFileWriter bufferFileWriter = super.createBufferFileWriter(channelID);
			bufferFileWriter.close();
			return bufferFileWriter;
		}
	}

}
