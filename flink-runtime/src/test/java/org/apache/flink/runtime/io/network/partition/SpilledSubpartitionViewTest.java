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

import org.apache.flink.runtime.io.disk.iomanager.BufferFileWriter;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.util.TestConsumerCallback;
import org.apache.flink.runtime.io.network.util.TestInfiniteBufferProvider;
import org.apache.flink.runtime.io.network.util.TestPooledBufferProvider;
import org.apache.flink.runtime.io.network.util.TestSubpartitionConsumer;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.junit.AfterClass;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.mockito.Mockito.mock;

/**
 * Tests for {@link SpillableSubpartitionView}, in addition to indirect tests via {@link
 * SpillableSubpartitionTest}.
 */
public class SpilledSubpartitionViewTest {

	private static final IOManager IO_MANAGER = new IOManagerAsync();

	private static final TestInfiniteBufferProvider writerBufferPool =
		new TestInfiniteBufferProvider();

	@AfterClass
	public static void shutdown() {
		IO_MANAGER.shutdown();
	}

	@Test
	public void testWriteConsume() throws Exception {
		// Config
		final int numberOfBuffersToWrite = 512;

		// Setup
		final BufferFileWriter writer = createWriterAndWriteBuffers(IO_MANAGER, writerBufferPool, numberOfBuffersToWrite);

		writer.close();

		TestPooledBufferProvider viewBufferPool = new TestPooledBufferProvider(1);

		TestSubpartitionConsumer consumer = new TestSubpartitionConsumer(
			false, new TestConsumerCallback.RecyclingCallback());

		SpilledSubpartitionView view = new SpilledSubpartitionView(
			mock(ResultSubpartition.class),
			viewBufferPool.getMemorySegmentSize(),
			writer,
			numberOfBuffersToWrite + 1, // +1 for end-of-partition
			consumer);

		consumer.setSubpartitionView(view);

		// Consume subpartition
		consumer.call();
	}

	@Test
	public void testConsumeWithFewBuffers() throws Exception {
		// Config
		final int numberOfBuffersToWrite = 512;

		// Setup
		final BufferFileWriter writer = createWriterAndWriteBuffers(IO_MANAGER, writerBufferPool, numberOfBuffersToWrite);

		writer.close();

		TestSubpartitionConsumer consumer = new TestSubpartitionConsumer(
			false, new TestConsumerCallback.RecyclingCallback());

		SpilledSubpartitionView view = new SpilledSubpartitionView(
			mock(ResultSubpartition.class),
			32 * 1024,
			writer,
			numberOfBuffersToWrite + 1,
			consumer);

		consumer.setSubpartitionView(view);

		// No buffer available, don't deadlock. We need to make progress in situations when the view
		// is consumed at an input gate with local and remote channels. The remote channels might
		// eat up all the buffers, at which point the spilled view will not have any buffers
		// available and the input gate can't make any progress if we don't return immediately.
		//
		// The current solution is straight-forward with a separate buffer per spilled subpartition,
		// but introduces memory-overhead.
		//
		// TODO Replace with asynchronous buffer pool request as this introduces extra buffers per
		// consumed subpartition.
		consumer.call();
	}

	@Test
	public void testReadMultipleFilesWithSingleBufferPool() throws Exception {
		ExecutorService executor = null;
		BufferFileWriter[] writers = null;
		ResultSubpartitionView[] readers = null;

		try {
			executor = Executors.newCachedThreadPool();

			// Setup
			writers = new BufferFileWriter[]{
				createWriterAndWriteBuffers(IO_MANAGER, writerBufferPool, 512),
				createWriterAndWriteBuffers(IO_MANAGER, writerBufferPool, 512)
			};

			readers = new ResultSubpartitionView[writers.length];
			TestSubpartitionConsumer[] consumers = new TestSubpartitionConsumer[writers.length];

			BufferProvider inputBuffers = new TestPooledBufferProvider(2);

			ResultSubpartition parent = mock(ResultSubpartition.class);

			// Wait for writers to finish
			for (BufferFileWriter writer : writers) {
				writer.close();
			}

			// Create the views depending on the test configuration
			for (int i = 0; i < readers.length; i++) {
				consumers[i] = new TestSubpartitionConsumer(
					false, new TestConsumerCallback.RecyclingCallback());

				readers[i] = new SpilledSubpartitionView(
					parent,
					inputBuffers.getMemorySegmentSize(),
					writers[i],
					512 + 1, // +1 for end of partition event
					consumers[i]);

				consumers[i].setSubpartitionView(readers[i]);
			}

			final List<Future<Boolean>> results = Lists.newArrayList();

			// Submit the consuming tasks
			for (TestSubpartitionConsumer consumer : consumers) {
				results.add(executor.submit(consumer));
			}

			// Wait for the results
			for (Future<Boolean> res : results) {
				try {
					res.get(2, TimeUnit.MINUTES);
				} catch (TimeoutException e) {
					throw new TimeoutException("There has been a timeout in the test. This " +
						"indicates that there is a bug/deadlock in the tested subpartition " +
						"view.");
				}
			}
		} finally {
			if (writers != null) {
				for (BufferFileWriter writer : writers) {
					if (writer != null) {
						writer.deleteChannel();
					}
				}
			}

			if (readers != null) {
				for (ResultSubpartitionView reader : readers) {
					if (reader != null) {
						reader.releaseAllResources();
					}
				}
			}

			if (executor != null) {
				executor.shutdown();
			}
		}
	}

	/**
	 * Returns a buffer file writer, to which the specified number of buffer write requests have
	 * been issued (including an end of partition event).
	 *
	 * <p> Call {@link BufferFileWriter#close()} to ensure that all buffers have been written.
	 */
	static BufferFileWriter createWriterAndWriteBuffers(
		IOManager ioManager,
		BufferProvider bufferProvider,
		int numberOfBuffers) throws IOException {

		final BufferFileWriter writer = ioManager.createBufferFileWriter(ioManager.createChannel());

		for (int i = 0; i < numberOfBuffers; i++) {
			writer.writeBlock(bufferProvider.requestBuffer());
		}

		writer.writeBlock(EventSerializer.toBuffer(EndOfPartitionEvent.INSTANCE));

		return writer;
	}

}
