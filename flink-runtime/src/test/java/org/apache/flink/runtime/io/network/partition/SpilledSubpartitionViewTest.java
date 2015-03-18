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

import com.google.common.collect.Lists;
import org.apache.flink.runtime.io.disk.iomanager.BufferFileWriter;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManager.IOMode;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.util.TestConsumerCallback.RecyclingCallback;
import org.apache.flink.runtime.io.network.util.TestInfiniteBufferProvider;
import org.apache.flink.runtime.io.network.util.TestPooledBufferProvider;
import org.apache.flink.runtime.io.network.util.TestSubpartitionConsumer;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.mockito.Mockito.mock;

/**
 * Test for both the asynchronous and synchronous spilled subpartition view implementation.
 */
@RunWith(Parameterized.class)
public class SpilledSubpartitionViewTest {

	private static final IOManager ioManager = new IOManagerAsync();

	private static final ExecutorService executor = Executors.newCachedThreadPool();

	private static final TestInfiniteBufferProvider writerBufferPool =
			new TestInfiniteBufferProvider();

	private IOMode ioMode;

	public SpilledSubpartitionViewTest(IOMode ioMode) {
		this.ioMode = ioMode;
	}

	@AfterClass
	public static void shutdown() {
		ioManager.shutdown();
		executor.shutdown();
	}

	@Parameterized.Parameters
	public static Collection<Object[]> ioMode() {
		return Arrays.asList(new Object[][]{
				{IOMode.SYNC},
				{IOMode.ASYNC}});
	}

	@Test
	public void testReadMultipleFilesWithSingleBufferPool() throws Exception {
		// Setup
		BufferFileWriter[] writers = new BufferFileWriter[]{
				createWriterAndWriteBuffers(ioManager, writerBufferPool, 512),
				createWriterAndWriteBuffers(ioManager, writerBufferPool, 512)
		};

		final ResultSubpartitionView[] readers = new ResultSubpartitionView[writers.length];

		// Make this buffer pool small so that we can test the behaviour of the asynchronous view
		// with few  buffers.
		final BufferProvider inputBuffers = new TestPooledBufferProvider(2);

		final ResultSubpartition parent = mock(ResultSubpartition.class);

		try {
			// Wait for writers to finish
			for (BufferFileWriter writer : writers) {
				writer.close();
			}

			// Create the views depending on the test configuration
			for (int i = 0; i < readers.length; i++) {
				if (ioMode.isSynchronous()) {
					readers[i] = new SpilledSubpartitionViewSyncIO(
							parent,
							inputBuffers.getMemorySegmentSize(),
							writers[i].getChannelID(),
							0);
				}
				else {
					// For the asynchronous view, it is important that a registered listener will
					// eventually be notified even if the view never got a buffer to read data into.
					//
					// At runtime, multiple threads never share the same buffer pool as in test. We
					// do it here to provoke the erroneous behaviour.
					readers[i] = new SpilledSubpartitionViewAsyncIO(
							parent, inputBuffers, ioManager, writers[i].getChannelID(), 0);
				}
			}

			final List<Future<Boolean>> results = Lists.newArrayList();

			// Submit the consuming tasks
			for (ResultSubpartitionView view : readers) {
				results.add(executor.submit(new TestSubpartitionConsumer(
						view, false, new RecyclingCallback())));
			}

			// Wait for the results
			for (Future<Boolean> res : results) {
				try {
					res.get(2, TimeUnit.MINUTES);
				}
				catch (TimeoutException e) {
					throw new TimeoutException("There has been a timeout in the test. This " +
							"indicates that there is a bug/deadlock in the tested subpartition " +
							"view. The timed out test was in " + ioMode + " mode.");
				}
			}
		}
		finally {
			for (BufferFileWriter writer : writers) {
				if (writer != null) {
					writer.deleteChannel();
				}
			}

			for (ResultSubpartitionView reader : readers) {
				if (reader != null) {
					reader.releaseAllResources();
				}
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
