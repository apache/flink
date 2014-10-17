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

package org.apache.flink.runtime.io.disk.iomanager;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.event.task.AbstractEvent;
import org.apache.flink.runtime.io.disk.iomanager.Channel.Enumerator;
import org.apache.flink.runtime.io.disk.iomanager.Channel.ID;
import org.apache.flink.runtime.io.network.Envelope;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferPoolOwner;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.BufferOrEvent;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.powermock.api.mockito.PowerMockito.mock;

public class IntermediateResultWriterReaderTest {

	private IOManager ioManager;

	private Enumerator enumerator;

	private NetworkBufferPool buffers;

	@Before
	public void setupIoManager() {
		ioManager = new IOManager("/Users/uce/Desktop/writer-test/");
		enumerator = ioManager.createChannelEnumerator();
		buffers = new NetworkBufferPool(1024, 16 * 1024);
	}

	@After
	public void verifyAllBuffersReturned() {
		assertEquals(buffers.getNumMemorySegments(), buffers.getNumAvailableMemorySegments());
	}

	@Test
	public void testWriteAndDelete() {
		try {
			final BufferFileWriter writer = ioManager.createBufferFileWriter(enumerator.next());

			final BufferPool bufferPool = buffers.createBufferPool(mock(BufferPoolOwner.class), 512, true);

			// ----------------------------------------------------------------
			// Write buffers from buffer pool
			// ----------------------------------------------------------------

			for (int i = 0; i < 1024; i++) {
				Buffer buffer = bufferPool.requestBuffer().waitForBuffer().getBuffer();
				writer.writeBuffer(buffer);
			}

			writer.deleteFile();

			bufferPool.destroy();
		}
		catch (Throwable t) {
			t.printStackTrace();
			Assert.fail("Unexpected exception.");
		}
	}

	@Test
	public void testWriteReadBufferEventSequence() {
		try {
			final ID ioChannel = enumerator.next();

			final int bufferSize = buffers.getMemorySegmentSize();
			final BufferPool outputBufferPool = buffers.createBufferPool(mock(BufferPoolOwner.class), 512, true);
			final BufferPool inputBufferPool = buffers.createBufferPool(mock(BufferPoolOwner.class), 128, true);

			final BufferFileWriter writer = ioManager.createBufferFileWriter(ioChannel);
			final BufferFileReader reader = ioManager.createBufferFileReader(ioChannel, inputBufferPool);

			Random random = new Random(0xB4DC0F7E);

			// ----------------------------------------------------------------
			// Write buffers from output buffer pool
			// ----------------------------------------------------------------

			int numWriteRequests = 2048;

			byte[] randomBytes = new byte[bufferSize];

			for (int i = 0; i < numWriteRequests; i++) {
				if (random.nextDouble() <= 0.9) {
					Buffer buffer = outputBufferPool.requestBuffer().waitForBuffer().getBuffer();
					int randomSize = random.nextInt(bufferSize);

					buffer.setSize(randomSize);

					random.nextBytes(randomBytes);

					buffer.getNioBuffer().put(randomBytes, 0, randomSize);

					writer.writeBuffer(buffer);
				}
				else {
					writer.writeEvent(new TestEvent(random.nextLong()));
				}
			}

			writer.close();

			// ----------------------------------------------------------------
			// Read file to input buffer pool
			// ----------------------------------------------------------------

			random.setSeed(0xB4DC0F7E); // reset seed

			byte[] verifyBytes = new byte[bufferSize];

			while (reader.hasNext()) {
				BufferOrEvent boe = reader.getNextBufferOrEvent();
				if (boe != null) {
					if (random.nextDouble() <= 0.9) {
						assertTrue(boe.isBuffer());
						Buffer buffer = boe.getBuffer();

						assertEquals(random.nextInt(bufferSize), buffer.getSize());

						// Verify buffer content
						random.nextBytes(randomBytes);

						buffer.getNioBuffer().get(verifyBytes, 0, buffer.getSize());

						boolean isEqual = true;
						for (int i = 0; i < buffer.getSize(); i++) {
							if (verifyBytes[i] != randomBytes[i]) {
								isEqual = false;
							}
						}

						assertTrue(isEqual);

						buffer.recycle();
					}
					else {
						assertTrue(boe.isEvent());

						TestEvent event = ((TestEvent) boe.getEvent());
						assertEquals(random.nextLong(), event.getId());
					}

					numWriteRequests--;
				}
			}

			assertEquals(0, numWriteRequests); // read as many as written

			reader.close();

			writer.deleteFile();

			outputBufferPool.destroy();

			inputBufferPool.destroy();
		}
		catch (Throwable t) {
			t.printStackTrace();
			Assert.fail("Unexpected exception");
		}
	}

	@Test
	public void testWriteBufferEventSequenceReadFileSegment() {
		try {
			final ID ioChannel = enumerator.next();

			final int bufferSize = buffers.getMemorySegmentSize();
			final BufferPool outputBufferPool = buffers.createBufferPool(mock(BufferPoolOwner.class), 512, true);

			final BufferFileWriter writer = ioManager.createBufferFileWriter(ioChannel);
			final BufferFileSegmentReader reader = ioManager.createBufferFileSegmentReader(ioChannel);

			Random random = new Random(0xB4DC0F7E);

			// ----------------------------------------------------------------
			// Write buffers from output buffer pool
			// ----------------------------------------------------------------
			int numWriteRequests = 2048;

			for (int i = 0; i < numWriteRequests; i++) {
				if (random.nextDouble() <= 0.9) {
					Buffer buffer = outputBufferPool.requestBuffer().waitForBuffer().getBuffer();

					int randomSize = random.nextInt(bufferSize);

					buffer.setSize(randomSize);

					writer.writeBuffer(buffer);
				}
				else {
					writer.writeEvent(new TestEvent(0L));
				}
			}

			writer.close();

			// ----------------------------------------------------------------
			// Read file segments
			// ----------------------------------------------------------------

			random.setSeed(0xB4DC0F7E); // reset seed

			int serializedEventLength = Envelope.toSerializedEvent(new TestEvent(0L)).remaining();

			while (reader.hasNext()) {
				FileSegment fileSegment = reader.getNextFileSegment();

				if (fileSegment != null) {
					if (random.nextDouble() <= 0.9) {
						assertTrue(fileSegment.isBuffer());
						assertEquals(random.nextInt(bufferSize), fileSegment.getLength());
					}
					else {
						assertTrue(fileSegment.isEvent());
						assertEquals(serializedEventLength, fileSegment.getLength());
					}

					numWriteRequests--;
				}
			}

			assertEquals(0, numWriteRequests); // read as many as written

			reader.close();

			writer.deleteFile();

			outputBufferPool.destroy();
		}
		catch (Throwable t) {
			t.printStackTrace();
			Assert.fail("Unexpected exception");
		}
	}

	// ------------------------------------------------------------------------

	public static final class TestEvent extends AbstractEvent {

		private long id;

		public TestEvent() {
		}

		public TestEvent(long id) {
			this.id = id;
		}

		public long getId() {
			return id;
		}

		@Override
		public void write(DataOutputView out) throws IOException {
			out.writeLong(id);
		}

		@Override
		public void read(DataInputView in) throws IOException {
			id = in.readLong();
		}

		@Override
		public boolean equals(Object obj) {
			return obj.getClass() == TestEvent.class && ((TestEvent) obj).id == this.id;
		}

		@Override
		public int hashCode() {
			return ((int) id) ^ ((int) (id >>> 32));
		}

		@Override
		public String toString() {
			return "TestEvent2 (" + id + ")";
		}
	}

}
