/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.nephele.taskmanager.transferenvelope;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayDeque;
import java.util.Queue;

import org.junit.Test;

import eu.stratosphere.core.memory.MemorySegment;
import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.BufferFactory;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.MemoryBuffer;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.taskmanager.bufferprovider.BufferAvailabilityListener;
import eu.stratosphere.nephele.taskmanager.bufferprovider.BufferProvider;
import eu.stratosphere.nephele.taskmanager.bufferprovider.BufferProviderBroker;
import eu.stratosphere.nephele.util.BufferPoolConnector;
import eu.stratosphere.nephele.util.InterruptibleByteChannel;
import eu.stratosphere.util.StringUtils;

/**
 * This class contains tests covering the deserialization of a byte stream to a transfer envelope.
 * 
 */
public class DefaultDeserializerTest {

	/**
	 * The size of the test byte buffers in byte.
	 */
	private static final int TEST_BUFFER_CAPACITY = 1024;

	/**
	 * The sequence number to be used during the tests.
	 */
	private static final int SEQUENCE_NUMBER = 0;

	/**
	 * The job ID to be used during the tests.
	 */
	private static final JobID JOB_ID = new JobID();

	/**
	 * The channel ID to be used during the tests.
	 */
	private static final ChannelID CHANNEL_ID = new ChannelID();

	/**
	 * A dummy implementation of a {@link BufferProvider} which is used in this test.
	 * <p>
	 * This class is not thread-safe.
	 * 
	 */
	private static final class TestBufferProvider implements BufferProvider {

		/**
		 * Stores the available byte buffers.
		 */
		private final Queue<MemorySegment> bufferPool;

		/**
		 * Constructs a new test buffer provider.
		 * 
		 * @param numberOfBuffers
		 *        the number of byte buffers this pool has available.
		 */
		private TestBufferProvider(final int numberOfBuffers) {

			this.bufferPool = new ArrayDeque<MemorySegment>(numberOfBuffers);
			for (int i = 0; i < numberOfBuffers; ++i) {
				this.bufferPool.add(new MemorySegment(new byte[TEST_BUFFER_CAPACITY]));
			}
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public Buffer requestEmptyBuffer(final int minimumSizeOfBuffer) throws IOException {

			if (this.bufferPool.isEmpty()) {
				return null;
			}

			return BufferFactory.createFromMemory(minimumSizeOfBuffer, this.bufferPool.poll(),
				new BufferPoolConnector(this.bufferPool));
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public Buffer requestEmptyBufferBlocking(final int minimumSizeOfBuffer) throws IOException,
				InterruptedException {

			throw new IllegalStateException("requestEmptyBufferBlocking called");
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public int getMaximumBufferSize() {

			throw new IllegalStateException("getMaximumBufferSize called");
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public boolean isShared() {

			throw new IllegalStateException("isShared called");
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void reportAsynchronousEvent() {

			throw new IllegalStateException("reportAsynchronousEvent called");
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public boolean registerBufferAvailabilityListener(final BufferAvailabilityListener bufferAvailabilityListener) {

			throw new IllegalStateException("registerBufferAvailabilityListener called");
		}
	}

	/**
	 * A dummy implementation of a {@link BufferProviderBroker} which is used during this test.
	 * <p>
	 * This class is not thread-safe.
	 * 
	 */
	private static final class TestBufferProviderBroker implements BufferProviderBroker {

		private final BufferProvider bufferProvider;

		private TestBufferProviderBroker(final BufferProvider bufferProvider) {
			this.bufferProvider = bufferProvider;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public BufferProvider getBufferProvider(final JobID jobID, final ChannelID sourceChannelID) throws IOException,
				InterruptedException {

			return this.bufferProvider;
		}
	}

	/**
	 * Constructs an {@link InterruptibleByteChannel} from which the deserializer to be tested can read its data.
	 * 
	 * @param readInterruptPositions
	 *        the positions after which the byte stream shall be interrupted
	 * @param testBufferSize
	 *        the size of the test buffer to create
	 * @return an {@link InterruptibleByteChannel} holding the serialized data in memory
	 * @throws IOException
	 *         thrown if an error occurs while serializing the original data
	 */
	private ReadableByteChannel createByteChannel(final int[] readInterruptPositions, final int testBufferSize)
			throws IOException {

		final TransferEnvelope te = new TransferEnvelope(SEQUENCE_NUMBER, JOB_ID, CHANNEL_ID);

		if (testBufferSize >= 0) {

			if (testBufferSize > 100) {
				throw new IllegalStateException("Test buffer size can be 100 bytes at most");
			}

			final Queue<MemorySegment> bufferPool = new ArrayDeque<MemorySegment>();
			final MemorySegment ms = new MemorySegment(new byte[TEST_BUFFER_CAPACITY]);

			final MemoryBuffer buffer = BufferFactory.createFromMemory(ms.size(), ms, new BufferPoolConnector(bufferPool));

			final ByteBuffer srcBuffer = ByteBuffer.allocate(testBufferSize);
			for (int i = 0; i < testBufferSize; ++i) {
				srcBuffer.put((byte) i);
			}
			srcBuffer.flip();

			buffer.write(srcBuffer);
			buffer.flip();
			te.setBuffer(buffer);
		}

		final DefaultSerializer ds = new DefaultSerializer();
		ds.setTransferEnvelope(te);

		final InterruptibleByteChannel ibc = new InterruptibleByteChannel(null, readInterruptPositions);

		while (ds.write(ibc));

		ibc.switchToReadPhase();

		return ibc;
	}

	/**
	 * Executes the deserialization method.
	 * 
	 * @param rbc
	 *        the byte channel to read the serialized data from
	 * @param bpb
	 *        the buffer provider broker to request empty buffers from
	 * @return the deserialized transfer envelope
	 * @throws IOException
	 *         thrown if an error occurs during the deserialization process
	 * @throws NoBufferAvailableException
	 *         thrown if the buffer provider broker could not provide an empty buffer
	 */
	private TransferEnvelope executeDeserialization(final ReadableByteChannel rbc, final BufferProviderBroker bpb)
			throws IOException, NoBufferAvailableException {

		final DefaultDeserializer dd = new DefaultDeserializer(bpb);

		TransferEnvelope te = dd.getFullyDeserializedTransferEnvelope();
		while (te == null) {

			dd.read(rbc);
			te = dd.getFullyDeserializedTransferEnvelope();
		}

		assertEquals(SEQUENCE_NUMBER, te.getSequenceNumber());
		assertEquals(JOB_ID, te.getJobID());
		assertEquals(CHANNEL_ID, te.getSource());

		return te;
	}

	/**
	 * Tests the deserialization process of a {@link TransferEnvelope} with a buffer when no interruption of the byte
	 * stream.
	 */
	@Test
	public void testDeserializationWithBufferAndWithoutInterruption() {

		try {

			final ReadableByteChannel rbc = createByteChannel(null, 10);

			final TestBufferProviderBroker tbpb = new TestBufferProviderBroker(new TestBufferProvider(1));

			final TransferEnvelope te = executeDeserialization(rbc, tbpb);

			assertNotNull(te.getBuffer());
			assertEquals(10, te.getBuffer().size());

		} catch (IOException ioe) {
			fail(StringUtils.stringifyException(ioe));
		} catch (NoBufferAvailableException nbae) {
			fail(StringUtils.stringifyException(nbae));
		}
	}

	/**
	 * Tests the deserialization process of a {@link TransferEnvelope} with a buffer and interruptions of the byte
	 * stream.
	 */
	@Test
	public void testDeserializationWithBufferAndInterruptions() {

		try {

			final ReadableByteChannel rbc = createByteChannel(new int[] { 3, 7, 24, 52 }, 10);

			final TestBufferProviderBroker tbpb = new TestBufferProviderBroker(new TestBufferProvider(1));

			final TransferEnvelope te = executeDeserialization(rbc, tbpb);

			assertNotNull(te.getBuffer());
			assertEquals(10, te.getBuffer().size());

		} catch (IOException ioe) {
			fail(StringUtils.stringifyException(ioe));
		} catch (NoBufferAvailableException nbae) {
			fail(StringUtils.stringifyException(nbae));
		}
	}

	/**
	 * Tests the deserialization process of a {@link TransferEnvelope} without a buffer and without interruptions of the
	 * byte stream.
	 */
	@Test
	public void testDeserializationWithoutBufferAndInterruptions() {

		try {

			final ReadableByteChannel rbc = createByteChannel(null, -1);

			final TestBufferProviderBroker tbpb = new TestBufferProviderBroker(new TestBufferProvider(1));

			final TransferEnvelope te = executeDeserialization(rbc, tbpb);

			assertNull(te.getBuffer());

		} catch (IOException ioe) {
			fail(StringUtils.stringifyException(ioe));
		} catch (NoBufferAvailableException nbae) {
			fail(StringUtils.stringifyException(nbae));
		}
	}

	/**
	 * Tests the deserialization process in case the buffer provide cannot deliver an empty buffer to read the byte
	 * stream into.
	 */
	@Test
	public void testDeserializationWithNoBufferAvailable() {

		try {
			final ReadableByteChannel rbc = createByteChannel(null, 10);
			final TestBufferProviderBroker tbpb = new TestBufferProviderBroker(new TestBufferProvider(0));
			executeDeserialization(rbc, tbpb);

		} catch (IOException ioe) {
			fail(StringUtils.stringifyException(ioe));
		} catch (NoBufferAvailableException nbae) {
			// Expected exception was successfully caught
			return;
		}

		fail("Expected NoBufferAvailableException but has not been thrown");
	}
}
