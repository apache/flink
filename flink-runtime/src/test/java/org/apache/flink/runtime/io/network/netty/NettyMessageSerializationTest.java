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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.event.task.IntegerTaskEvent;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferDecompressor;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the serialization and deserialization of the various {@link NettyMessage} sub-classes.
 */
@RunWith(Parameterized.class)
public class NettyMessageSerializationTest {

	private static final int BUFFER_SIZE = 1024;

	private static final BufferCompressor COMPRESSOR = new BufferCompressor(BUFFER_SIZE, "LZ4");

	private static final BufferDecompressor DECOMPRESSOR = new BufferDecompressor(BUFFER_SIZE, "LZ4");

	private final EmbeddedChannel channel = new EmbeddedChannel(
			new NettyMessage.NettyMessageEncoder(), // outbound messages
			new NettyMessage.NettyMessageDecoder()); // inbound messages

	private final Random random = new Random();

	// ------------------------------------------------------------------------
	//  parameters
	// ------------------------------------------------------------------------

	private final boolean testReadOnlyBuffer;

	private final boolean testCompressedBuffer;

	@Parameterized.Parameters(name = "testReadOnlyBuffer = {0}, testCompressedBuffer = {1}")
	public static Collection<Object[]> testReadOnlyBuffer() {
		return Arrays.asList(new Object[][] {
			{false, false},
			{true, false},
			{false, true},
			{true, true}
		});
	}

	public NettyMessageSerializationTest(boolean testReadOnlyBuffer, boolean testCompressedBuffer) {
		this.testReadOnlyBuffer = testReadOnlyBuffer;
		this.testCompressedBuffer = testCompressedBuffer;
	}

	@Test
	public void testEncodeDecode() {
		testEncodeDecodeBuffer(testReadOnlyBuffer, testCompressedBuffer);

		{
			{
				IllegalStateException expectedError = new IllegalStateException();
				InputChannelID receiverId = new InputChannelID();

				NettyMessage.ErrorResponse expected = new NettyMessage.ErrorResponse(expectedError, receiverId);
				NettyMessage.ErrorResponse actual = encodeAndDecode(expected);

				assertEquals(expected.cause.getClass(), actual.cause.getClass());
				assertEquals(expected.cause.getMessage(), actual.cause.getMessage());
				assertEquals(receiverId, actual.receiverId);
			}

			{
				IllegalStateException expectedError = new IllegalStateException("Illegal illegal illegal");
				InputChannelID receiverId = new InputChannelID();

				NettyMessage.ErrorResponse expected = new NettyMessage.ErrorResponse(expectedError, receiverId);
				NettyMessage.ErrorResponse actual = encodeAndDecode(expected);

				assertEquals(expected.cause.getClass(), actual.cause.getClass());
				assertEquals(expected.cause.getMessage(), actual.cause.getMessage());
				assertEquals(receiverId, actual.receiverId);
			}

			{
				IllegalStateException expectedError = new IllegalStateException("Illegal illegal illegal");

				NettyMessage.ErrorResponse expected = new NettyMessage.ErrorResponse(expectedError);
				NettyMessage.ErrorResponse actual = encodeAndDecode(expected);

				assertEquals(expected.cause.getClass(), actual.cause.getClass());
				assertEquals(expected.cause.getMessage(), actual.cause.getMessage());
				assertNull(actual.receiverId);
				assertTrue(actual.isFatalError());
			}
		}

		{
			NettyMessage.PartitionRequest expected = new NettyMessage.PartitionRequest(new ResultPartitionID(), random.nextInt(), new InputChannelID(), random.nextInt());
			NettyMessage.PartitionRequest actual = encodeAndDecode(expected);

			assertEquals(expected.partitionId, actual.partitionId);
			assertEquals(expected.queueIndex, actual.queueIndex);
			assertEquals(expected.receiverId, actual.receiverId);
			assertEquals(expected.credit, actual.credit);
		}

		{
			NettyMessage.TaskEventRequest expected = new NettyMessage.TaskEventRequest(new IntegerTaskEvent(random.nextInt()), new ResultPartitionID(), new InputChannelID());
			NettyMessage.TaskEventRequest actual = encodeAndDecode(expected);

			assertEquals(expected.event, actual.event);
			assertEquals(expected.partitionId, actual.partitionId);
			assertEquals(expected.receiverId, actual.receiverId);
		}

		{
			NettyMessage.CancelPartitionRequest expected = new NettyMessage.CancelPartitionRequest(new InputChannelID());
			NettyMessage.CancelPartitionRequest actual = encodeAndDecode(expected);

			assertEquals(expected.receiverId, actual.receiverId);
		}

		{
			NettyMessage.CloseRequest expected = new NettyMessage.CloseRequest();
			NettyMessage.CloseRequest actual = encodeAndDecode(expected);

			assertEquals(expected.getClass(), actual.getClass());
		}

		{
			NettyMessage.AddCredit expected = new NettyMessage.AddCredit(new ResultPartitionID(), random.nextInt(Integer.MAX_VALUE) + 1, new InputChannelID());
			NettyMessage.AddCredit actual = encodeAndDecode(expected);

			assertEquals(expected.partitionId, actual.partitionId);
			assertEquals(expected.credit, actual.credit);
			assertEquals(expected.receiverId, actual.receiverId);
		}
	}

	private void testEncodeDecodeBuffer(boolean testReadOnlyBuffer, boolean testCompressedBuffer) {
		NetworkBuffer buffer = new NetworkBuffer(MemorySegmentFactory.allocateUnpooledSegment(BUFFER_SIZE), FreeingBufferRecycler.INSTANCE);

		for (int i = 0; i < BUFFER_SIZE; i += 8) {
			buffer.writeLong(i);
		}

		Buffer testBuffer = testReadOnlyBuffer ? buffer.readOnlySlice() : buffer;
		if (testCompressedBuffer) {
			testBuffer = COMPRESSOR.compressToOriginalBuffer(buffer);
		}

		NettyMessage.BufferResponse expected = new NettyMessage.BufferResponse(
			testBuffer, random.nextInt(), new InputChannelID(), random.nextInt());
		NettyMessage.BufferResponse actual = encodeAndDecode(expected);

		// Netty 4.1 is not copying the messages, but retaining slices of them. BufferResponse actual is in this case
		// holding a reference to the buffer. Buffer will be recycled only once "actual" will be released.
		assertFalse(buffer.isRecycled());
		assertFalse(testBuffer.isRecycled());

		ByteBuf retainedSlice = actual.getNettyBuffer();
		if (testCompressedBuffer) {
			assertTrue(actual.isCompressed);
			retainedSlice = decompress(retainedSlice);
		}

		// Ensure not recycled and same size as original buffer
		assertEquals(1, retainedSlice.refCnt());
		assertEquals(BUFFER_SIZE, retainedSlice.readableBytes());

		for (int i = 0; i < BUFFER_SIZE; i += 8) {
			assertEquals(i, retainedSlice.readLong());
		}

		// Release the retained slice
		actual.releaseBuffer();
		if (testCompressedBuffer) {
			retainedSlice.release();
		}
		assertEquals(0, retainedSlice.refCnt());
		assertTrue(buffer.isRecycled());
		assertTrue(testBuffer.isRecycled());

		assertEquals(expected.sequenceNumber, actual.sequenceNumber);
		assertEquals(expected.receiverId, actual.receiverId);
		assertEquals(expected.backlog, actual.backlog);
	}

	private ByteBuf decompress(ByteBuf buffer) {
		MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(BUFFER_SIZE);
		Buffer compressedBuffer = new NetworkBuffer(segment, FreeingBufferRecycler.INSTANCE);
		buffer.readBytes(compressedBuffer.asByteBuf(), buffer.readableBytes());
		compressedBuffer.setCompressed(true);
		return DECOMPRESSOR.decompressToOriginalBuffer(compressedBuffer).asByteBuf();
	}

	@SuppressWarnings("unchecked")
	private <T extends NettyMessage> T encodeAndDecode(T msg) {
		channel.writeOutbound(msg);
		ByteBuf encoded = (ByteBuf) channel.readOutbound();

		assertTrue(channel.writeInbound(encoded));

		return (T) channel.readInbound();
	}
}
