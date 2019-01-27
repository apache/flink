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
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;

import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.util.TestTaskEvent;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.PooledByteBufAllocator;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.flink.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;

import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNull;
import static junit.framework.TestCase.assertTrue;
import static org.apache.flink.util.Preconditions.checkState;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.spy;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * Test the message decoder.
 */
public class ZeroCopyNettyMessageDecoderTest {
	private static final PooledByteBufAllocator ALLOCATOR = new PooledByteBufAllocator();

	private static final InputChannelID NORMAL_INPUT_CHANNEL_ID = new InputChannelID();
	private static final InputChannelID RELEASED_INPUT_CHANNEL_ID = new InputChannelID();

	/**
	 * Verifies that the message decoder works well for the upstream netty handler pipeline.
	 */
	@Test
	public void testUpstreamMessageDecoder() throws Exception {
		EmbeddedChannel channel = new EmbeddedChannel(new ZeroCopyNettyMessageDecoder(null));
		NettyMessage[] messages = new NettyMessage[]{
			new NettyMessage.PartitionRequest(new ResultPartitionID(), 1, NORMAL_INPUT_CHANNEL_ID, 2),
			new NettyMessage.TaskEventRequest(new TestTaskEvent(), new ResultPartitionID(), NORMAL_INPUT_CHANNEL_ID),
			new NettyMessage.CloseRequest(),
			new NettyMessage.CancelPartitionRequest(NORMAL_INPUT_CHANNEL_ID),
			new NettyMessage.AddCredit(new ResultPartitionID(), 2, NORMAL_INPUT_CHANNEL_ID),
		};

		// segment points: 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15
		//                 |     |     |     |       |        |
		ByteBuf[] splitBuffers = segmentMessages(messages, 3, new int[] {
			1, 7, 11, 14
		});
		readInputAndVerify(channel, splitBuffers, messages);

		splitBuffers = segmentMessages(messages, 3, new int[] {
			1, 4, 7, 9, 12, 14
		});
		readInputAndVerify(channel, splitBuffers, messages);
	}

	/**
	 * Verifies that the message decoder works well for the downstream netty handler pipeline.
	 */
	@Test
	public void testDownstreamMessageDecode() throws Exception {
		// 8 buffers required for running 2 rounds and 4 buffers each round.
		EmbeddedChannel channel = new EmbeddedChannel(
			new ZeroCopyNettyMessageDecoder(new CreditedBasedReceiverSideNetworkBufferAllocator(createPartitionRequestClientHandler(8))));
		Buffer event = createDataBuffer(32);
		event.tagAsEvent();

		NettyMessage[] messages = new NettyMessage[]{
			new NettyMessage.BufferResponse(createDataBuffer(128), 0, NORMAL_INPUT_CHANNEL_ID, 4),
			new NettyMessage.BufferResponse(createDataBuffer(256), 1, NORMAL_INPUT_CHANNEL_ID, 3),
			new NettyMessage.BufferResponse(event, 2, NORMAL_INPUT_CHANNEL_ID, 4),
			new NettyMessage.ErrorResponse(new RuntimeException("test"), NORMAL_INPUT_CHANNEL_ID),
			new NettyMessage.BufferResponse(createDataBuffer(56), 3, NORMAL_INPUT_CHANNEL_ID, 4),
		};

		// segment points: 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15
		//                 |     |     |     |       |        |
		ByteBuf[] splitBuffers = segmentMessages(messages, 3, new int[] {
			1, 7, 11, 14
		});
		readInputAndVerify(channel, splitBuffers, messages);

		splitBuffers = segmentMessages(messages, 3, new int[] {
			1, 4, 7, 9, 12, 14
		});
		readInputAndVerify(channel, splitBuffers, messages);
	}

	/**
	 * Verifies that NettyMessageDecoder works well with buffers sent to a released channel.
	 * For such a channel, no Buffer is available to receive the data buffer in the message,
	 * and the data buffer part should be discarded before reading the next message.
	 */
	@Test
	public void testDownstreamMessageDecodeWithReleasedInputChannel() throws Exception {
		// 6 buffers required for running 2 rounds and 3 buffers each round.
		EmbeddedChannel channel = new EmbeddedChannel(
			new ZeroCopyNettyMessageDecoder(new CreditedBasedReceiverSideNetworkBufferAllocator(createPartitionRequestClientHandler(6))));

		Buffer event = createDataBuffer(32);
		event.tagAsEvent();

		NettyMessage[] messages = new NettyMessage[]{
			new NettyMessage.BufferResponse(createDataBuffer(128), 0, NORMAL_INPUT_CHANNEL_ID, 4),
			new NettyMessage.BufferResponse(createDataBuffer(256), 1, RELEASED_INPUT_CHANNEL_ID, 3),
			new NettyMessage.BufferResponse(event, 2, NORMAL_INPUT_CHANNEL_ID, 4),
			new NettyMessage.ErrorResponse(new RuntimeException("test"), RELEASED_INPUT_CHANNEL_ID),
			new NettyMessage.BufferResponse(createDataBuffer(64), 3, NORMAL_INPUT_CHANNEL_ID, 4),
		};

		// segment points: 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15
		//                 |     |     |     |       |        |
		ByteBuf[] splitBuffers = segmentMessages(messages, 3, new int[]{
			1, 4, 7, 9, 12, 14
		});
		readInputAndVerify(channel, splitBuffers, messages);

		splitBuffers = segmentMessages(messages, 3, new int[]{
			1, 3, 4, 5, 7, 10, 13
		});
		readInputAndVerify(channel, splitBuffers, messages);
	}

	//------------------------------------------------------------------------------------------------------------------

	private void readInputAndVerify(EmbeddedChannel channel, ByteBuf[] inputBuffers, NettyMessage[] expected) throws Exception {
		for (ByteBuf buffer : inputBuffers) {
			channel.writeInbound(buffer);
		}

		channel.runPendingTasks();

		List<NettyMessage> decodedMessages = new ArrayList<>();
		Object input;
		while ((input = channel.readInbound()) != null) {
			assertTrue(input instanceof NettyMessage);
			decodedMessages.add((NettyMessage) input);
		}

		assertEquals(expected.length, decodedMessages.size());
		for (int i = 0; i < expected.length; ++i) {
			assertEquals(expected[i].getClass(), decodedMessages.get(i).getClass());

			if (expected[i] instanceof NettyMessage.AddCredit ||
				expected[i] instanceof NettyMessage.PartitionRequest ||
				expected[i] instanceof NettyMessage.TaskEventRequest ||
				expected[i] instanceof NettyMessage.CancelPartitionRequest ||
				expected[i] instanceof NettyMessage.CloseRequest) {

				assertTrue("Received different message, expected = " + expected[i] + ", actual = " + decodedMessages.get(i),
					isObjectEquals(expected[i], decodedMessages.get(i)));
			} else if (expected[i] instanceof NettyMessage.BufferResponse) {
				assertEquals(((NettyMessage.BufferResponse) expected[i]).backlog, ((NettyMessage.BufferResponse) decodedMessages.get(i)).backlog);
				assertEquals(((NettyMessage.BufferResponse) expected[i]).sequenceNumber, ((NettyMessage.BufferResponse) decodedMessages.get(i)).sequenceNumber);
				assertEquals(((NettyMessage.BufferResponse) expected[i]).dataBufferSize, ((NettyMessage.BufferResponse) decodedMessages.get(i)).dataBufferSize);
				assertEquals(((NettyMessage.BufferResponse) expected[i]).receiverId, ((NettyMessage.BufferResponse) decodedMessages.get(i)).receiverId);

				if (((NettyMessage.BufferResponse) expected[i]).receiverId.equals(RELEASED_INPUT_CHANNEL_ID)) {
					assertNull(((NettyMessage.BufferResponse) decodedMessages.get(i)).buffer);
				} else {
					assertEquals(((NettyMessage.BufferResponse) expected[i]).buffer, ((NettyMessage.BufferResponse) decodedMessages.get(i)).buffer);
				}
			} else if (expected[i] instanceof NettyMessage.ErrorResponse) {
				assertEquals(((NettyMessage.ErrorResponse) expected[i]).receiverId, ((NettyMessage.ErrorResponse) decodedMessages.get(i)).receiverId);
				assertEquals(((NettyMessage.ErrorResponse) expected[i]).cause.getClass(), ((NettyMessage.ErrorResponse) decodedMessages.get(i)).cause.getClass());
				assertEquals(((NettyMessage.ErrorResponse) expected[i]).cause.getMessage(), ((NettyMessage.ErrorResponse) decodedMessages.get(i)).cause.getMessage());
			} else {
				fail("Unsupported message type");
			}
		}
	}

	/**
	 * Segments the serialized buffer of the messages. This method first segments each message into
	 * numberOfSegmentsEachMessage parts, and number all the boundary and inner segment points from
	 * start. Then the segments points whose index remain in the segmentPointIndex are used to finally
	 * segment the serialized buffer.
	 *
	 * <p>For example, suppose there are 3 messages and numberOfSegmentsEachMessage is 3,
	 * then all the available segment points is illustrated as follows:
	 *
	 * <pre>
	 * +---------------+---------------+-------------------+
	 * |    |     |    |    |    |     |     |       |     |
	 * +---------------+---------------+-------------------+
	 * 0    1     2    3    4    5     6     7       8     9
	 * </pre>
	 *
	 * @param messages                    The messages to be serialized and segmented.
	 * @param numberOfSegmentsEachMessage How much parts each message is segmented into.
	 * @param segmentPointIndex           The chosen segment points.
	 * @return The segmented ByteBuf.
	 */
	private ByteBuf[] segmentMessages(NettyMessage[] messages, int numberOfSegmentsEachMessage, int[] segmentPointIndex) throws Exception {
		List<Integer> segmentPoints = new ArrayList<>();
		ByteBuf allData = Unpooled.buffer();

		int startBytesOfCurrentMessage = 0;
		for (NettyMessage message : messages) {
			ByteBuf buf = message.write(ALLOCATOR);

			// Records the position of each segments point.
			int length = buf.readableBytes();

			for (int i = 0; i < numberOfSegmentsEachMessage; ++i) {
				segmentPoints.add(startBytesOfCurrentMessage + length * i / numberOfSegmentsEachMessage);
			}

			allData.writeBytes(buf);
			startBytesOfCurrentMessage += length;
		}

		// Adds the last segment point.
		segmentPoints.add(allData.readableBytes());

		// Segments the serialized buffer according to the segment points.
		List<ByteBuf> segmentedBuffers = new ArrayList<>();

		for (int i = 0; i <= segmentPointIndex.length; ++i) {
			ByteBuf buf = Unpooled.buffer();

			int startPos = (i == 0 ? 0 : segmentPoints.get(segmentPointIndex[i - 1]));
			int endPos = (i == segmentPointIndex.length ? segmentPoints.get(segmentPoints.size() - 1) : segmentPoints.get(segmentPointIndex[i]));

			checkState(startPos == allData.readerIndex());

			buf.writeBytes(allData, endPos - startPos);
			segmentedBuffers.add(buf);
		}

		checkState(!allData.isReadable());
		return segmentedBuffers.toArray(new ByteBuf[0]);
	}

	private Buffer createDataBuffer(int size) {
		MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(size);
		Buffer buffer = new NetworkBuffer(segment, FreeingBufferRecycler.INSTANCE);
		for (int i = 0; i < size / 4; ++i) {
			((NetworkBuffer) buffer).writeInt(i);
		}

		return buffer;
	}

	private CreditBasedPartitionRequestClientHandler createPartitionRequestClientHandler(int numberOfBuffersInNormalChannel) throws IOException {
		CreditBasedPartitionRequestClientHandler handler = new CreditBasedPartitionRequestClientHandler();

		RemoteInputChannel normalInputChannel = spy(new RemoteInputChannel(
			mock(SingleInputGate.class),
			0,
			new ResultPartitionID(),
			mock(ConnectionID.class),
			mock(ConnectionManager.class),
			mock(TaskIOMetricGroup.class)
		));
		List<MemorySegment> memorySegments = new ArrayList<>();
		for (int i = 0; i < numberOfBuffersInNormalChannel; ++i) {
			memorySegments.add(MemorySegmentFactory.allocateUnpooledSegment(1024));
		}
		normalInputChannel.assignExclusiveSegments(memorySegments);
		when(normalInputChannel.getInputChannelId()).thenReturn(NORMAL_INPUT_CHANNEL_ID);
		handler.addInputChannel(normalInputChannel);

		RemoteInputChannel releasedInputChannel = spy(new RemoteInputChannel(
			mock(SingleInputGate.class),
			0,
			new ResultPartitionID(),
			mock(ConnectionID.class),
			mock(ConnectionManager.class),
			mock(TaskIOMetricGroup.class)
		));
		when(releasedInputChannel.getInputChannelId()).thenReturn(RELEASED_INPUT_CHANNEL_ID);
		when(releasedInputChannel.isReleased()).thenReturn(true);
		handler.addInputChannel(releasedInputChannel);

		return handler;
	}

	private boolean isObjectEquals(Object first, Object second) throws Exception {
		if (!first.getClass().equals(second.getClass())) {
			return false;
		}

		Field[] fields = first.getClass().getDeclaredFields();

		for (Field field : fields) {
			if (!Modifier.isStatic(field.getModifiers())) {
				Object firstPropertyValue = field.get(first);
				Object secondPropertyValue = field.get(second);

				if (!firstPropertyValue.equals(secondPropertyValue)) {
					return false;
				}
			}
		}

		return true;
	}
}
