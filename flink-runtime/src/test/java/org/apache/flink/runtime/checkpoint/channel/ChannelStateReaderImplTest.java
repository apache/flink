package org.apache.flink.runtime.checkpoint.channel;
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.core.memory.HeapMemorySegment;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.InputChannelStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toMap;
import static org.apache.flink.runtime.checkpoint.channel.ChannelStateReader.ReadResult.HAS_MORE_DATA;
import static org.apache.flink.runtime.checkpoint.channel.ChannelStateReader.ReadResult.NO_MORE_DATA;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * {@link ChannelStateReaderImpl} test.
 */
public class ChannelStateReaderImplTest {

	private static final InputChannelInfo CHANNEL = new InputChannelInfo(1, 2);
	private static final byte[] DATA = generateData(10);
	private ChannelStateReaderImpl reader;

	@Before
	public void init() {
		reader = getReader(CHANNEL, DATA);
	}

	@After
	public void tearDown() throws Exception {
		reader.close();
	}

	@Test
	public void testDifferentBufferSizes() throws Exception {
		for (int bufferSize = 1; bufferSize < 2 * DATA.length; bufferSize++) {
			try (ChannelStateReaderImpl reader = getReader(CHANNEL, DATA)) { // re-create reader to re-read the same channel
				readAndVerify(bufferSize, CHANNEL, DATA, reader);
			}
		}
	}

	@Test
	public void testWithOffsets() throws IOException {
		Map<InputChannelStateHandle, byte[]> handlesAndBytes = generateHandlesWithBytes(10, 20);
		ChannelStateReader reader = new ChannelStateReaderImpl(taskStateSnapshot(handlesAndBytes.keySet()), new ChannelStateSerializerImpl());
		for (Map.Entry<InputChannelStateHandle, byte[]> e : handlesAndBytes.entrySet()) {
			readAndVerify(42, e.getKey().getInfo(), e.getValue(), reader);
		}
	}

	@Test(expected = Exception.class)
	public void testReadOnlyOnce() throws IOException {
		reader.readInputData(CHANNEL, getBuffer(DATA.length));
		reader.readInputData(CHANNEL, getBuffer(DATA.length));
	}

	@Test(expected = IllegalStateException.class)
	public void testReadClosed() throws Exception {
		reader.close();
		reader.readInputData(CHANNEL, getBuffer(DATA.length));
	}

	@Test
	public void testReadUnknownChannelState() throws IOException {
		InputChannelInfo unknownChannel = new InputChannelInfo(CHANNEL.getGateIdx() + 1, CHANNEL.getInputChannelIdx() + 1);
		assertEquals(NO_MORE_DATA, reader.readInputData(unknownChannel, getBuffer(DATA.length)));
	}

	private TaskStateSnapshot taskStateSnapshot(Collection<InputChannelStateHandle> inputChannelStateHandles) {
		return new TaskStateSnapshot(Collections.singletonMap(
			new OperatorID(),
			new OperatorSubtaskState(
				StateObjectCollection.empty(),
				StateObjectCollection.empty(),
				StateObjectCollection.empty(),
				StateObjectCollection.empty(),
				new StateObjectCollection<>(inputChannelStateHandles),
				StateObjectCollection.empty()
			)));
	}

	static byte[] generateData(int len) {
		byte[] bytes = new byte[len];
		new Random().nextBytes(bytes);
		return bytes;
	}

	private byte[] toBytes(NetworkBuffer buffer) {
		byte[] buf = new byte[buffer.readableBytes()];
		buffer.readBytes(buf);
		return buf;
	}

	private ChannelStateReaderImpl getReader(InputChannelInfo channel, byte[] data) {
		return new ChannelStateReaderImpl(
			taskStateSnapshot(singletonList(new InputChannelStateHandle(channel, new ByteStreamStateHandle("", data), singletonList(0L)))),
			new ChannelStateSerializerImpl() {
				@Override
				public void readHeader(InputStream stream) {
				}

				@Override
				public int readLength(InputStream stream) {
					return data.length;
				}
			});
	}

	private void readAndVerify(int bufferSize, InputChannelInfo channelInfo, byte[] data, ChannelStateReader reader) throws IOException {
		int dataSize = data.length;
		int iterations = dataSize / bufferSize + (-(dataSize % bufferSize) >>> 31);
		NetworkBuffer buffer = getBuffer(bufferSize);
		try {
			for (int i = 0; i < iterations; i++) {
				String hint = String.format("dataSize=%d, bufferSize=%d, iteration=%d/%d", dataSize, bufferSize, i + 1, iterations);
				boolean isLast = i == iterations - 1;
				assertEquals(hint, isLast ? NO_MORE_DATA : HAS_MORE_DATA, reader.readInputData(channelInfo, buffer));
				assertEquals(hint, isLast ? dataSize - bufferSize * i : bufferSize, buffer.readableBytes());
				assertArrayEquals(hint, Arrays.copyOfRange(data, i * bufferSize, Math.min(dataSize, (i + 1) * bufferSize)), toBytes(buffer));
				buffer.resetReaderIndex();
				buffer.resetWriterIndex();
			}
		} finally {
			buffer.release();
		}
	}

	private NetworkBuffer getBuffer(int len) {
		return new NetworkBuffer(HeapMemorySegment.FACTORY.allocateUnpooledSegment(len, null), FreeingBufferRecycler.INSTANCE);
	}

	private Map<InputChannelStateHandle, byte[]> generateHandlesWithBytes(int numHandles, int handleDataSize) throws IOException {
		Map<Integer, byte[]> offsetsAndBytes = new HashMap<>();
		ByteArrayOutputStream baos = new ByteArrayOutputStream(100);
		DataOutputStream out = new DataOutputStream(baos);
		ChannelStateSerializerImpl serializer = new ChannelStateSerializerImpl();
		serializer.writeHeader(out);
		for (int i = 0; i < numHandles; i++) {
			offsetsAndBytes.put(baos.size(), writeSomeBytes(handleDataSize, out, serializer));
		}
		ByteStreamStateHandle sharedUnderlyingHandle = new ByteStreamStateHandle("", baos.toByteArray());
		return offsetsAndBytes.entrySet().stream().collect(toMap(
			e -> new InputChannelStateHandle(new InputChannelInfo(e.getKey(), e.getKey()), sharedUnderlyingHandle, singletonList((long) e.getKey())),
			Map.Entry::getValue));
	}

	private byte[] writeSomeBytes(int bytesCount, DataOutputStream out, ChannelStateSerializer serializer) throws IOException {
		byte[] bytes = generateData(bytesCount);
		NetworkBuffer buf = getBuffer(bytesCount);
		try {
			buf.writeBytes(bytes);
			serializer.writeData(out, buf);
			return bytes;
		} finally {
			buf.release();
		}
	}

}
