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

package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.core.memory.HeapMemorySegment;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter.ChannelStateWriteResult;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.state.InputChannelStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory.MemoryCheckpointOutputStream;
import org.apache.flink.util.function.RunnableWithException;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * {@link ChannelStateCheckpointWriter} test.
 */
public class ChannelStateCheckpointWriterTest {
	private static final RunnableWithException NO_OP_RUNNABLE = () -> {
	};
	private final Random random = new Random();

	@Test
	public void testEmptyState() throws Exception {
		MemoryCheckpointOutputStream stream = new MemoryCheckpointOutputStream(1000) {
			@Override
			public StreamStateHandle closeAndGetHandle() {
				fail("closeAndGetHandle shouldn't be called for empty channel state");
				return null;
			}
		};
		ChannelStateCheckpointWriter writer = new ChannelStateCheckpointWriter(
				1L,
				new ChannelStateWriteResult(),
				stream,
				new ChannelStateSerializerImpl(),
				NO_OP_RUNNABLE
		);
		writer.completeOutput();
		writer.completeInput();
		assertTrue(stream.isClosed());
	}

	@Test
	public void testRecyclingBuffers() throws Exception {
		ChannelStateCheckpointWriter writer = createWriter(new ChannelStateWriteResult());
		NetworkBuffer buffer = new NetworkBuffer(HeapMemorySegment.FACTORY.allocateUnpooledSegment(10, null), FreeingBufferRecycler.INSTANCE);
		writer.writeInput(new InputChannelInfo(1, 2), buffer);
		assertTrue(buffer.isRecycled());
	}

	@Test
	public void testFlush() throws Exception {
		class FlushRecorder extends DataOutputStream {
			private boolean flushed = false;

			private FlushRecorder() {
				super(new ByteArrayOutputStream());
			}

			@Override
			public void flush() throws IOException {
				flushed = true;
				super.flush();
			}
		}

		FlushRecorder dataStream = new FlushRecorder();
		final ChannelStateCheckpointWriter writer = new ChannelStateCheckpointWriter(
			1L,
			new ChannelStateWriteResult(),
			new ChannelStateSerializerImpl(),
			NO_OP_RUNNABLE,
			new MemoryCheckpointOutputStream(42),
			dataStream
		);

		writer.completeInput();
		writer.completeOutput();

		assertTrue(dataStream.flushed);
	}

	@Test
	public void testResultCompletion() throws Exception {
		ChannelStateWriteResult result = new ChannelStateWriteResult();
		ChannelStateCheckpointWriter writer = createWriter(result);
		writer.completeInput();
		assertFalse(result.isDone());
		writer.completeOutput();
		assertTrue(result.isDone());
	}

	@Test
	public void testRecordingOffsets() throws Exception {
		Map<InputChannelInfo, Integer> offsetCounts = new HashMap<>();
		offsetCounts.put(new InputChannelInfo(1, 1), 1);
		offsetCounts.put(new InputChannelInfo(1, 2), 2);
		offsetCounts.put(new InputChannelInfo(1, 3), 99);

		ChannelStateWriteResult result = new ChannelStateWriteResult();
		ChannelStateCheckpointWriter writer = createWriter(result);
		for (Map.Entry<InputChannelInfo, Integer> e : offsetCounts.entrySet()) {
			for (int i = 0; i < e.getValue(); i++) {
				write(writer, e.getKey(), getData(100));
			}
		}
		writer.completeInput();
		writer.completeOutput();

		for (InputChannelStateHandle handle : result.inputChannelStateHandles.get()) {
			assertEquals(handle.getOffsets().size(), (int) offsetCounts.remove(handle.getInfo()));
		}
		assertTrue(offsetCounts.isEmpty());
	}

	private byte[] getData(int len) {
		byte[] bytes = new byte[len];
		random.nextBytes(bytes);
		return bytes;
	}

	private void write(ChannelStateCheckpointWriter writer, InputChannelInfo channelInfo, byte[] data) throws Exception {
		NetworkBuffer buffer = new NetworkBuffer(HeapMemorySegment.FACTORY.allocateUnpooledSegment(data.length, null), FreeingBufferRecycler.INSTANCE);
		buffer.setBytes(0, data);
		writer.writeInput(channelInfo, buffer);
	}

	private ChannelStateCheckpointWriter createWriter(ChannelStateWriteResult result) throws Exception {
		return new ChannelStateCheckpointWriter(
			1L,
			result,
			new MemoryCheckpointOutputStream(1000),
			new ChannelStateSerializerImpl(),
			NO_OP_RUNNABLE
		);
	}

}
