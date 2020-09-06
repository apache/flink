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

package org.apache.flink.runtime.state;

import org.apache.flink.core.memory.HeapMemorySegment;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateReader;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateReader.ReadResult;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateReaderImpl;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter.ChannelStateWriteResult;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriterImpl;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.checkpoint.channel.ResultSubpartitionInfo;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.memory.NonPersistentMetadataCheckpointStorageLocation;
import org.apache.flink.util.function.BiFunctionWithException;

import org.junit.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

import static java.util.Collections.singletonMap;
import static org.apache.flink.runtime.checkpoint.CheckpointType.CHECKPOINT;
import static org.apache.flink.runtime.checkpoint.channel.ChannelStateReader.ReadResult.NO_MORE_DATA;
import static org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter.SEQUENCE_NUMBER_UNKNOWN;
import static org.apache.flink.util.CloseableIterator.ofElements;
import static org.apache.flink.util.Preconditions.checkState;
import static org.junit.Assert.assertArrayEquals;

/**
 * ChannelPersistenceITCase.
 */
public class ChannelPersistenceITCase {
	private static final Random RANDOM = new Random(System.currentTimeMillis());

	@Test
	public void testReadWritten() throws Exception {
		long checkpointId = 1L;

		InputChannelInfo inputChannelInfo = new InputChannelInfo(2, 3);
		byte[] inputChannelInfoData = randomBytes(1024);

		ResultSubpartitionInfo resultSubpartitionInfo = new ResultSubpartitionInfo(4, 5);
		byte[] resultSubpartitionInfoData = randomBytes(1024);

		ChannelStateWriteResult handles = write(
			checkpointId,
			singletonMap(inputChannelInfo, inputChannelInfoData),
			singletonMap(resultSubpartitionInfo, resultSubpartitionInfoData)
		);

		assertArrayEquals(inputChannelInfoData, read(
			toTaskStateSnapshot(handles),
			inputChannelInfoData.length,
			(reader, mem) -> reader.readInputData(inputChannelInfo, new NetworkBuffer(mem, FreeingBufferRecycler.INSTANCE))
		));

		assertArrayEquals(resultSubpartitionInfoData, read(
			toTaskStateSnapshot(handles),
			resultSubpartitionInfoData.length,
			(reader, mem) -> reader.readOutputData(resultSubpartitionInfo, new BufferBuilder(mem, FreeingBufferRecycler.INSTANCE))
		));
	}

	private byte[] randomBytes(int size) {
		byte[] bytes = new byte[size];
		RANDOM.nextBytes(bytes);
		return bytes;
	}

	private ChannelStateWriteResult write(long checkpointId, Map<InputChannelInfo, byte[]> icMap, Map<ResultSubpartitionInfo, byte[]> rsMap) throws Exception {
		int maxStateSize = sizeOfBytes(icMap) + sizeOfBytes(rsMap) + Long.BYTES * 2;
		Map<InputChannelInfo, Buffer> icBuffers = wrapWithBuffers(icMap);
		Map<ResultSubpartitionInfo, Buffer> rsBuffers = wrapWithBuffers(rsMap);
		try (ChannelStateWriterImpl writer = new ChannelStateWriterImpl("test", getStreamFactoryFactory(maxStateSize))) {
			writer.open();
			writer.start(checkpointId, new CheckpointOptions(CHECKPOINT, new CheckpointStorageLocationReference("poly".getBytes())));
			for (Map.Entry<InputChannelInfo, Buffer> e : icBuffers.entrySet()) {
				writer.addInputData(checkpointId, e.getKey(), SEQUENCE_NUMBER_UNKNOWN, ofElements(Buffer::recycleBuffer, e.getValue()));
			}
			writer.finishInput(checkpointId);
			for (Map.Entry<ResultSubpartitionInfo, Buffer> e : rsBuffers.entrySet()) {
				writer.addOutputData(checkpointId, e.getKey(), SEQUENCE_NUMBER_UNKNOWN, e.getValue());
			}
			writer.finishOutput(checkpointId);
			ChannelStateWriteResult result = writer.getAndRemoveWriteResult(checkpointId);
			result.getResultSubpartitionStateHandles().join(); // prevent abnormal complete in close
			return result;
		}
	}

	public static CheckpointStorageWorkerView getStreamFactoryFactory() {
		return getStreamFactoryFactory(42);
	}

	public static CheckpointStorageWorkerView getStreamFactoryFactory(int maxStateSize) {
		return new CheckpointStorageWorkerView() {
			@Override
			public CheckpointStreamFactory resolveCheckpointStorageLocation(long checkpointId, CheckpointStorageLocationReference reference) {
				return new NonPersistentMetadataCheckpointStorageLocation(maxStateSize);
			}

			@Override
			public CheckpointStreamFactory.CheckpointStateOutputStream createTaskOwnedStateStream() {
				throw new UnsupportedOperationException();
			}
		};
	}

	private byte[] read(TaskStateSnapshot taskStateSnapshot, int size, BiFunctionWithException<ChannelStateReader, MemorySegment, ReadResult, Exception> readFn) throws Exception {
		byte[] dst = new byte[size];
		HeapMemorySegment mem = HeapMemorySegment.FACTORY.wrap(dst);
		try {
			checkState(NO_MORE_DATA == readFn.apply(new ChannelStateReaderImpl(taskStateSnapshot), mem));
		} finally {
			mem.free();
		}
		return dst;
	}

	private TaskStateSnapshot toTaskStateSnapshot(ChannelStateWriteResult t) throws Exception {
		return new TaskStateSnapshot(singletonMap(new OperatorID(),
			new OperatorSubtaskState(
				StateObjectCollection.empty(),
				StateObjectCollection.empty(),
				StateObjectCollection.empty(),
				StateObjectCollection.empty(),
				new StateObjectCollection<>(t.getInputChannelStateHandles().get()),
				new StateObjectCollection<>(t.getResultSubpartitionStateHandles().get())
			)
		));
	}

	@SuppressWarnings("unchecked")
	private <C> List<C> collect(Collection<StateObject> handles, Class<C> clazz) {
		return handles.stream().filter(clazz::isInstance).map(h -> (C) h).collect(Collectors.toList());
	}

	private static int sizeOfBytes(Map<?, byte[]> map) {
		return map.values().stream().mapToInt(d -> d.length).sum();
	}

	private <K> Map<K, Buffer> wrapWithBuffers(Map<K, byte[]> icMap) {
		return icMap.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> wrapWithBuffer(e.getValue())));
	}

	private static Buffer wrapWithBuffer(byte[] data) {
		NetworkBuffer buffer = new NetworkBuffer(HeapMemorySegment.FACTORY.allocateUnpooledSegment(data.length, null), FreeingBufferRecycler.INSTANCE);
		buffer.writeBytes(data);
		return buffer;
	}

}
