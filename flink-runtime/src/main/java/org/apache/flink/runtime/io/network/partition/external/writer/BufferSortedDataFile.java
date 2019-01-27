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

package org.apache.flink.runtime.io.network.partition.external.writer;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.io.disk.ChannelBackendMutableObjectIterator;
import org.apache.flink.runtime.io.disk.iomanager.BufferFileWriter;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.api.serialization.RecordSerializer;
import org.apache.flink.runtime.io.network.api.serialization.SerializerManager;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.partition.FixedLengthBufferPool;
import org.apache.flink.runtime.operators.sort.SortedDataFile;
import org.apache.flink.runtime.plugable.CopySerializationDelegate;
import org.apache.flink.runtime.plugable.SerializationDelegate;

import java.io.IOException;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A type of sorted data file who saves the record in the format of serialized buffers.
 */
public class BufferSortedDataFile<T> implements SortedDataFile<T> {
	private final FileIOChannel.ID channelID;
	private final int fileId;

	private final RecordSerializer<IOReadableWritable> recordSerializer;
	private final SerializationDelegate<T> serializationDelegate;
	private final CopySerializationDelegate<T> copySerializationDelegate;

	private final FixedLengthBufferPool bufferPool;
	private final BufferFileWriter streamFileWriter;

	private final Counter numBytesOut;
	private final Counter numBuffersOut;

	private BufferBuilder currentBufferBuilders;
	private long bytesWritten;

	private boolean isWritingFinished;

	public BufferSortedDataFile(FileIOChannel.ID channelID, int fileId, TypeSerializer<T> serializer,
								IOManager ioManager, List<MemorySegment> writeMemory,
								SerializerManager<SerializationDelegate<T>> serializerManager,
								Counter numBytesOut, Counter numBuffersOut) throws IOException {
		this.channelID = channelID;
		this.fileId = fileId;

		this.recordSerializer = serializerManager.getRecordSerializer();
		this.serializationDelegate = new SerializationDelegate<>(serializer);
		this.copySerializationDelegate = new CopySerializationDelegate<>(serializer);

		this.bufferPool = new FixedLengthBufferPool(writeMemory, false);
		this.streamFileWriter = ioManager.createStreamFileWriter(channelID);

		this.numBytesOut = numBytesOut;
		this.numBuffersOut = numBuffersOut;
	}

	@Override
	public FileIOChannel getWriteChannel() {
		return streamFileWriter;
	}

	@Override
	public FileIOChannel.ID getChannelID() {
		return channelID;
	}

	@Override
	public void writeRecord(T record) throws IOException {
		serializationDelegate.setInstance(record);
		recordSerializer.serializeRecord(serializationDelegate);
		copyToFile();
	}

	@Override
	public void copyRecord(DataInputView serializedRecord) throws IOException {
		checkState(!isWritingFinished, "");

		copySerializationDelegate.setInputView(serializedRecord);
		recordSerializer.serializeRecord(copySerializationDelegate);

		copyToFile();
	}

	@Override
	public void finishWriting() throws IOException {
		if (isWritingFinished) {
			return;
		}

		if (recordSerializer.hasSerializedData()) {
			flushInternalSerializer();
		}
		checkState(!recordSerializer.hasSerializedData(), "All data should be written at once");
		tryFinishCurrentBufferBuilder();

		streamFileWriter.close();
		isWritingFinished = true;
	}

	@Override
	public ChannelBackendMutableObjectIterator<T> createReader(List<MemorySegment> readMemory) throws IOException {
		// TODO: will be implemented later
		return new ChannelBackendMutableObjectIterator<T>() {
			@Override
			public FileIOChannel getReaderChannel() {
				return null;
			}

			@Override
			public T next(T reuse) throws IOException {
				return null;
			}

			@Override
			public T next() throws IOException {
				return null;
			}
		};
	}

	public void flush() throws IOException {
		if (recordSerializer.hasSerializedData()) {
			flushInternalSerializer();
		}
		checkState(!recordSerializer.hasSerializedData(), "All data should be written at once");
		tryFinishCurrentBufferBuilder();
	}

	private void flushInternalSerializer() throws IOException {
		BufferBuilder bufferBuilder = getCurrentBufferBuilder();
		RecordSerializer.SerializationResult result = recordSerializer.flushToBufferBuilder(bufferBuilder);

		while (result.isFullBuffer()) {
			tryFinishCurrentBufferBuilder();

			if (result.isFullRecord()) {
				break;
			}

			bufferBuilder = getCurrentBufferBuilder();
			result = recordSerializer.flushToBufferBuilder(bufferBuilder);
		}
	}

	public long getBytesWritten() {
		return bytesWritten;
	}

	public int getFileId() {
		return fileId;
	}

	private void copyToFile() throws IOException {
		recordSerializer.reset();

		BufferBuilder bufferBuilder = getCurrentBufferBuilder();
		RecordSerializer.SerializationResult result = recordSerializer.copyToBufferBuilder(bufferBuilder);

		while (result.isFullBuffer()) {
			tryFinishCurrentBufferBuilder();

			if (result.isFullRecord()) {
				break;
			}

			bufferBuilder = getCurrentBufferBuilder();
			result = recordSerializer.copyToBufferBuilder(bufferBuilder);
		}
	}

	private BufferBuilder getCurrentBufferBuilder() throws IOException {
		if (currentBufferBuilders == null) {
			try {
				currentBufferBuilders = bufferPool.requestBufferBuilderBlocking();
			} catch (InterruptedException e) {
				throw new IOException("Failed to request buffer");
			}
			checkState(currentBufferBuilders != null,
				"Failed to request a buffer.");
		}

		return currentBufferBuilders;
	}

	private void tryFinishCurrentBufferBuilder() throws IOException {
		if (currentBufferBuilders != null) {
			int bufferSize = currentBufferBuilders.finish();

			BufferConsumer bufferConsumer = currentBufferBuilders.createBufferConsumer();
			Buffer buffer = bufferConsumer.build();

			streamFileWriter.writeBlock(buffer);
			bytesWritten = bytesWritten + bufferSize;

			if (numBytesOut != null) {
				numBytesOut.inc(bufferSize);
			}

			if (numBuffersOut != null) {
				numBuffersOut.inc();
			}

			bufferConsumer.close();
			currentBufferBuilders = null;
		}
	}
}
