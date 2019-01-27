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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.ChannelBackendMutableObjectIterator;
import org.apache.flink.runtime.io.disk.iomanager.BufferFileWriter;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.external.PartitionIndex;

import java.io.IOException;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkState;

public class ConcatPartitionedBufferSortedDataFile<T> implements PartitionedSortedDataFile<T> {
	private final FileIOChannel.ID channel;
	private final int fileId;

	private final IOManager ioManager;
	private final BufferFileWriter streamFileWriter;

	private final PartitionIndexGenerator partitionIndexGenerator;
	private long bytesWritten;

	private boolean isWritingFinished;

	public ConcatPartitionedBufferSortedDataFile(int numberOfSubpartitions, FileIOChannel.ID channel, int fileId, IOManager ioManager) throws IOException {
		this.channel = channel;
		this.fileId = fileId;
		this.ioManager = ioManager;

		this.streamFileWriter = ioManager.createStreamFileWriter(channel);
		this.partitionIndexGenerator = new PartitionIndexGenerator(numberOfSubpartitions);
	}

	@Override
	public FileIOChannel getWriteChannel() {
		return streamFileWriter;
	}

	@Override
	public FileIOChannel.ID getChannelID() {
		return this.channel;
	}

	@Override
	public void writeRecord(Tuple2<Integer, T> record) throws IOException {
		throw new UnsupportedOperationException("Can not write record to concated buffer file.");
	}

	@Override
	public void copyRecord(DataInputView serializedRecord) throws IOException {
		throw new UnsupportedOperationException("Can not copy record to concated buffer file.");
	}

	@Override
	public long getBytesWritten() throws IOException {
		return bytesWritten;
	}

	public void writeBuffer(int partition, Buffer buffer) throws IOException {
		checkState(!isWritingFinished, "");

		partitionIndexGenerator.updatePartitionIndexBeforeWriting(partition, bytesWritten, 0);

		streamFileWriter.writeBlock(buffer);

		bytesWritten = bytesWritten + buffer.getSize();
	}

	@Override
	public void finishWriting() throws IOException {
		if (!isWritingFinished) {
			streamFileWriter.close();
			partitionIndexGenerator.finishWriting(bytesWritten, 0);

			isWritingFinished = true;
		}
	}

	@Override
	public ChannelBackendMutableObjectIterator<Tuple2<Integer, T>> createReader(List<MemorySegment> readMemory) throws IOException {
		//TODO: will be implemented later.
		return new ChannelBackendMutableObjectIterator<Tuple2<Integer, T>>() {
			@Override
			public FileIOChannel getReaderChannel() {
				return null;
			}

			@Override
			public Tuple2<Integer, T> next(Tuple2<Integer, T> reuse) throws IOException {
				return null;
			}

			@Override
			public Tuple2<Integer, T> next() throws IOException {
				return null;
			}
		};
	}

	@Override
	public List<PartitionIndex> getPartitionIndexList() {
		return partitionIndexGenerator.getPartitionIndices();
	}

	public int getFileId() {
		return fileId;
	}
}
