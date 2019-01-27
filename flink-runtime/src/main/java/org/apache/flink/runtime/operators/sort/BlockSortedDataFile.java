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

package org.apache.flink.runtime.operators.sort;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.ChannelBackendMutableObjectIterator;
import org.apache.flink.runtime.io.disk.ChannelReaderInputViewIterator;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelReader;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelWriter;
import org.apache.flink.runtime.io.disk.iomanager.ChannelReaderInputView;
import org.apache.flink.runtime.io.disk.iomanager.ChannelWriterOutputView;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;

import java.io.IOException;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A type of sorted data file who saves record continuously.
 */
public class BlockSortedDataFile<T> implements SortedDataFile<T> {
	private final FileIOChannel.ID channelID;
	private final TypeSerializer<T> serialize;
	private final IOManager ioManager;

	private final BlockChannelWriter<MemorySegment> blockFileWriter;
	private final ChannelWriterOutputView channelWriterOutputView;

	private boolean isWritingFinished;

	public BlockSortedDataFile(FileIOChannel.ID channelID, TypeSerializer<T> serialize, IOManager ioManager, List<MemorySegment> writeMemory) throws IOException {
		checkArgument(writeMemory.size() > 0, "Write memory are required for the BlockSortedDataFile.");

		this.channelID = channelID;
		this.serialize = serialize;
		this.ioManager = ioManager;

		blockFileWriter = ioManager.createBlockChannelWriter(channelID);
		channelWriterOutputView = new ChannelWriterOutputView(blockFileWriter, writeMemory, writeMemory.get(0).size());
	}

	@Override
	public FileIOChannel getWriteChannel() {
		return blockFileWriter;
	}

	@Override
	public FileIOChannel.ID getChannelID() {
		return channelID;
	}

	@Override
	public void writeRecord(T record) throws IOException {
		checkState(!isWritingFinished, "");

		serialize.serialize(record, channelWriterOutputView);
	}

	@Override
	public void copyRecord(DataInputView serializedRecord) throws IOException {
		checkState(!isWritingFinished, "");

		serialize.copy(serializedRecord, channelWriterOutputView);
	}

	@Override
	public long getBytesWritten() throws IOException {
		return channelWriterOutputView.getBytesWritten();
	}

	@Override
	public void finishWriting() throws IOException {
		if (isWritingFinished) {
			return;
		}

		channelWriterOutputView.close();
		isWritingFinished = true;
	}

	@Override
	public ChannelBackendMutableObjectIterator<T> createReader(List<MemorySegment> readMemory) throws IOException {
		checkState(isWritingFinished, "");

		final BlockChannelReader<MemorySegment> reader = this.ioManager.createBlockChannelReader(channelID);

		final ChannelReaderInputView inputView = new ChannelReaderInputView(
			reader, readMemory, channelWriterOutputView.getBlockCount(), false);

		return new ChannelReaderInputViewIterator<T>(inputView, null, this.serialize);
	}
}
