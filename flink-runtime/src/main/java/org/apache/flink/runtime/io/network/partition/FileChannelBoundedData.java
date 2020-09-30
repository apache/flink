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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.util.IOUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * An implementation of {@link BoundedData} that writes directly into a File Channel.
 * The readers are simple file channel readers using a simple dedicated buffer pool.
 */
final class FileChannelBoundedData implements BoundedData {

	private final Path filePath;

	private final FileChannel fileChannel;

	private final ByteBuffer[] headerAndBufferArray;

	private long size;

	FileChannelBoundedData(Path filePath, FileChannel fileChannel) {
		this.filePath = checkNotNull(filePath);
		this.fileChannel = checkNotNull(fileChannel);
		this.headerAndBufferArray = BufferReaderWriterUtil.allocatedWriteBufferArray();
	}

	@Override
	public void writeBuffer(Buffer buffer) throws IOException {
		size += BufferReaderWriterUtil.writeToByteChannel(fileChannel, buffer, headerAndBufferArray);
	}

	@Override
	public void finishWrite() throws IOException {
		fileChannel.close();
	}

	@Override
	public Reader createReader(ResultSubpartitionView subpartitionView) throws IOException {
		checkState(!fileChannel.isOpen());

		final FileChannel fc = FileChannel.open(filePath, StandardOpenOption.READ);
		return new FileBufferReader(fc);
	}

	@Override
	public long getSize() {
		return size;
	}

	@Override
	public void close() throws IOException {
		IOUtils.closeQuietly(fileChannel);
		Files.delete(filePath);
	}

	// ------------------------------------------------------------------------

	public static FileChannelBoundedData create(Path filePath) throws IOException {
		final FileChannel fileChannel = FileChannel.open(
				filePath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);

		return new FileChannelBoundedData(filePath, fileChannel);
	}

	// ------------------------------------------------------------------------

	static final class FileBufferReader implements BoundedData.Reader {

		private final FileChannel fileChannel;

		private final ByteBuffer headerBuffer;

		FileBufferReader(FileChannel fileChannel) {
			this.fileChannel = checkNotNull(fileChannel);
			this.headerBuffer = BufferReaderWriterUtil.allocatedHeaderBuffer();
		}

		@Nullable
		@Override
		public BoundedPartitionData nextData() throws IOException {
			return BufferReaderWriterUtil.readFromByteChannel(fileChannel, headerBuffer);
		}

		@Override
		public void close() throws IOException {
			fileChannel.close();
		}
	}
}
