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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.IOUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil.readFromByteChannel;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Reader which can read all data of the target subpartition from a {@link PartitionedFile}.
 */
public class PartitionedFileReader implements AutoCloseable {

	/** Used to read buffers from file channel. */
	private final ByteBuffer headerBuf = BufferReaderWriterUtil.allocatedHeaderBuffer();

	/** Used to read index entry from index file. */
	private final ByteBuffer indexEntryBuf;

	/** Target {@link PartitionedFile} to read. */
	private final PartitionedFile partitionedFile;

	/** Target subpartition to read. */
	private final int targetSubpartition;

	/** Data file channel of the target {@link PartitionedFile}. */
	private final FileChannel dataFileChannel;

	/** Index file channel of the target {@link PartitionedFile}. */
	private final FileChannel indexFileChannel;

	/** Next data region to be read. */
	private int nextRegionToRead;

	/** Number of remaining buffers in the current data region read. */
	private int currentRegionRemainingBuffers;

	/** Whether this partitioned file reader is closed. */
	private boolean isClosed;

	public PartitionedFileReader(
			PartitionedFile partitionedFile,
			int targetSubpartition) throws IOException {
		this.partitionedFile = checkNotNull(partitionedFile);
		this.targetSubpartition = targetSubpartition;

		this.indexEntryBuf = ByteBuffer.allocateDirect(PartitionedFile.INDEX_ENTRY_SIZE);
		BufferReaderWriterUtil.configureByteBuffer(indexEntryBuf);

		this.dataFileChannel = openFileChannel(partitionedFile.getDataFilePath());
		try {
			this.indexFileChannel = openFileChannel(partitionedFile.getIndexFilePath());
		} catch (Throwable throwable) {
			IOUtils.closeQuietly(dataFileChannel);
			throw throwable;
		}
	}

	private FileChannel openFileChannel(Path path) throws IOException {
		return FileChannel.open(path, StandardOpenOption.READ);
	}

	private boolean moveToNextReadableRegion() throws IOException {
		if (currentRegionRemainingBuffers > 0) {
			return true;
		}

		while (nextRegionToRead < partitionedFile.getNumRegions()) {
			partitionedFile.getIndexEntry(
				indexFileChannel, indexEntryBuf, nextRegionToRead, targetSubpartition);
			long dataOffset = indexEntryBuf.getLong();
			currentRegionRemainingBuffers = indexEntryBuf.getInt();
			++nextRegionToRead;

			if (currentRegionRemainingBuffers > 0) {
				dataFileChannel.position(dataOffset);
				return true;
			}
		}

		return false;
	}

	/**
	 * Reads a buffer from the {@link PartitionedFile} and moves the read position forward.
	 *
	 * <p>Note: The caller is responsible for recycling the target buffer if any exception occurs.
	 */
	@Nullable
	public Buffer readBuffer(MemorySegment target, BufferRecycler recycler) throws IOException {
		checkState(!isClosed, "File reader is already closed.");

		if (moveToNextReadableRegion()) {
			--currentRegionRemainingBuffers;
			return readFromByteChannel(dataFileChannel, headerBuf, target, recycler);
		}

		return null;
	}

	@VisibleForTesting
	public boolean hasRemaining() throws IOException {
		checkState(!isClosed, "File reader is already closed.");

		return moveToNextReadableRegion();
	}

	@Override
	public void close() throws IOException {
		if (isClosed) {
			return;
		}
		isClosed = true;

		IOException exception = null;
		try {
			if (dataFileChannel != null) {
				dataFileChannel.close();
			}
		} catch (IOException ioException) {
			exception = ioException;
		}

		try {
			if (indexFileChannel != null) {
				indexFileChannel.close();
			}
		} catch (IOException ioException) {
			exception = ExceptionUtils.firstOrSuppressed(ioException, exception);
		}

		if (exception != null) {
			throw exception;
		}
	}
}
