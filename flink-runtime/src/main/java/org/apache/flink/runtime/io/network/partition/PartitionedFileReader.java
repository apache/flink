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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil.readFromByteChannel;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Reader which can read all data of the target subpartition from a {@link PartitionedFile}. */
class PartitionedFileReader {

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

    /** Next file offset to be read. */
    private long nextOffsetToRead;

    /** Number of remaining buffers in the current data region read. */
    private int currentRegionRemainingBuffers;

    PartitionedFileReader(
            PartitionedFile partitionedFile,
            int targetSubpartition,
            FileChannel dataFileChannel,
            FileChannel indexFileChannel) {
        checkArgument(checkNotNull(dataFileChannel).isOpen(), "Data file channel must be opened.");
        checkArgument(
                checkNotNull(indexFileChannel).isOpen(), "Index file channel must be opened.");

        this.partitionedFile = checkNotNull(partitionedFile);
        this.targetSubpartition = targetSubpartition;
        this.dataFileChannel = dataFileChannel;
        this.indexFileChannel = indexFileChannel;

        this.indexEntryBuf = ByteBuffer.allocateDirect(PartitionedFile.INDEX_ENTRY_SIZE);
        BufferReaderWriterUtil.configureByteBuffer(indexEntryBuf);
    }

    private void moveToNextReadableRegion() throws IOException {
        while (currentRegionRemainingBuffers <= 0
                && nextRegionToRead < partitionedFile.getNumRegions()) {
            partitionedFile.getIndexEntry(
                    indexFileChannel, indexEntryBuf, nextRegionToRead, targetSubpartition);
            nextOffsetToRead = indexEntryBuf.getLong();
            currentRegionRemainingBuffers = indexEntryBuf.getInt();
            ++nextRegionToRead;
        }
    }

    /**
     * Reads a buffer from the current region of the target {@link PartitionedFile} and moves the
     * read position forward.
     *
     * <p>Note: The caller is responsible for recycling the target buffer if any exception occurs.
     *
     * @param target The target {@link MemorySegment} to read data to.
     * @param recycler The {@link BufferRecycler} which is responsible to recycle the target buffer.
     * @return A {@link Buffer} containing the data read.
     */
    @Nullable
    Buffer readCurrentRegion(MemorySegment target, BufferRecycler recycler) throws IOException {
        if (currentRegionRemainingBuffers == 0) {
            return null;
        }

        dataFileChannel.position(nextOffsetToRead);
        Buffer buffer = readFromByteChannel(dataFileChannel, headerBuf, target, recycler);
        nextOffsetToRead = dataFileChannel.position();
        --currentRegionRemainingBuffers;
        return buffer;
    }

    boolean hasRemaining() throws IOException {
        moveToNextReadableRegion();
        return currentRegionRemainingBuffers > 0;
    }

    /** Gets read priority of this file reader. Smaller value indicates higher priority. */
    long getPriority() {
        return nextOffsetToRead;
    }
}
