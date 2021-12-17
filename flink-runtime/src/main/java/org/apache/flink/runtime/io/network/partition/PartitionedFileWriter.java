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
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.IOUtils;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * File writer which can write buffers and generate {@link PartitionedFile}. Data is written region
 * by region. Before writing a new region, the method {@link PartitionedFileWriter#startNewRegion}
 * must be called. After writing all data, the method {@link PartitionedFileWriter#finish} must be
 * called to close all opened files and return the target {@link PartitionedFile}.
 */
@NotThreadSafe
public class PartitionedFileWriter implements AutoCloseable {

    private static final int MIN_INDEX_BUFFER_SIZE = 50 * PartitionedFile.INDEX_ENTRY_SIZE;

    /** Number of channels. When writing a buffer, target subpartition must be in this range. */
    private final int numSubpartitions;

    /** Opened data file channel of the target {@link PartitionedFile}. */
    private final FileChannel dataFileChannel;

    /** Opened index file channel of the target {@link PartitionedFile}. */
    private final FileChannel indexFileChannel;

    /** Data file path of the target {@link PartitionedFile}. */
    private final Path dataFilePath;

    /** Index file path of the target {@link PartitionedFile}. */
    private final Path indexFilePath;

    /** Offset in the data file for each subpartition in the current region. */
    private final long[] subpartitionOffsets;

    /** Number of buffers written for each subpartition in the current region. */
    private final int[] subpartitionBuffers;

    /** Maximum number of bytes can be used to buffer index entries. */
    private final int maxIndexBufferSize;

    /** A piece of unmanaged memory for caching of region index entries. */
    private ByteBuffer indexBuffer;

    /** Whether all index entries are cached in the index buffer or not. */
    private boolean allIndexEntriesCached = true;

    /** Number of bytes written to the target {@link PartitionedFile}. */
    private long totalBytesWritten;

    /** Number of regions written to the target {@link PartitionedFile}. */
    private int numRegions;

    /** Total number of buffers in the data file. */
    private long numBuffers;

    /** Current subpartition to write buffers to. */
    private int currentSubpartition = -1;

    /**
     * Broadcast region is an optimization for the broadcast partition which writes the same data to
     * all subpartitions. For a broadcast region, data is only written once and the indexes of all
     * subpartitions point to the same offset in the data file.
     */
    private boolean isBroadcastRegion;

    /** Whether this file writer is finished or not. */
    private boolean isFinished;

    /** Whether this file writer is closed or not. */
    private boolean isClosed;

    public PartitionedFileWriter(int numSubpartitions, int maxIndexBufferSize, String basePath)
            throws IOException {
        checkArgument(numSubpartitions > 0, "Illegal number of subpartitions.");
        checkArgument(maxIndexBufferSize > 0, "Illegal maximum index cache size.");
        checkArgument(basePath != null, "Base path must not be null.");

        this.numSubpartitions = numSubpartitions;
        this.maxIndexBufferSize = alignMaxIndexBufferSize(maxIndexBufferSize);
        this.subpartitionOffsets = new long[numSubpartitions];
        this.subpartitionBuffers = new int[numSubpartitions];
        this.dataFilePath = new File(basePath + PartitionedFile.DATA_FILE_SUFFIX).toPath();
        this.indexFilePath = new File(basePath + PartitionedFile.INDEX_FILE_SUFFIX).toPath();

        this.indexBuffer = ByteBuffer.allocate(MIN_INDEX_BUFFER_SIZE);
        BufferReaderWriterUtil.configureByteBuffer(indexBuffer);

        this.dataFileChannel = openFileChannel(dataFilePath);
        try {
            this.indexFileChannel = openFileChannel(indexFilePath);
        } catch (Throwable throwable) {
            // ensure that the data file channel is closed if any exception occurs
            IOUtils.closeQuietly(dataFileChannel);
            IOUtils.deleteFileQuietly(dataFilePath);
            throw throwable;
        }
    }

    private FileChannel openFileChannel(Path path) throws IOException {
        return FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
    }

    private int alignMaxIndexBufferSize(int maxIndexBufferSize) {
        return maxIndexBufferSize
                / PartitionedFile.INDEX_ENTRY_SIZE
                * PartitionedFile.INDEX_ENTRY_SIZE;
    }

    /**
     * Persists the region index of the current data region and starts a new region to write.
     *
     * <p>Note: The caller is responsible for releasing the failed {@link PartitionedFile} if any
     * exception occurs.
     *
     * @param isBroadcastRegion Whether it's a broadcast region. See {@link #isBroadcastRegion}.
     */
    public void startNewRegion(boolean isBroadcastRegion) throws IOException {
        checkState(!isFinished, "File writer is already finished.");
        checkState(!isClosed, "File writer is already closed.");

        writeRegionIndex();
        this.isBroadcastRegion = isBroadcastRegion;
    }

    private void writeIndexEntry(long subpartitionOffset, int numBuffers) throws IOException {
        if (!indexBuffer.hasRemaining()) {
            if (!extendIndexBufferIfPossible()) {
                flushIndexBuffer();
                indexBuffer.clear();
                allIndexEntriesCached = false;
            }
        }

        indexBuffer.putLong(subpartitionOffset);
        indexBuffer.putInt(numBuffers);
    }

    private boolean extendIndexBufferIfPossible() {
        if (indexBuffer.capacity() >= maxIndexBufferSize) {
            return false;
        }

        int newIndexBufferSize = Math.min(maxIndexBufferSize, 2 * indexBuffer.capacity());
        ByteBuffer newIndexBuffer = ByteBuffer.allocate(newIndexBufferSize);
        indexBuffer.flip();
        newIndexBuffer.put(indexBuffer);
        BufferReaderWriterUtil.configureByteBuffer(newIndexBuffer);
        indexBuffer = newIndexBuffer;

        return true;
    }

    private void writeRegionIndex() throws IOException {
        if (Arrays.stream(subpartitionBuffers).sum() > 0) {
            for (int channel = 0; channel < numSubpartitions; ++channel) {
                writeIndexEntry(subpartitionOffsets[channel], subpartitionBuffers[channel]);
            }

            currentSubpartition = -1;
            ++numRegions;
            Arrays.fill(subpartitionBuffers, 0);
        }
    }

    private void flushIndexBuffer() throws IOException {
        indexBuffer.flip();
        if (indexBuffer.limit() > 0) {
            BufferReaderWriterUtil.writeBuffer(indexFileChannel, indexBuffer);
        }
    }

    /**
     * Writes a list of {@link Buffer}s to this {@link PartitionedFile}. It guarantees that after
     * the return of this method, the target buffers can be released. In a data region, all data of
     * the same subpartition must be written together.
     *
     * <p>Note: The caller is responsible for recycling the target buffers and releasing the failed
     * {@link PartitionedFile} if any exception occurs.
     */
    public void writeBuffers(List<BufferWithChannel> bufferWithChannels) throws IOException {
        checkState(!isFinished, "File writer is already finished.");
        checkState(!isClosed, "File writer is already closed.");

        if (bufferWithChannels.isEmpty()) {
            return;
        }

        numBuffers += bufferWithChannels.size();
        long expectedBytes;
        ByteBuffer[] bufferWithHeaders = new ByteBuffer[2 * bufferWithChannels.size()];

        if (isBroadcastRegion) {
            expectedBytes = collectBroadcastBuffers(bufferWithChannels, bufferWithHeaders);
        } else {
            expectedBytes = collectUnicastBuffers(bufferWithChannels, bufferWithHeaders);
        }

        totalBytesWritten += expectedBytes;
        BufferReaderWriterUtil.writeBuffers(dataFileChannel, expectedBytes, bufferWithHeaders);
    }

    private long collectUnicastBuffers(
            List<BufferWithChannel> bufferWithChannels, ByteBuffer[] bufferWithHeaders) {
        long expectedBytes = 0;
        long fileOffset = totalBytesWritten;
        for (int i = 0; i < bufferWithChannels.size(); i++) {
            int subpartition = bufferWithChannels.get(i).getChannelIndex();
            if (subpartition != currentSubpartition) {
                checkState(
                        subpartitionBuffers[subpartition] == 0,
                        "Must write data of the same channel together.");
                subpartitionOffsets[subpartition] = fileOffset;
                currentSubpartition = subpartition;
            }

            Buffer buffer = bufferWithChannels.get(i).getBuffer();
            int numBytes = setBufferWithHeader(buffer, bufferWithHeaders, 2 * i);
            expectedBytes += numBytes;
            fileOffset += numBytes;
            ++subpartitionBuffers[subpartition];
        }
        return expectedBytes;
    }

    private long collectBroadcastBuffers(
            List<BufferWithChannel> bufferWithChannels, ByteBuffer[] bufferWithHeaders) {
        // set the file offset of all channels as the current file size on the first call
        if (subpartitionBuffers[0] == 0) {
            for (int subpartition = 0; subpartition < numSubpartitions; ++subpartition) {
                subpartitionOffsets[subpartition] = totalBytesWritten;
            }
        }

        for (int subpartition = 0; subpartition < numSubpartitions; ++subpartition) {
            subpartitionBuffers[subpartition] += bufferWithChannels.size();
        }

        long expectedBytes = 0;
        for (int i = 0; i < bufferWithChannels.size(); i++) {
            Buffer buffer = bufferWithChannels.get(i).getBuffer();
            int numBytes = setBufferWithHeader(buffer, bufferWithHeaders, 2 * i);
            expectedBytes += numBytes;
        }
        return expectedBytes;
    }

    private int setBufferWithHeader(Buffer buffer, ByteBuffer[] bufferWithHeaders, int index) {
        ByteBuffer header = BufferReaderWriterUtil.allocatedHeaderBuffer();
        BufferReaderWriterUtil.setByteChannelBufferHeader(buffer, header);

        bufferWithHeaders[index] = header;
        bufferWithHeaders[index + 1] = buffer.getNioBufferReadable();

        return header.remaining() + buffer.readableBytes();
    }

    /**
     * Finishes writing the {@link PartitionedFile} which closes the file channel and returns the
     * corresponding {@link PartitionedFile}.
     *
     * <p>Note: The caller is responsible for releasing the failed {@link PartitionedFile} if any
     * exception occurs.
     */
    public PartitionedFile finish() throws IOException {
        checkState(!isFinished, "File writer is already finished.");
        checkState(!isClosed, "File writer is already closed.");

        isFinished = true;

        writeRegionIndex();
        flushIndexBuffer();
        indexBuffer.rewind();

        long dataFileSize = dataFileChannel.size();
        long indexFileSize = indexFileChannel.size();
        close();

        ByteBuffer indexEntryCache = null;
        if (allIndexEntriesCached) {
            indexEntryCache = indexBuffer;
        }
        indexBuffer = null;
        return new PartitionedFile(
                numRegions,
                numSubpartitions,
                dataFilePath,
                indexFilePath,
                dataFileSize,
                indexFileSize,
                numBuffers,
                indexEntryCache);
    }

    /** Used to close and delete the failed {@link PartitionedFile} when any exception occurs. */
    public void releaseQuietly() {
        IOUtils.closeQuietly(this);
        IOUtils.deleteFileQuietly(dataFilePath);
        IOUtils.deleteFileQuietly(indexFilePath);
    }

    @Override
    public void close() throws IOException {
        if (isClosed) {
            return;
        }
        isClosed = true;

        IOException exception = null;
        try {
            dataFileChannel.close();
        } catch (IOException ioException) {
            exception = ioException;
        }

        try {
            indexFileChannel.close();
        } catch (IOException ioException) {
            exception = ExceptionUtils.firstOrSuppressed(ioException, exception);
        }

        if (exception != null) {
            throw exception;
        }
    }
}
