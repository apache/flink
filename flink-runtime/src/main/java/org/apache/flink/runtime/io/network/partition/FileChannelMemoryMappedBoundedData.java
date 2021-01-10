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

import org.apache.flink.shaded.netty4.io.netty.util.internal.PlatformDependent;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * An implementation of {@link BoundedData} that writes directly into a File Channel and maps the
 * file into memory after writing. Readers simply access the memory mapped data. All readers access
 * the same memory, which is mapped in a read-only manner.
 *
 * <p>Similarly as the {@link MemoryMappedBoundedData}, this implementation needs to work around the
 * fact that the memory mapped regions cannot exceed 2GB in Java. While the implementation writes to
 * the a single file, the result may be multiple memory mapped buffers.
 *
 * <h2>Important!</h2>
 *
 * <p>This class performs absolutely no synchronization and relies on single threaded access or
 * externally synchronized access. Concurrent access around disposal may cause segmentation faults!
 */
final class FileChannelMemoryMappedBoundedData implements BoundedData {

    /** The file channel backing the memory mapped file. */
    private final FileChannel fileChannel;

    /**
     * The reusable array with header buffer and data buffer, to use gathering writes on the file
     * channel ({@link java.nio.channels.GatheringByteChannel#write(ByteBuffer[])}).
     */
    private final ByteBuffer[] headerAndBufferArray;

    /** All memory mapped regions. */
    private final ArrayList<ByteBuffer> memoryMappedRegions;

    /** The path of the memory mapped file. */
    private final Path filePath;

    /**
     * The position in the file channel. Cached for efficiency, because an actual position lookup in
     * the channel involves various locks and checks.
     */
    private long pos;

    /** The position where the current memory mapped region must end. */
    private long endOfCurrentRegion;

    /** The position where the current memory mapped started. */
    private long startOfCurrentRegion;

    /** The maximum size of each mapped region. */
    private final long maxRegionSize;

    FileChannelMemoryMappedBoundedData(
            Path filePath, FileChannel fileChannel, int maxSizePerMappedRegion) {

        this.filePath = filePath;
        this.fileChannel = fileChannel;
        this.headerAndBufferArray = BufferReaderWriterUtil.allocatedWriteBufferArray();
        this.memoryMappedRegions = new ArrayList<>(4);
        this.maxRegionSize = maxSizePerMappedRegion;
        this.endOfCurrentRegion = maxSizePerMappedRegion;
    }

    @Override
    public void writeBuffer(Buffer buffer) throws IOException {
        if (tryWriteBuffer(buffer)) {
            return;
        }

        mapRegionAndStartNext();

        if (!tryWriteBuffer(buffer)) {
            throwTooLargeBuffer(buffer);
        }
    }

    private boolean tryWriteBuffer(Buffer buffer) throws IOException {
        final long spaceLeft = endOfCurrentRegion - pos;
        final long bytesWritten =
                BufferReaderWriterUtil.writeToByteChannelIfBelowSize(
                        fileChannel, buffer, headerAndBufferArray, spaceLeft);

        if (bytesWritten >= 0) {
            pos += bytesWritten;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public BoundedData.Reader createReader(ResultSubpartitionView ignored) {
        checkState(!fileChannel.isOpen());

        final List<ByteBuffer> buffers =
                memoryMappedRegions.stream()
                        .map((bb) -> bb.duplicate().order(ByteOrder.nativeOrder()))
                        .collect(Collectors.toList());

        return new MemoryMappedBoundedData.BufferSlicer(buffers);
    }

    /**
     * Finishes the current region and prevents further writes. After calling this method, further
     * calls to {@link #writeBuffer(Buffer)} will fail.
     */
    @Override
    public void finishWrite() throws IOException {
        mapRegionAndStartNext();
        fileChannel.close();
    }

    /**
     * Closes the file and unmaps all memory mapped regions. After calling this method, access to
     * any ByteBuffer obtained from this instance will cause a segmentation fault.
     */
    public void close() throws IOException {
        IOUtils.closeQuietly(fileChannel);

        for (ByteBuffer bb : memoryMappedRegions) {
            PlatformDependent.freeDirectBuffer(bb);
        }
        memoryMappedRegions.clear();

        // To make this compatible with all versions of Windows, we must wait with
        // deleting the file until it is unmapped.
        // See also
        // https://stackoverflow.com/questions/11099295/file-flag-delete-on-close-and-memory-mapped-files/51649618#51649618

        Files.delete(filePath);
    }

    @Override
    public long getSize() {
        return pos;
    }

    @Override
    public Path getFilePath() {
        return filePath;
    }

    private void mapRegionAndStartNext() throws IOException {
        final ByteBuffer region =
                fileChannel.map(
                        MapMode.READ_ONLY, startOfCurrentRegion, pos - startOfCurrentRegion);
        region.order(ByteOrder.nativeOrder());
        memoryMappedRegions.add(region);

        startOfCurrentRegion = pos;
        endOfCurrentRegion = startOfCurrentRegion + maxRegionSize;
    }

    private void throwTooLargeBuffer(Buffer buffer) throws IOException {
        throw new IOException(
                String.format(
                        "The buffer (%d bytes) is larger than the maximum size of a memory buffer (%d bytes)",
                        buffer.getSize(), maxRegionSize));
    }

    // ------------------------------------------------------------------------
    //  Factories
    // ------------------------------------------------------------------------

    /**
     * Creates new FileChannelMemoryMappedBoundedData, creating a memory mapped file at the given
     * path.
     */
    public static FileChannelMemoryMappedBoundedData create(Path memMappedFilePath)
            throws IOException {
        return createWithRegionSize(memMappedFilePath, Integer.MAX_VALUE);
    }

    /**
     * Creates new FileChannelMemoryMappedBoundedData, creating a memory mapped file at the given
     * path. Each mapped region (= ByteBuffer) will be of the given size.
     */
    public static FileChannelMemoryMappedBoundedData createWithRegionSize(
            Path memMappedFilePath, int regionSize) throws IOException {
        checkNotNull(memMappedFilePath, "memMappedFilePath");
        checkArgument(regionSize > 0, "regions size most be > 0");

        final FileChannel fileChannel =
                FileChannel.open(
                        memMappedFilePath,
                        StandardOpenOption.READ,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.CREATE_NEW);

        return new FileChannelMemoryMappedBoundedData(memMappedFilePath, fileChannel, regionSize);
    }
}
