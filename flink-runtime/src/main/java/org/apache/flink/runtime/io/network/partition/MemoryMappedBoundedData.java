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

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * An implementation of {@link BoundedData} simply through ByteBuffers backed by memory, typically
 * from a memory mapped file, so the data gets automatically evicted from memory if it grows large.
 *
 * <p>Most of the code in this class is a workaround for the fact that a memory mapped region in
 * Java cannot be larger than 2GB (== signed 32 bit int max value). The class takes {@link Buffer
 * Buffers} and writes them to several memory mapped region, using the {@link
 * BufferReaderWriterUtil} class.
 *
 * <h2>Important!</h2>
 *
 * <p>This class performs absolutely no synchronization and relies on single threaded access or
 * externally synchronized access. Concurrent access around disposal may cause segmentation faults!
 *
 * <p>This class does limited sanity checks and assumes correct use from {@link
 * BoundedBlockingSubpartition} and {@link BoundedBlockingSubpartitionReader}, such as writing first
 * and reading after. Not obeying these contracts throws NullPointerExceptions.
 */
final class MemoryMappedBoundedData implements BoundedData {

    /** Memory mappings should be at the granularity of page sizes, for efficiency. */
    private static final int PAGE_SIZE = PageSizeUtil.getSystemPageSizeOrConservativeMultiple();

    /**
     * The the current memory mapped region we are writing to. This value is null once writing has
     * finished or the buffers are disposed.
     */
    @Nullable private ByteBuffer currentBuffer;

    /** All memory mapped regions that are already full (completed). */
    private final ArrayList<ByteBuffer> fullBuffers;

    /** The file channel backing the memory mapped file. */
    private final FileChannel file;

    /** The path of the memory mapped file. */
    private final Path filePath;

    /** The offset where the next mapped region should start. */
    private long nextMappingOffset;

    /** The size of each mapped region. */
    private final long mappingSize;

    MemoryMappedBoundedData(Path filePath, FileChannel fileChannel, int maxSizePerByteBuffer)
            throws IOException {

        this.filePath = filePath;
        this.file = fileChannel;
        this.mappingSize = alignSize(maxSizePerByteBuffer);
        this.fullBuffers = new ArrayList<>(4);

        rollOverToNextBuffer();
    }

    @Override
    public void writeBuffer(Buffer buffer) throws IOException {
        assert currentBuffer != null;

        if (BufferReaderWriterUtil.writeBuffer(buffer, currentBuffer)) {
            return;
        }

        rollOverToNextBuffer();

        if (!BufferReaderWriterUtil.writeBuffer(buffer, currentBuffer)) {
            throwTooLargeBuffer(buffer);
        }
    }

    @Override
    public BufferSlicer createReader(ResultSubpartitionView ignored) {
        assert currentBuffer == null;

        final List<ByteBuffer> buffers =
                fullBuffers.stream()
                        .map((bb) -> bb.slice().order(ByteOrder.nativeOrder()))
                        .collect(Collectors.toList());

        return new BufferSlicer(buffers);
    }

    /**
     * Finishes the current region and prevents further writes. After calling this method, further
     * calls to {@link #writeBuffer(Buffer)} will fail.
     */
    @Override
    public void finishWrite() throws IOException {
        assert currentBuffer != null;

        currentBuffer.flip();
        fullBuffers.add(currentBuffer);
        currentBuffer = null; // fail further writes fast
        file.close(); // won't map further regions from now on
    }

    /**
     * Unmaps the file from memory and deletes the file. After calling this method, access to any
     * ByteBuffer obtained from this instance will cause a segmentation fault.
     */
    public void close() throws IOException {
        IOUtils.closeQuietly(file); // in case we dispose before finishing writes

        for (ByteBuffer bb : fullBuffers) {
            PlatformDependent.freeDirectBuffer(bb);
        }
        fullBuffers.clear();

        if (currentBuffer != null) {
            PlatformDependent.freeDirectBuffer(currentBuffer);
            currentBuffer = null;
        }

        // To make this compatible with all versions of Windows, we must wait with
        // deleting the file until it is unmapped.
        // See also
        // https://stackoverflow.com/questions/11099295/file-flag-delete-on-close-and-memory-mapped-files/51649618#51649618

        Files.delete(filePath);
    }

    /**
     * Gets the number of bytes of all written data (including the metadata in the buffer headers).
     */
    @Override
    public long getSize() {
        long size = 0L;
        for (ByteBuffer bb : fullBuffers) {
            size += bb.remaining();
        }
        if (currentBuffer != null) {
            size += currentBuffer.position();
        }
        return size;
    }

    @Override
    public Path getFilePath() {
        return filePath;
    }

    private void rollOverToNextBuffer() throws IOException {
        if (currentBuffer != null) {
            // we need to remember the original buffers, not any slices.
            // slices have no cleaner, which we need to trigger explicit unmapping
            currentBuffer.flip();
            fullBuffers.add(currentBuffer);
        }

        currentBuffer = file.map(MapMode.READ_WRITE, nextMappingOffset, mappingSize);
        currentBuffer.order(ByteOrder.nativeOrder());
        nextMappingOffset += mappingSize;
    }

    private void throwTooLargeBuffer(Buffer buffer) throws IOException {
        throw new IOException(
                String.format(
                        "The buffer (%d bytes) is larger than the maximum size of a memory buffer (%d bytes)",
                        buffer.getSize(), mappingSize));
    }

    /**
     * Rounds the size down to the next multiple of the {@link #PAGE_SIZE}. We need to round down
     * here to not exceed the original maximum size value. Otherwise, values like INT_MAX would
     * round up to overflow the valid maximum size of a memory mapping region in Java.
     */
    private static int alignSize(int maxRegionSize) {
        checkArgument(maxRegionSize >= PAGE_SIZE);
        return maxRegionSize - (maxRegionSize % PAGE_SIZE);
    }

    // ------------------------------------------------------------------------
    //  Reader
    // ------------------------------------------------------------------------

    /**
     * The "reader" for the memory region. It slices a sequence of buffers from the sequence of
     * mapped ByteBuffers.
     */
    static final class BufferSlicer implements BoundedData.Reader {

        /**
         * The memory mapped region we currently read from. Max 2GB large. Further regions may be in
         * the {@link #furtherData} field.
         */
        private ByteBuffer currentData;

        /**
         * Further byte buffers, to handle cases where there is more data than fits into one mapped
         * byte buffer (2GB = Integer.MAX_VALUE).
         */
        private final Iterator<ByteBuffer> furtherData;

        BufferSlicer(Iterable<ByteBuffer> data) {
            this.furtherData = data.iterator();
            this.currentData = furtherData.next();
        }

        @Override
        @Nullable
        public Buffer nextBuffer() {
            // should only be null once empty or disposed, in which case this method
            // should not be called any more
            assert currentData != null;

            final Buffer next = BufferReaderWriterUtil.sliceNextBuffer(currentData);
            if (next != null) {
                return next;
            }

            if (!furtherData.hasNext()) {
                return null;
            }

            currentData = furtherData.next();
            return nextBuffer();
        }

        @Override
        public void close() throws IOException {
            // nothing to do, this class holds no actual resources of its own,
            // only references to the mapped byte buffers
        }
    }

    // ------------------------------------------------------------------------
    //  Factories
    // ------------------------------------------------------------------------

    /** Creates new MemoryMappedBoundedData, creating a memory mapped file at the given path. */
    public static MemoryMappedBoundedData create(Path memMappedFilePath) throws IOException {
        return createWithRegionSize(memMappedFilePath, Integer.MAX_VALUE);
    }

    /**
     * Creates new MemoryMappedBoundedData, creating a memory mapped file at the given path. Each
     * mapped region (= ByteBuffer) will be of the given size.
     */
    public static MemoryMappedBoundedData createWithRegionSize(
            Path memMappedFilePath, int regionSize) throws IOException {
        final FileChannel fileChannel =
                FileChannel.open(
                        memMappedFilePath,
                        StandardOpenOption.READ,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.CREATE_NEW);

        return new MemoryMappedBoundedData(memMappedFilePath, fileChannel, regionSize);
    }
}
