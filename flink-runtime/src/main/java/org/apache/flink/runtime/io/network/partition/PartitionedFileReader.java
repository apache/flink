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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferHeader;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.CompositeBuffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.function.Consumer;

import static org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil.HEADER_LENGTH;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Reader which can read all data of the target subpartition from a {@link PartitionedFile}. */
class PartitionedFileReader {

    /** Used to read buffer headers from file channel. */
    private final ByteBuffer headerBuf;

    /** Used to read index entry from index file. */
    private final ByteBuffer indexEntryBuf;

    /** Target {@link PartitionedFile} to read. */
    private final PartitionedFile partitionedFile;

    /** Target subpartitions to read. */
    private final ResultSubpartitionIndexSet subpartitionIndexSet;

    /** Data file channel of the target {@link PartitionedFile}. */
    private final FileChannel dataFileChannel;

    /** Index file channel of the target {@link PartitionedFile}. */
    private final FileChannel indexFileChannel;

    /**
     * Records the shift position of the subpartition write order. For example, if the write order
     * of subpartitions is [4, 5, 0, 1, 2, 3], then this value would be 4.
     */
    private final int subpartitionOrderRotationIndex;

    /** Next data region to be read. */
    private int nextRegionToRead;

    /** Next file offset to be read. */
    private long nextOffsetToRead;

    /** Number of remaining bytes in the current data region read. */
    private long currentRegionRemainingBytes;

    /** A queue storing pairs of file offsets and sizes to be read. */
    private final Queue<Tuple2<Long, Long>> offsetAndSizesToRead = new ArrayDeque<>();

    PartitionedFileReader(
            PartitionedFile partitionedFile,
            ResultSubpartitionIndexSet subpartitionIndexSet,
            FileChannel dataFileChannel,
            FileChannel indexFileChannel,
            ByteBuffer headerBuffer,
            ByteBuffer indexEntryBuffer,
            int subpartitionOrderRotationIndex) {
        checkArgument(checkNotNull(dataFileChannel).isOpen(), "Data file channel must be opened.");
        checkArgument(
                checkNotNull(indexFileChannel).isOpen(), "Index file channel must be opened.");

        this.partitionedFile = checkNotNull(partitionedFile);
        this.subpartitionIndexSet = subpartitionIndexSet;
        this.dataFileChannel = dataFileChannel;
        this.indexFileChannel = indexFileChannel;
        this.headerBuf = headerBuffer;
        this.indexEntryBuf = indexEntryBuffer;
        this.subpartitionOrderRotationIndex = subpartitionOrderRotationIndex;
    }

    private void moveToNextReadablePosition(ByteBuffer indexEntryBuf) throws IOException {
        while (currentRegionRemainingBytes <= 0 && hasNextPositionToRead()) {
            if (!offsetAndSizesToRead.isEmpty()) {
                Tuple2<Long, Long> offsetAndSize = offsetAndSizesToRead.poll();
                nextOffsetToRead = offsetAndSize.f0;
                currentRegionRemainingBytes = offsetAndSize.f1;
            } else {
                // move to next region which has buffers
                if (nextRegionToRead < partitionedFile.getNumRegions()) {
                    updateReadableOffsetAndSize(indexEntryBuf, offsetAndSizesToRead);
                    ++nextRegionToRead;
                }
            }
        }
    }

    private boolean hasNextPositionToRead() {
        return !offsetAndSizesToRead.isEmpty()
                || nextRegionToRead < partitionedFile.getNumRegions();
    }

    /**
     * Updates the readable offsets and sizes for subpartitions based on a given index buffer. This
     * method handles cases where the subpartition range is split by a rotation index, ensuring that
     * all necessary index entries are processed.
     *
     * <p>The method operates in the following way:
     *
     * <ol>
     *   <li>It checks if the range of subpartition indices requires handling of a wrap around the
     *       rotation index.
     *   <li>If no wrap is necessary (when the range does not cross the rotation point), it directly
     *       updates readable offsets and sizes for the entire range.
     *   <li>If a wrap is necessary, it splits the process into two updates:
     *       <ul>
     *         <li>Firstly, it updates from the rotation index to the end subpartition.
     *         <li>Secondly, it updates from the start subpartition to just before the rotation
     *             index.
     *       </ul>
     * </ol>
     *
     * <p>This ensures that all relevant subpartitions are correctly processed and offsets and sizes
     * are added to the queue for subsequent reading.
     *
     * @param indexEntryBuf A ByteBuffer containing index entries which provide offset and size
     *     information.
     * @param offsetAndSizesToRead A queue to store the updated offsets and sizes.
     * @throws IOException If an I/O error occurs when accessing the index file channel.
     */
    @VisibleForTesting
    void updateReadableOffsetAndSize(
            ByteBuffer indexEntryBuf, Queue<Tuple2<Long, Long>> offsetAndSizesToRead)
            throws IOException {
        int startSubpartition = subpartitionIndexSet.getStartIndex();
        int endSubpartition = subpartitionIndexSet.getEndIndex();

        if (startSubpartition >= subpartitionOrderRotationIndex
                || endSubpartition < subpartitionOrderRotationIndex) {
            updateReadableOffsetAndSize(
                    startSubpartition, endSubpartition, indexEntryBuf, offsetAndSizesToRead);
        } else {
            updateReadableOffsetAndSize(
                    subpartitionOrderRotationIndex,
                    endSubpartition,
                    indexEntryBuf,
                    offsetAndSizesToRead);
            updateReadableOffsetAndSize(
                    startSubpartition,
                    subpartitionOrderRotationIndex - 1,
                    indexEntryBuf,
                    offsetAndSizesToRead);
        }
    }

    /**
     * Updates the readable offsets and sizes for a specified range of subpartitions. If offsets are
     * contiguous, they are merged into a single entry. If not contiguous, each subpartition's
     * offset and size must come from the same buffer, and individual tuples are added for each
     * entry.
     *
     * @param startSubpartition The starting index of the subpartition range to be processed.
     * @param endSubpartition The ending index of the subpartition range to be processed.
     * @param indexEntryBuf A ByteBuffer containing the index entries to read offsets and sizes.
     * @param offsetAndSizesToRead A queue to store the updated offsets and sizes.
     * @throws IOException If an I/O error occurs during reading of index entries.
     * @throws IllegalStateException If offsets are not contiguous and not from a single buffer.
     */
    private void updateReadableOffsetAndSize(
            int startSubpartition,
            int endSubpartition,
            ByteBuffer indexEntryBuf,
            Queue<Tuple2<Long, Long>> offsetAndSizesToRead)
            throws IOException {
        partitionedFile.getIndexEntry(
                indexFileChannel, indexEntryBuf, nextRegionToRead, startSubpartition);
        long startPartitionOffset = indexEntryBuf.getLong();
        long startPartitionSize = indexEntryBuf.getLong();

        partitionedFile.getIndexEntry(
                indexFileChannel, indexEntryBuf, nextRegionToRead, endSubpartition);
        long endPartitionOffset = indexEntryBuf.getLong();
        long endPartitionSize = indexEntryBuf.getLong();

        if (startPartitionOffset != endPartitionOffset || startPartitionSize != endPartitionSize) {
            offsetAndSizesToRead.add(
                    Tuple2.of(
                            startPartitionOffset,
                            endPartitionOffset + endPartitionSize - startPartitionOffset));
        } else if (startPartitionSize != 0) {
            // this branch is for broadcast subpartitions
            for (int i = startSubpartition; i <= endSubpartition; i++) {
                offsetAndSizesToRead.add(Tuple2.of(startPartitionOffset, startPartitionSize));
            }
        }
    }

    /**
     * Reads a buffer from the current region of the target {@link PartitionedFile} and moves the
     * read position forward.
     *
     * <p>Note: The caller is responsible for recycling the target buffer if any exception occurs.
     *
     * @param freeSegments The free {@link MemorySegment}s to read data to.
     * @param recycler The {@link BufferRecycler} which is responsible to recycle the target buffer.
     * @param consumer The target {@link Buffer} stores the data read from file channel.
     * @return Whether the file reader has remaining data to read.
     */
    boolean readCurrentRegion(
            Queue<MemorySegment> freeSegments, BufferRecycler recycler, Consumer<Buffer> consumer)
            throws IOException {
        if (currentRegionRemainingBytes == 0) {
            return false;
        }

        checkArgument(!freeSegments.isEmpty(), "No buffer available for data reading.");
        dataFileChannel.position(nextOffsetToRead);

        BufferAndHeader partialBuffer = new BufferAndHeader(null, null);
        try {
            while (!freeSegments.isEmpty() && currentRegionRemainingBytes > 0) {
                MemorySegment segment = freeSegments.poll();
                int numBytes = (int) Math.min(segment.size(), currentRegionRemainingBytes);
                ByteBuffer byteBuffer = segment.wrap(0, numBytes);

                try {
                    BufferReaderWriterUtil.readByteBufferFully(dataFileChannel, byteBuffer);
                    byteBuffer.flip();
                    currentRegionRemainingBytes -= byteBuffer.remaining();
                    nextOffsetToRead += byteBuffer.remaining();
                } catch (Throwable throwable) {
                    freeSegments.add(segment);
                    throw throwable;
                }

                NetworkBuffer buffer = new NetworkBuffer(segment, recycler);
                buffer.setSize(byteBuffer.remaining());
                try {
                    partialBuffer = processBuffer(byteBuffer, buffer, partialBuffer, consumer);
                } catch (Throwable throwable) {
                    partialBuffer = new BufferAndHeader(null, null);
                    throw throwable;
                } finally {
                    buffer.recycleBuffer();
                }
            }
        } finally {
            if (headerBuf.position() > 0) {
                nextOffsetToRead -= headerBuf.position();
                currentRegionRemainingBytes += headerBuf.position();
                headerBuf.clear();
            }
            if (partialBuffer.header != null) {
                nextOffsetToRead -= HEADER_LENGTH;
                currentRegionRemainingBytes += HEADER_LENGTH;
            }
            if (partialBuffer.buffer != null) {
                nextOffsetToRead -= partialBuffer.buffer.readableBytes();
                currentRegionRemainingBytes += partialBuffer.buffer.readableBytes();
                partialBuffer.buffer.recycleBuffer();
            }
        }
        return hasRemaining();
    }

    boolean hasRemaining() throws IOException {
        moveToNextReadablePosition(indexEntryBuf);
        return currentRegionRemainingBytes > 0;
    }

    void initRegionIndex(ByteBuffer initIndexEntryBuffer) throws IOException {
        moveToNextReadablePosition(initIndexEntryBuffer);
    }

    /** Gets read priority of this file reader. Smaller value indicates higher priority. */
    long getPriority() {
        return nextOffsetToRead;
    }

    private BufferAndHeader processBuffer(
            ByteBuffer byteBuffer,
            Buffer buffer,
            BufferAndHeader partialBuffer,
            Consumer<Buffer> consumer) {
        BufferHeader header = partialBuffer.header;
        CompositeBuffer targetBuffer = partialBuffer.buffer;
        while (byteBuffer.hasRemaining()) {
            if (header == null && (header = parseBufferHeader(byteBuffer)) == null) {
                break;
            }

            if (targetBuffer != null) {
                buffer.retainBuffer();
                int position = byteBuffer.position() + targetBuffer.missingLength();
                targetBuffer.addPartialBuffer(
                        buffer.readOnlySlice(byteBuffer.position(), targetBuffer.missingLength()));
                byteBuffer.position(position);
            } else if (byteBuffer.remaining() < header.getLength()) {
                if (byteBuffer.hasRemaining()) {
                    buffer.retainBuffer();
                    targetBuffer = new CompositeBuffer(header);
                    targetBuffer.addPartialBuffer(
                            buffer.readOnlySlice(byteBuffer.position(), byteBuffer.remaining()));
                }
                break;
            } else {
                buffer.retainBuffer();
                targetBuffer = new CompositeBuffer(header);
                targetBuffer.addPartialBuffer(
                        buffer.readOnlySlice(byteBuffer.position(), header.getLength()));
                byteBuffer.position(byteBuffer.position() + header.getLength());
            }

            header = null;
            consumer.accept(targetBuffer);
            targetBuffer = null;
        }
        return new BufferAndHeader(targetBuffer, header);
    }

    private BufferHeader parseBufferHeader(ByteBuffer buffer) {
        BufferHeader header = null;
        if (headerBuf.position() > 0) {
            while (headerBuf.hasRemaining()) {
                headerBuf.put(buffer.get());
            }
            headerBuf.flip();
            header = BufferReaderWriterUtil.parseBufferHeader(headerBuf);
            headerBuf.clear();
        }

        if (header == null && buffer.remaining() < HEADER_LENGTH) {
            headerBuf.put(buffer);
        } else if (header == null) {
            header = BufferReaderWriterUtil.parseBufferHeader(buffer);
        }
        return header;
    }

    private static class BufferAndHeader {

        private final CompositeBuffer buffer;
        private final BufferHeader header;

        BufferAndHeader(CompositeBuffer buffer, BufferHeader header) {
            this.buffer = buffer;
            this.header = header;
        }
    }
}
