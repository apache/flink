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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.file;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferHeader;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.CompositeBuffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.buffer.ReadOnlySlicedNetworkBuffer;
import org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil.HEADER_LENGTH;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The implementation of {@link PartitionFileReader} with producer-merge mode. In this mode, the
 * shuffle data is written in the producer side, the consumer side need to read multiple producers
 * to get its partition data.
 *
 * <p>Note that one partition file may contain the data of multiple subpartitions.
 */
public class ProducerMergedPartitionFileReader implements PartitionFileReader {

    private static final Logger LOG =
            LoggerFactory.getLogger(ProducerMergedPartitionFileReader.class);

    private final ByteBuffer reusedHeaderBuffer = BufferReaderWriterUtil.allocatedHeaderBuffer();

    private final Path dataFilePath;

    private final ProducerMergedPartitionFileIndex dataIndex;

    private volatile FileChannel fileChannel;

    @VisibleForTesting
    ProducerMergedPartitionFileReader(
            Path dataFilePath, ProducerMergedPartitionFileIndex dataIndex) {
        this.dataFilePath = dataFilePath;
        this.dataIndex = dataIndex;
    }

    @Override
    public ReadBufferResult readBuffer(
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId,
            int segmentId,
            int bufferIndex,
            MemorySegment memorySegment,
            BufferRecycler recycler,
            @Nullable ReadProgress readProgress,
            @Nullable CompositeBuffer partialBuffer)
            throws IOException {

        lazyInitializeFileChannel();

        // Get the read offset, including the start offset, the end offset
        Tuple2<Long, Long> startAndEndOffset =
                getReadStartAndEndOffset(subpartitionId, bufferIndex, readProgress, partialBuffer);
        if (startAndEndOffset == null) {
            return null;
        }
        long readStartOffset = startAndEndOffset.f0;
        long readEndOffset = startAndEndOffset.f1;

        int numBytesToRead =
                Math.min(memorySegment.size(), (int) (readEndOffset - readStartOffset));

        if (numBytesToRead == 0) {
            return null;
        }

        List<Buffer> readBuffers = new LinkedList<>();
        ByteBuffer byteBuffer = memorySegment.wrap(0, numBytesToRead);
        fileChannel.position(readStartOffset);
        // Read data to the memory segment, note the read size is numBytesToRead
        readFileDataToBuffer(memorySegment, recycler, byteBuffer);

        // Slice the read memory segment to multiple small network buffers and add them to
        // readBuffers
        Tuple2<Integer, Integer> partial =
                sliceBuffer(byteBuffer, memorySegment, partialBuffer, recycler, readBuffers);

        return getReadBufferResult(
                readBuffers,
                readStartOffset,
                readEndOffset,
                numBytesToRead,
                partial.f0,
                partial.f1);
    }

    @Override
    public long getPriority(
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId,
            int segmentId,
            int bufferIndex,
            @Nullable ReadProgress readProgress) {
        lazyInitializeFileChannel();

        ProducerMergedReadProgress progress = convertToCurrentReadProgress(readProgress);
        if (progress != null
                && progress.getCurrentBufferOffset() != progress.getEndOfRegionOffset()) {
            return progress.getCurrentBufferOffset();
        }
        return dataIndex
                .getRegion(subpartitionId, bufferIndex)
                .map(ProducerMergedPartitionFileIndex.FixedSizeRegion::getRegionStartOffset)
                .orElse(Long.MAX_VALUE);
    }

    @Override
    public void release() {
        if (fileChannel != null) {
            try {
                fileChannel.close();
            } catch (IOException e) {
                ExceptionUtils.rethrow(e, "Failed to close file channel.");
            }
        }
        IOUtils.deleteFileQuietly(dataFilePath);
    }

    /**
     * Initialize the file channel in a lazy manner, which can reduce usage of the file descriptor
     * resource.
     */
    private void lazyInitializeFileChannel() {
        if (fileChannel == null) {
            try {
                fileChannel = FileChannel.open(dataFilePath, StandardOpenOption.READ);
            } catch (IOException e) {
                ExceptionUtils.rethrow(e, "Failed to open file channel.");
            }
        }
    }

    /**
     * Slice the read memory segment to multiple small network buffers.
     *
     * <p>Note that although the method appears to be split into multiple buffers, the sliced
     * buffers still share the same one actual underlying memory segment.
     *
     * @param byteBuffer the byte buffer to be sliced, it points to the underlying memorySegment
     * @param memorySegment the underlying memory segment to be sliced
     * @param partialBuffer the partial buffer, if the partial buffer is not null, it contains the
     *     partial data buffer from the previous read
     * @param readBuffers the read buffers list is to accept the sliced buffers
     * @return the first field is the number of total sliced bytes, the second field is the bytes of
     *     the partial buffer
     */
    private Tuple2<Integer, Integer> sliceBuffer(
            ByteBuffer byteBuffer,
            MemorySegment memorySegment,
            @Nullable CompositeBuffer partialBuffer,
            BufferRecycler bufferRecycler,
            List<Buffer> readBuffers) {
        checkState(reusedHeaderBuffer.position() == 0);
        checkState(partialBuffer == null || partialBuffer.missingLength() > 0);

        NetworkBuffer buffer = new NetworkBuffer(memorySegment, bufferRecycler);
        buffer.setSize(byteBuffer.remaining());

        try {
            int numSlicedBytes = 0;
            if (partialBuffer != null) {
                // If there is a previous small partial buffer, the current read operation should
                // read additional data and combine it with the existing partial to construct a new
                // complete buffer
                buffer.retainBuffer();
                int position = byteBuffer.position() + partialBuffer.missingLength();
                int numPartialBytes = partialBuffer.missingLength();
                partialBuffer.addPartialBuffer(
                        buffer.readOnlySlice(byteBuffer.position(), numPartialBytes));
                numSlicedBytes += numPartialBytes;
                byteBuffer.position(position);
                readBuffers.add(partialBuffer);
            }

            partialBuffer = null;
            while (byteBuffer.hasRemaining()) {
                // Parse the small buffer's header
                BufferHeader header = parseBufferHeader(byteBuffer);
                if (header == null) {
                    // If the remaining data length in the buffer is not enough to construct a new
                    // complete buffer header, drop it directly.
                    break;
                } else {
                    numSlicedBytes += HEADER_LENGTH;
                }

                if (header.getLength() <= byteBuffer.remaining()) {
                    // The remaining data length in the buffer is enough to generate a new small
                    // sliced network buffer. The small sliced buffer is not a partial buffer, we
                    // should read the slice of the buffer directly
                    buffer.retainBuffer();
                    ReadOnlySlicedNetworkBuffer slicedBuffer =
                            buffer.readOnlySlice(byteBuffer.position(), header.getLength());
                    slicedBuffer.setDataType(header.getDataType());
                    slicedBuffer.setCompressed(header.isCompressed());
                    byteBuffer.position(byteBuffer.position() + header.getLength());
                    numSlicedBytes += header.getLength();
                    readBuffers.add(slicedBuffer);
                } else {
                    // The remaining data length in the buffer is smaller than the actual length of
                    // the buffer, so we should generate a new partial buffer, allowing for
                    // generating a new complete buffer during the next read operation
                    buffer.retainBuffer();
                    int numPartialBytes = byteBuffer.remaining();
                    numSlicedBytes += numPartialBytes;
                    partialBuffer = new CompositeBuffer(header);
                    partialBuffer.addPartialBuffer(
                            buffer.readOnlySlice(byteBuffer.position(), numPartialBytes));
                    readBuffers.add(partialBuffer);
                    break;
                }
            }
            return Tuple2.of(numSlicedBytes, getPartialBufferReadBytes(partialBuffer));
        } catch (Throwable throwable) {
            LOG.error("Failed to slice the read buffer {}.", byteBuffer, throwable);
            throw throwable;
        } finally {
            buffer.recycleBuffer();
        }
    }

    /**
     * Return a tuple of the start and end file offset, or return null if the buffer is not found in
     * the data index.
     */
    @Nullable
    private Tuple2<Long, Long> getReadStartAndEndOffset(
            TieredStorageSubpartitionId subpartitionId,
            int bufferIndex,
            @Nullable ReadProgress currentReadProgress,
            @Nullable CompositeBuffer partialBuffer) {
        ProducerMergedReadProgress readProgress = convertToCurrentReadProgress(currentReadProgress);
        long readStartOffset;
        long readEndOffset;
        if (readProgress == null
                || readProgress.getCurrentBufferOffset() == readProgress.getEndOfRegionOffset()) {
            Optional<ProducerMergedPartitionFileIndex.FixedSizeRegion> regionOpt =
                    dataIndex.getRegion(subpartitionId, bufferIndex);
            if (!regionOpt.isPresent()) {
                return null;
            }
            readStartOffset = regionOpt.get().getRegionStartOffset();
            readEndOffset = regionOpt.get().getRegionEndOffset();
        } else {
            readStartOffset =
                    readProgress.getCurrentBufferOffset()
                            + getPartialBufferReadBytes(partialBuffer);
            readEndOffset = readProgress.getEndOfRegionOffset();
        }

        checkState(readStartOffset <= readEndOffset);
        return Tuple2.of(readStartOffset, readEndOffset);
    }

    private static ReadBufferResult getReadBufferResult(
            List<Buffer> readBuffers,
            long readStartOffset,
            long readEndOffset,
            int numBytesToRead,
            int numBytesRealRead,
            int numBytesReadPartialBuffer) {
        boolean shouldContinueRead = readStartOffset + numBytesRealRead < readEndOffset;
        ProducerMergedReadProgress readProgress =
                new ProducerMergedReadProgress(
                        readStartOffset + numBytesRealRead - numBytesReadPartialBuffer,
                        readEndOffset);
        checkState(
                numBytesRealRead <= numBytesToRead
                        && numBytesToRead - numBytesRealRead < HEADER_LENGTH);

        return new ReadBufferResult(readBuffers, shouldContinueRead, readProgress);
    }

    private void readFileDataToBuffer(
            MemorySegment memorySegment, BufferRecycler recycler, ByteBuffer byteBuffer)
            throws IOException {
        try {
            BufferReaderWriterUtil.readByteBufferFully(fileChannel, byteBuffer);
            byteBuffer.flip();
        } catch (Throwable throwable) {
            recycler.recycle(memorySegment);
            throw throwable;
        }
    }

    private static int getPartialBufferReadBytes(@Nullable CompositeBuffer partialBuffer) {
        return partialBuffer == null ? 0 : partialBuffer.readableBytes() + HEADER_LENGTH;
    }

    private static ProducerMergedReadProgress convertToCurrentReadProgress(
            @Nullable ReadProgress readProgress) {
        if (readProgress == null) {
            return null;
        }
        checkState(readProgress instanceof ProducerMergedReadProgress);
        return (ProducerMergedReadProgress) readProgress;
    }

    private BufferHeader parseBufferHeader(ByteBuffer buffer) {
        checkArgument(reusedHeaderBuffer.position() == 0);

        BufferHeader header = null;
        try {
            if (buffer.remaining() >= HEADER_LENGTH) {
                // The remaining data length in the buffer is enough to construct a new complete
                // buffer, parse and create a new buffer header
                header = BufferReaderWriterUtil.parseBufferHeader(buffer);
            }
            // If the remaining data length in the buffer is smaller than the header. Drop it
            // directly
        } catch (Throwable throwable) {
            reusedHeaderBuffer.clear();
            LOG.error("Failed to parse buffer header.", throwable);
            throw throwable;
        }
        reusedHeaderBuffer.clear();
        return header;
    }

    /**
     * The implementation of {@link PartitionFileReader.ReadProgress} mainly includes current
     * reading offset, end of read offset, etc.
     */
    public static class ProducerMergedReadProgress implements PartitionFileReader.ReadProgress {
        /**
         * The current reading buffer file offset. Note the offset does not contain the length of
         * the partial buffer, because the partial buffer may be dropped at anytime.
         */
        private final long currentBufferOffset;

        /** The end of region file offset. */
        private final long endOfRegionOffset;

        public ProducerMergedReadProgress(long currentBufferOffset, long endOfRegionOffset) {
            this.currentBufferOffset = currentBufferOffset;
            this.endOfRegionOffset = endOfRegionOffset;
        }

        public long getCurrentBufferOffset() {
            return currentBufferOffset;
        }

        public long getEndOfRegionOffset() {
            return endOfRegionOffset;
        }
    }
}
