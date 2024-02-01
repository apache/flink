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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FatalExitExceptionHandler;
import org.apache.flink.util.concurrent.FutureUtils;

import org.apache.flink.shaded.guava31.com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageUtils.generateBufferWithHeaders;
import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.SegmentPartitionFile.getSegmentPath;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The implementation of {@link PartitionFileWriter} with segment file mode. In this mode, each
 * segment of one subpartition is written to an independent file.
 *
 * <p>After finishing writing a segment, a segment-finish file is written to ensure the downstream
 * reads only when the entire segment file is written, avoiding partial data reads. The downstream
 * can determine if the current segment is complete by checking for the existence of the
 * segment-finish file.
 *
 * <p>To minimize the number of files, each subpartition keeps only a single segment-finish file.
 * For instance, if segment-finish file 5 exists, it indicates that segments 1 to 5 have all been
 * finished.
 */
public class SegmentPartitionFileWriter implements PartitionFileWriter {

    private final ExecutorService ioExecutor =
            Executors.newSingleThreadExecutor(
                    new ThreadFactoryBuilder()
                            .setNameFormat("Segment partition file flush thread")
                            .setUncaughtExceptionHandler(FatalExitExceptionHandler.INSTANCE)
                            .build());
    private final String basePath;

    private final WritableByteChannel[] subpartitionChannels;

    private volatile boolean isReleased;

    SegmentPartitionFileWriter(String basePath, int numSubpartitions) {
        this.basePath = basePath;
        this.subpartitionChannels = new WritableByteChannel[numSubpartitions];
        Arrays.fill(subpartitionChannels, null);
    }

    @Override
    public CompletableFuture<Void> write(
            TieredStoragePartitionId partitionId, List<SubpartitionBufferContext> buffersToWrite) {
        List<CompletableFuture<Void>> completableFutures = new ArrayList<>();
        buffersToWrite.forEach(
                subpartitionBuffers -> {
                    int subpartitionId = subpartitionBuffers.getSubpartitionId();
                    List<SegmentBufferContext> segmentBufferContexts =
                            subpartitionBuffers.getSegmentBufferContexts();
                    segmentBufferContexts.forEach(
                            segmentBufferContext -> {
                                CompletableFuture<Void> flushSuccessNotifier =
                                        new CompletableFuture<>();
                                ioExecutor.execute(
                                        () ->
                                                flushOrFinishSegment(
                                                        partitionId,
                                                        subpartitionId,
                                                        segmentBufferContext,
                                                        flushSuccessNotifier));
                                completableFutures.add(flushSuccessNotifier);
                            });
                });
        return FutureUtils.waitForAll(completableFutures);
    }

    @Override
    public void release() {
        if (isReleased) {
            return;
        }
        isReleased = true;
        try {
            ioExecutor.shutdown();
            if (!ioExecutor.awaitTermination(5L, TimeUnit.MINUTES)) {
                throw new TimeoutException("Timeout to shutdown the flush thread.");
            }
            for (WritableByteChannel writeChannel : subpartitionChannels) {
                if (writeChannel != null) {
                    writeChannel.close();
                }
            }
        } catch (Exception e) {
            ExceptionUtils.rethrow(e);
        }
    }

    // ------------------------------------------------------------------------
    //  Internal Methods
    // ------------------------------------------------------------------------

    private void flushOrFinishSegment(
            TieredStoragePartitionId partitionId,
            int subpartitionId,
            SegmentBufferContext segmentBufferContext,
            CompletableFuture<Void> flushSuccessNotifier) {
        int segmentId = segmentBufferContext.getSegmentId();
        List<Tuple2<Buffer, Integer>> buffersToFlush = segmentBufferContext.getBufferAndIndexes();
        boolean isSegmentFinished = segmentBufferContext.isSegmentFinished();
        checkState(!buffersToFlush.isEmpty() || isSegmentFinished);

        if (buffersToFlush.size() > 0) {
            flush(partitionId, subpartitionId, segmentId, buffersToFlush);
        }
        if (isSegmentFinished) {
            writeSegmentFinishFile(partitionId, subpartitionId, segmentId);
        }
        flushSuccessNotifier.complete(null);
    }

    /** This method is only called by the flushing thread. */
    private void flush(
            TieredStoragePartitionId partitionId,
            int subpartitionId,
            int segmentId,
            List<Tuple2<Buffer, Integer>> buffersToFlush) {
        try {
            writeBuffers(
                    partitionId,
                    subpartitionId,
                    segmentId,
                    buffersToFlush,
                    getTotalBytes(buffersToFlush));
            buffersToFlush.forEach(bufferToFlush -> bufferToFlush.f0.recycleBuffer());
        } catch (IOException exception) {
            ExceptionUtils.rethrow(exception);
        }
    }

    /**
     * Writing a segment-finish file when the current segment is complete. The downstream can
     * determine if the current segment is complete by checking for the existence of the
     * segment-finish file.
     *
     * <p>Note that the method is only called by the flushing thread.
     */
    private void writeSegmentFinishFile(
            TieredStoragePartitionId partitionId, int subpartitionId, int segmentId) {
        try {
            WritableByteChannel channel = subpartitionChannels[subpartitionId];
            if (channel != null) {
                channel.close();
                subpartitionChannels[subpartitionId] = null;
            }
            SegmentPartitionFile.writeSegmentFinishFile(
                    basePath, partitionId, subpartitionId, segmentId);
        } catch (IOException exception) {
            ExceptionUtils.rethrow(exception);
        }
    }

    private long getTotalBytes(List<Tuple2<Buffer, Integer>> buffersToFlush) {
        long expectedBytes = 0;
        for (Tuple2<Buffer, Integer> bufferToFlush : buffersToFlush) {
            Buffer buffer = bufferToFlush.f0;
            int numBytes = buffer.readableBytes() + BufferReaderWriterUtil.HEADER_LENGTH;
            expectedBytes += numBytes;
        }
        return expectedBytes;
    }

    private void writeBuffers(
            TieredStoragePartitionId partitionId,
            int subpartitionId,
            int segmentId,
            List<Tuple2<Buffer, Integer>> buffersToFlush,
            long expectedBytes)
            throws IOException {
        WritableByteChannel currentChannel =
                getOrInitSubpartitionChannel(partitionId, subpartitionId, segmentId);
        SegmentPartitionFile.writeBuffers(
                currentChannel, expectedBytes, generateBufferWithHeaders(buffersToFlush));
    }

    private WritableByteChannel getOrInitSubpartitionChannel(
            TieredStoragePartitionId partitionId, int subpartitionId, int segmentId)
            throws IOException {
        WritableByteChannel currentChannel = subpartitionChannels[subpartitionId];
        if (currentChannel == null) {
            Path writingSegmentPath =
                    getSegmentPath(basePath, partitionId, subpartitionId, segmentId);
            FileSystem fs = writingSegmentPath.getFileSystem();
            currentChannel =
                    Channels.newChannel(
                            fs.create(writingSegmentPath, FileSystem.WriteMode.NO_OVERWRITE));
            subpartitionChannels[subpartitionId] = currentChannel;
        }
        return currentChannel;
    }
}
