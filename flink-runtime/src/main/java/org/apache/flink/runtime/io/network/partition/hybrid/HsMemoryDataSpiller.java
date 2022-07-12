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

package org.apache.flink.runtime.io.network.partition.hybrid;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil;
import org.apache.flink.runtime.io.network.partition.hybrid.HsFileDataIndex.SpilledBuffer;

import org.apache.flink.shaded.guava30.com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * This component is responsible for asynchronously writing in-memory data to disk. Each spilling
 * operation will write the disk file sequentially.
 */
public class HsMemoryDataSpiller implements AutoCloseable {
    /** One thread to perform spill operation. */
    private final ExecutorService ioExecutor =
            Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryBuilder().setNameFormat("hybrid spiller thread").build());

    /** File channel to write data. */
    private final FileChannel dataFileChannel;

    /** Records the current writing location. */
    private long totalBytesWritten;

    public HsMemoryDataSpiller(FileChannel dataFileChannel) {
        this.dataFileChannel = dataFileChannel;
    }

    /**
     * Spilling buffers to disk asynchronously.
     *
     * @param bufferToSpill buffers need to be spilled, must ensure that it is sorted by
     *     (subpartitionId, bufferIndex).
     * @return the completable future contains spilled buffers information.
     */
    public CompletableFuture<List<SpilledBuffer>> spillAsync(
            List<BufferWithIdentity> bufferToSpill) {
        CompletableFuture<List<SpilledBuffer>> spilledFuture = new CompletableFuture<>();
        ioExecutor.execute(() -> spill(bufferToSpill, spilledFuture));
        return spilledFuture;
    }

    /** Called in single-threaded ioExecutor. Order is guaranteed. */
    private void spill(
            List<BufferWithIdentity> toWrite,
            CompletableFuture<List<SpilledBuffer>> spilledFuture) {
        try {
            List<SpilledBuffer> spilledBuffers = new ArrayList<>();
            long expectedBytes = createSpilledBuffersAndGetTotalBytes(toWrite, spilledBuffers);
            // write all buffers to file
            writeBuffers(toWrite, expectedBytes);
            // complete spill future when buffers are written to disk successfully.
            // note that the ownership of these buffers is transferred to the MemoryDataManager,
            // which controls data's life cycle.
            // TODO update file data index and handle buffers release in future ticket.
            spilledFuture.complete(spilledBuffers);
        } catch (Throwable t) {
            spilledFuture.completeExceptionally(t);
        }
    }

    /**
     * Compute buffer's file offset and create spilled buffers.
     *
     * @param toWrite for create {@link SpilledBuffer}.
     * @param spilledBuffers receive the created {@link SpilledBuffer} by this method.
     * @return total bytes(header size + buffer size) of all buffers to write.
     */
    private long createSpilledBuffersAndGetTotalBytes(
            List<BufferWithIdentity> toWrite, List<SpilledBuffer> spilledBuffers) {
        long expectedBytes = 0;
        for (BufferWithIdentity bufferWithIdentity : toWrite) {
            Buffer buffer = bufferWithIdentity.getBuffer();
            int numBytes = buffer.readableBytes() + BufferReaderWriterUtil.HEADER_LENGTH;
            spilledBuffers.add(
                    new SpilledBuffer(
                            bufferWithIdentity.getChannelIndex(),
                            bufferWithIdentity.getBufferIndex(),
                            totalBytesWritten + expectedBytes));
            expectedBytes += numBytes;
        }
        return expectedBytes;
    }

    /** Write all buffers to disk. */
    private void writeBuffers(List<BufferWithIdentity> bufferWithIdentities, long expectedBytes)
            throws IOException {
        if (bufferWithIdentities.isEmpty()) {
            return;
        }

        ByteBuffer[] bufferWithHeaders = new ByteBuffer[2 * bufferWithIdentities.size()];

        for (int i = 0; i < bufferWithIdentities.size(); i++) {
            Buffer buffer = bufferWithIdentities.get(i).getBuffer();
            setBufferWithHeader(buffer, bufferWithHeaders, 2 * i);
        }

        BufferReaderWriterUtil.writeBuffers(dataFileChannel, expectedBytes, bufferWithHeaders);
        totalBytesWritten += expectedBytes;
    }

    private void setBufferWithHeader(Buffer buffer, ByteBuffer[] bufferWithHeaders, int index) {
        ByteBuffer header = BufferReaderWriterUtil.allocatedHeaderBuffer();
        BufferReaderWriterUtil.setByteChannelBufferHeader(buffer, header);

        bufferWithHeaders[index] = header;
        bufferWithHeaders[index + 1] = buffer.getNioBufferReadable();
    }

    @Override
    public void close() throws Exception {
        ioExecutor.shutdown();
    }
}
