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

package org.apache.flink.table.runtime.io;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.compression.BlockCompressionFactory;
import org.apache.flink.runtime.io.compression.BlockCompressor;
import org.apache.flink.runtime.io.disk.iomanager.AbstractChannelWriterOutputView;
import org.apache.flink.runtime.io.disk.iomanager.BufferFileWriter;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A {@link org.apache.flink.core.memory.DataOutputView} that is backed by a {@link FileIOChannel},
 * making it effectively a data output stream. The view will compress its data before writing it in
 * blocks to the underlying channel.
 */
public final class CompressedHeaderlessChannelWriterOutputView
        extends AbstractChannelWriterOutputView implements BufferRecycler {

    private final MemorySegment buffer;
    private final LinkedBlockingQueue<MemorySegment> compressedBuffers =
            new LinkedBlockingQueue<>();
    private final BlockCompressor compressor;
    private final BufferFileWriter writer;
    private final int compressionBlockSize;

    private int blockCount;

    private long numBytes;
    private long numCompressedBytes;

    public CompressedHeaderlessChannelWriterOutputView(
            BufferFileWriter writer,
            BlockCompressionFactory compressionCodecFactory,
            int compressionBlockSize) {
        super(compressionBlockSize, 0);

        this.compressionBlockSize = compressionBlockSize;
        buffer = MemorySegmentFactory.wrap(new byte[compressionBlockSize]);
        compressor = compressionCodecFactory.getCompressor();
        for (int i = 0; i < 2; i++) {
            compressedBuffers.add(
                    MemorySegmentFactory.wrap(
                            new byte[compressor.getMaxCompressedSize(compressionBlockSize)]));
        }
        this.writer = writer;

        try {
            advance();
        } catch (IOException ioex) {
            throw new RuntimeException(ioex);
        }
    }

    @Override
    public FileIOChannel getChannel() {
        return writer;
    }

    @Override
    public int close() throws IOException {
        if (!writer.isClosed()) {
            int currentPositionInSegment = getCurrentPositionInSegment();
            writeCompressed(buffer, currentPositionInSegment);
            clear();
            this.writer.close();
        }
        return -1;
    }

    @Override
    protected MemorySegment nextSegment(MemorySegment current, int positionInCurrent)
            throws IOException {
        if (current != null) {
            writeCompressed(current, compressionBlockSize);
        }
        return buffer;
    }

    private void writeCompressed(MemorySegment current, int size) throws IOException {
        MemorySegment compressedBuffer;
        try {
            compressedBuffer = compressedBuffers.take();
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
        int compressedLen =
                compressor.compress(current.getArray(), 0, size, compressedBuffer.getArray(), 0);
        NetworkBuffer networkBuffer = new NetworkBuffer(compressedBuffer, this);
        networkBuffer.setSize(compressedLen);
        writer.writeBlock(networkBuffer);
        blockCount++;
        numBytes += size;
        numCompressedBytes += compressedLen;
    }

    @Override
    public long getNumBytes() {
        return numBytes;
    }

    @Override
    public long getNumCompressedBytes() {
        return numCompressedBytes;
    }

    @Override
    public int getBlockCount() {
        return blockCount;
    }

    @Override
    public void recycle(MemorySegment memorySegment) {
        compressedBuffers.add(memorySegment);
    }
}
