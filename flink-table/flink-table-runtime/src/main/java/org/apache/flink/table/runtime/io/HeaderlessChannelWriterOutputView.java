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
import org.apache.flink.runtime.io.disk.iomanager.AbstractChannelWriterOutputView;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelWriter;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A {@link org.apache.flink.core.memory.DataOutputView} that is backed by a {@link
 * BlockChannelWriter}, making it effectively a data output stream. The view writes its data in
 * blocks to the underlying channel, but does not expect header for each block.
 */
public final class HeaderlessChannelWriterOutputView extends AbstractChannelWriterOutputView {

    private final BlockChannelWriter<MemorySegment> writer; // the writer to the channel

    private int blockCount; // the number of blocks used

    public HeaderlessChannelWriterOutputView(
            BlockChannelWriter<MemorySegment> writer, List<MemorySegment> memory, int segmentSize) {
        super(segmentSize, 0);

        if (writer == null) {
            throw new NullPointerException();
        }

        this.writer = writer;

        Preconditions.checkNotNull(memory);

        // load the segments into the queue
        final LinkedBlockingQueue<MemorySegment> queue = writer.getReturnQueue();
        for (int i = memory.size() - 1; i >= 0; --i) {
            final MemorySegment seg = memory.get(i);
            if (seg.size() != segmentSize) {
                throw new IllegalArgumentException("This segment are not of the specified size.");
            }
            queue.add(seg);
        }

        // get the first segment
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

    /**
     * Closes this OutputView, closing the underlying writer. And return number bytes in last memory
     * segment.
     */
    @Override
    public int close() throws IOException {
        if (!writer.isClosed()) {
            int currentPositionInSegment = getCurrentPositionInSegment();
            // write last segment
            writer.writeBlock(getCurrentSegment());
            clear();

            writer.getReturnQueue().clear();
            this.writer.close();

            return currentPositionInSegment;
        }
        return -1;
    }

    @Override
    public int getBlockCount() {
        return this.blockCount;
    }

    @Override
    public long getNumBytes() throws IOException {
        return writer.getSize();
    }

    @Override
    public long getNumCompressedBytes() throws IOException {
        return writer.getSize();
    }

    @Override
    public MemorySegment nextSegment(MemorySegment current, int posInSegment) throws IOException {
        if (current != null) {
            writer.writeBlock(current);
        }

        final MemorySegment next = this.writer.getNextReturnedBlock();
        this.blockCount++;
        return next;
    }
}
