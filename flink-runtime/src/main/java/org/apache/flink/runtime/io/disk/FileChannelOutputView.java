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

package org.apache.flink.runtime.io.disk;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelWriter;
import org.apache.flink.runtime.memory.AbstractPagedOutputView;
import org.apache.flink.runtime.memory.MemoryManager;

import java.io.IOException;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link org.apache.flink.core.memory.DataOutputView} that is backed by a {@link
 * BlockChannelWriter}, making it effectively a data output stream. The view writes it data in
 * blocks to the underlying channel.
 */
public class FileChannelOutputView extends AbstractPagedOutputView {

    private final BlockChannelWriter<MemorySegment> writer; // the writer to the channel

    private final MemoryManager memManager;

    private final List<MemorySegment> memory;

    private int numBlocksWritten;

    private int bytesInLatestSegment;

    // --------------------------------------------------------------------------------------------

    public FileChannelOutputView(
            BlockChannelWriter<MemorySegment> writer,
            MemoryManager memManager,
            List<MemorySegment> memory,
            int segmentSize)
            throws IOException {
        super(segmentSize, 0);

        checkNotNull(writer);
        checkNotNull(memManager);
        checkNotNull(memory);
        checkArgument(!writer.isClosed());

        this.writer = writer;
        this.memManager = memManager;
        this.memory = memory;

        for (MemorySegment next : memory) {
            writer.getReturnQueue().add(next);
        }

        // move to the first page
        advance();
    }

    // --------------------------------------------------------------------------------------------

    /**
     * Closes this output, writing pending data and releasing the memory.
     *
     * @throws IOException Thrown, if the pending data could not be written.
     */
    public void close() throws IOException {
        close(false);
    }

    /**
     * Closes this output, writing pending data and releasing the memory.
     *
     * @throws IOException Thrown, if the pending data could not be written.
     */
    public void closeAndDelete() throws IOException {
        close(true);
    }

    private void close(boolean delete) throws IOException {
        try {
            // send off set last segment, if we have not been closed before
            MemorySegment current = getCurrentSegment();
            if (current != null) {
                writeSegment(current, getCurrentPositionInSegment());
            }

            clear();
            if (delete) {
                writer.closeAndDelete();
            } else {
                writer.close();
            }
        } finally {
            memManager.release(memory);
        }
    }

    // --------------------------------------------------------------------------------------------

    /**
     * Gets the number of blocks written by this output view.
     *
     * @return The number of blocks written by this output view.
     */
    public int getBlockCount() {
        return numBlocksWritten;
    }

    /**
     * Gets the number of bytes written in the latest memory segment.
     *
     * @return The number of bytes written in the latest memory segment.
     */
    public int getBytesInLatestSegment() {
        return bytesInLatestSegment;
    }

    public long getWriteOffset() {
        return ((long) numBlocksWritten) * segmentSize + getCurrentPositionInSegment();
    }

    @Override
    protected MemorySegment nextSegment(MemorySegment current, int posInSegment)
            throws IOException {
        if (current != null) {
            writeSegment(current, posInSegment);
        }
        return writer.getNextReturnedBlock();
    }

    private void writeSegment(MemorySegment segment, int writePosition) throws IOException {
        writer.writeBlock(segment);
        numBlocksWritten++;
        bytesInLatestSegment = writePosition;
    }
}
