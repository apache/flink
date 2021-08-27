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

package org.apache.flink.runtime.io.disk.iomanager;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.memory.AbstractPagedOutputView;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A {@link org.apache.flink.core.memory.DataOutputView} that is backed by a {@link
 * BlockChannelWriter}, making it effectively a data output stream. The view writes its data in
 * blocks to the underlying channel, adding a minimal header to each block. The data can be re-read
 * by a {@link ChannelReaderInputView}, if it uses the same block size.
 */
public final class ChannelWriterOutputView extends AbstractPagedOutputView {

    /** The magic number that identifies blocks as blocks from a ChannelWriterOutputView. */
    protected static final short HEADER_MAGIC_NUMBER = (short) 0xC0FE;

    /** The length of the header put into the blocks. */
    protected static final int HEADER_LENGTH = 8;

    /** The offset to the flags in the header; */
    protected static final int HEADER_FLAGS_OFFSET = 2;

    /** The offset to the header field indicating the number of bytes in the block */
    protected static final int HEAD_BLOCK_LENGTH_OFFSET = 4;

    /** The flag marking a block as the last block. */
    protected static final short FLAG_LAST_BLOCK = (short) 0x1;

    // --------------------------------------------------------------------------------------------

    private final BlockChannelWriter<MemorySegment> writer; // the writer to the channel

    private long
            bytesBeforeSegment; // the number of bytes written before the current memory segment

    private int blockCount; // the number of blocks used

    private final int numSegments; // the number of memory segments used by this view

    // --------------------------------------------------------------------------------------------

    /**
     * Creates an new ChannelWriterOutputView that writes to the given channel and buffers data in
     * the given memory segments. If the given memory segments are null, the writer takes its
     * buffers directly from the return queue of the writer. Note that this variant locks if no
     * buffers are contained in the return queue.
     *
     * @param writer The writer to write to.
     * @param memory The memory used to buffer data, or null, to utilize solely the return queue.
     * @param segmentSize The size of the memory segments.
     */
    public ChannelWriterOutputView(
            BlockChannelWriter<MemorySegment> writer, List<MemorySegment> memory, int segmentSize) {
        super(segmentSize, HEADER_LENGTH);

        if (writer == null) {
            throw new NullPointerException();
        }

        this.writer = writer;

        if (memory == null) {
            this.numSegments = 0;
        } else {
            this.numSegments = memory.size();
            // load the segments into the queue
            final LinkedBlockingQueue<MemorySegment> queue = writer.getReturnQueue();
            for (int i = memory.size() - 1; i >= 0; --i) {
                final MemorySegment seg = memory.get(i);
                if (seg.size() != segmentSize) {
                    throw new IllegalArgumentException(
                            "The supplied memory segments are not of the specified size.");
                }
                queue.add(seg);
            }
        }

        // get the first segment
        try {
            advance();
        } catch (IOException ioex) {
            throw new RuntimeException(
                    "BUG: IOException occurred while getting first block for ChannelWriterOutputView.",
                    ioex);
        }
    }

    /**
     * Creates an new ChannelWriterOutputView that writes to the given channel. It uses only a
     * single memory segment for the buffering, which it takes from the writer's return queue. Note
     * that this variant locks if no buffers are contained in the return queue.
     *
     * @param writer The writer to write to.
     * @param segmentSize The size of the memory segments.
     */
    public ChannelWriterOutputView(BlockChannelWriter<MemorySegment> writer, int segmentSize) {
        this(writer, null, segmentSize);
    }

    // --------------------------------------------------------------------------------------------

    /**
     * Closes this OutputView, closing the underlying writer and returning all memory segments.
     *
     * @return A list containing all memory segments originally supplied to this view.
     * @throws IOException Thrown, if the underlying writer could not be properly closed.
     */
    public List<MemorySegment> close() throws IOException {
        // send off set last segment
        writeSegment(getCurrentSegment(), getCurrentPositionInSegment(), true);
        clear();

        // close the writer and gather all segments
        final LinkedBlockingQueue<MemorySegment> queue = this.writer.getReturnQueue();
        this.writer.close();

        // re-collect all memory segments
        ArrayList<MemorySegment> list = new ArrayList<MemorySegment>(this.numSegments);
        for (int i = 0; i < this.numSegments; i++) {
            final MemorySegment m = queue.poll();
            if (m == null) {
                // we get null if the queue is empty. that should not be the case if the reader was
                // properly closed.
                throw new RuntimeException(
                        "ChannelWriterOutputView: MemorySegments have been taken from return queue by different actor.");
            }
            list.add(m);
        }

        return list;
    }

    // --------------------------------------------------------------------------------------------

    /**
     * Gets the number of blocks used by this view.
     *
     * @return The number of blocks used.
     */
    public int getBlockCount() {
        return this.blockCount;
    }

    /**
     * Gets the number of pay-load bytes already written. This excludes the number of bytes spent on
     * headers in the segments.
     *
     * @return The number of bytes that have been written to this output view.
     */
    public long getBytesWritten() {
        return this.bytesBeforeSegment + getCurrentPositionInSegment() - HEADER_LENGTH;
    }

    /**
     * Gets the number of bytes used by this output view, including written bytes and header bytes.
     *
     * @return The number of bytes that have been written to this output view.
     */
    public long getBytesMemoryUsed() {
        return (this.blockCount - 1) * getSegmentSize() + getCurrentPositionInSegment();
    }

    // --------------------------------------------------------------------------------------------
    //                                      Page Management
    // --------------------------------------------------------------------------------------------

    protected final MemorySegment nextSegment(MemorySegment current, int posInSegment)
            throws IOException {
        if (current != null) {
            writeSegment(current, posInSegment, false);
        }

        final MemorySegment next = this.writer.getNextReturnedBlock();
        this.blockCount++;
        return next;
    }

    private void writeSegment(MemorySegment segment, int writePosition, boolean lastSegment)
            throws IOException {
        segment.putShort(0, HEADER_MAGIC_NUMBER);
        segment.putShort(HEADER_FLAGS_OFFSET, lastSegment ? FLAG_LAST_BLOCK : 0);
        segment.putInt(HEAD_BLOCK_LENGTH_OFFSET, writePosition);

        this.writer.writeBlock(segment);
        this.bytesBeforeSegment += writePosition - HEADER_LENGTH;
    }
}
