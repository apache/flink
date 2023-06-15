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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.util.LinkedList;

import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * When getting buffers, The {@link SortBasedDataBuffer} should recycle the read target buffer with
 * the given {@link BufferRecycler}.
 */
@NotThreadSafe
public class SortBasedDataBuffer extends SortBuffer {

    public SortBasedDataBuffer(
            LinkedList<MemorySegment> freeSegments,
            BufferRecycler bufferRecycler,
            int numSubpartitions,
            int bufferSize,
            int numGuaranteedBuffers,
            @Nullable int[] customReadOrder) {
        super(
                freeSegments,
                bufferRecycler,
                numSubpartitions,
                bufferSize,
                numGuaranteedBuffers,
                customReadOrder);
    }

    @Override
    public BufferWithChannel getNextBuffer(MemorySegment transitBuffer) {
        checkState(isFinished, "Sort buffer is not ready to be read.");
        checkState(!isReleased, "Sort buffer is already released.");

        if (!hasRemaining()) {
            return null;
        }

        int numBytesCopied = 0;
        DataType bufferDataType = DataType.DATA_BUFFER;
        int channelIndex = subpartitionReadOrder[readOrderIndex];

        do {
            int sourceSegmentIndex = getSegmentIndexFromPointer(readIndexEntryAddress);
            int sourceSegmentOffset = getSegmentOffsetFromPointer(readIndexEntryAddress);
            MemorySegment sourceSegment = segments.get(sourceSegmentIndex);

            long lengthAndDataType = sourceSegment.getLong(sourceSegmentOffset);
            int length = getSegmentIndexFromPointer(lengthAndDataType);
            DataType dataType = DataType.values()[getSegmentOffsetFromPointer(lengthAndDataType)];

            // return the data read directly if the next to read is an event
            if (dataType.isEvent() && numBytesCopied > 0) {
                break;
            }
            bufferDataType = dataType;

            // get the next index entry address and move the read position forward
            long nextReadIndexEntryAddress = sourceSegment.getLong(sourceSegmentOffset + 8);
            sourceSegmentOffset += INDEX_ENTRY_SIZE;

            // allocate a temp buffer for the event if the target buffer is not big enough
            if (bufferDataType.isEvent() && transitBuffer.size() < length) {
                transitBuffer = MemorySegmentFactory.allocateUnpooledSegment(length);
            }

            numBytesCopied +=
                    copyRecordOrEvent(
                            transitBuffer,
                            numBytesCopied,
                            sourceSegmentIndex,
                            sourceSegmentOffset,
                            length);

            if (recordRemainingBytes == 0) {
                // move to next channel if the current channel has been finished
                if (readIndexEntryAddress == lastIndexEntryAddresses[channelIndex]) {
                    updateReadChannelAndIndexEntryAddress();
                    break;
                }
                readIndexEntryAddress = nextReadIndexEntryAddress;
            }
        } while (numBytesCopied < transitBuffer.size() && bufferDataType.isBuffer());

        numTotalBytesRead += numBytesCopied;
        Buffer buffer =
                new NetworkBuffer(transitBuffer, (buf) -> {}, bufferDataType, numBytesCopied);
        return new BufferWithChannel(buffer, channelIndex);
    }
}
