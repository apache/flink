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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.BufferWithChannel;
import org.apache.flink.runtime.io.network.partition.SortBasedDataBuffer;
import org.apache.flink.runtime.io.network.partition.SortBuffer;

import javax.annotation.Nullable;

import java.util.LinkedList;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * When getting buffers, The {@link SortBasedDataBuffer} need not recycle the read target buffer..
 */
public class TieredStorageSortBuffer extends SortBuffer {

    public TieredStorageSortBuffer(
            LinkedList<MemorySegment> freeSegments,
            BufferRecycler bufferRecycler,
            int numSubpartitions,
            int bufferSize,
            int numGuaranteedBuffers) {
        super(
                freeSegments,
                bufferRecycler,
                numSubpartitions,
                bufferSize,
                numGuaranteedBuffers,
                null);
    }

    @Override
    public BufferWithChannel getNextBuffer(@Nullable MemorySegment transitBuffer) {
        checkState(isFinished, "Sort buffer is not ready to be read.");
        checkState(!isReleased, "Sort buffer is already released.");

        if (!hasRemaining()) {
            freeSegments.add(transitBuffer);
            return null;
        }

        int numBytesRead = 0;
        Buffer.DataType bufferDataType = Buffer.DataType.DATA_BUFFER;
        int currentReadingSubpartitionId = subpartitionReadOrder[readOrderIndex];

        do {
            // Get the buffer index and offset from the index entry
            int toReadBufferIndex = getSegmentIndexFromPointer(readIndexEntryAddress);
            int toReadOffsetInBuffer = getSegmentOffsetFromPointer(readIndexEntryAddress);

            // Get the lengthAndDataType buffer according the buffer index
            MemorySegment toReadBuffer = segments.get(toReadBufferIndex);

            // From the lengthAndDataType buffer, read and get the length and the data type
            long lengthAndDataType = toReadBuffer.getLong(toReadOffsetInBuffer);
            int recordLength = getSegmentIndexFromPointer(lengthAndDataType);
            Buffer.DataType dataType =
                    Buffer.DataType.values()[getSegmentOffsetFromPointer(lengthAndDataType)];

            // If the buffer is an event and some data has been read, return it directly to ensure
            // that the event will occupy one buffer independently
            if (dataType.isEvent() && numBytesRead > 0) {
                break;
            }
            bufferDataType = dataType;

            // Get the next index entry address and move the read position forward
            long nextReadIndexEntryAddress = toReadBuffer.getLong(toReadOffsetInBuffer + 8);
            toReadOffsetInBuffer += INDEX_ENTRY_SIZE;

            // Allocate a temp buffer for the event, recycle the original buffer
            if (bufferDataType.isEvent()) {
                freeSegments.add(transitBuffer);
                transitBuffer = MemorySegmentFactory.allocateUnpooledSegment(recordLength);
            }

            // Start reading data from the data buffer
            numBytesRead +=
                    copyRecordOrEvent(
                            transitBuffer,
                            numBytesRead,
                            toReadBufferIndex,
                            toReadOffsetInBuffer,
                            recordLength);

            if (recordRemainingBytes == 0) {
                // move to next channel if the current channel has been finished
                if (readIndexEntryAddress
                        == lastIndexEntryAddresses[currentReadingSubpartitionId]) {
                    updateReadChannelAndIndexEntryAddress();
                    break;
                }
                readIndexEntryAddress = nextReadIndexEntryAddress;
            }
        } while (numBytesRead < transitBuffer.size() && bufferDataType.isBuffer());

        numTotalBytesRead += numBytesRead;
        return new BufferWithChannel(
                new NetworkBuffer(
                        transitBuffer,
                        bufferDataType == Buffer.DataType.DATA_BUFFER
                                ? bufferRecycler
                                : FreeingBufferRecycler.INSTANCE,
                        bufferDataType,
                        numBytesRead),
                currentReadingSubpartitionId);
    }
}
