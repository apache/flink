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

package org.apache.flink.runtime.io.network.buffer;

import javax.annotation.concurrent.NotThreadSafe;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * BufferConsumer with partial record length if a record is spanning over buffers
 *
 * <p>`partialRecordLength` is the length of bytes to skip in order to start with a complete record,
 * from position index 0 of the underlying MemorySegment. `partialRecordLength` is used in
 * approximate local recovery to find the start position of a complete record on a BufferConsumer,
 * so called `partial record clean-up`.
 *
 * <p>Partial records happen if a record can not fit into one buffer, then the remaining part of the
 * same record is put into the next buffer. Hence partial records only exist at the beginning of a
 * buffer. Partial record clean-up is needed in the mode of approximate local recovery. If a record
 * is spanning over multiple buffers, and the first (several) buffers have got lost due to the
 * failure of the receiver task, the remaining data belonging to the same record in transition
 * should be cleaned up.
 *
 * <p>If partialRecordLength == 0, the buffer starts with a complete record
 *
 * <p>If partialRecordLength > 0, the buffer starts with a partial record, its length =
 * partialRecordLength
 *
 * <p>If partialRecordLength < 0, partialRecordLength is undefined. It is currently used in {@cite
 * ResultSubpartitionRecoveredStateHandler#recover}
 */
@NotThreadSafe
public class BufferConsumerWithPartialRecordLength {
    private final BufferConsumer bufferConsumer;
    private final int partialRecordLength;

    public BufferConsumerWithPartialRecordLength(
            BufferConsumer bufferConsumer, int partialRecordLength) {
        this.bufferConsumer = checkNotNull(bufferConsumer);
        this.partialRecordLength = partialRecordLength;
    }

    public BufferConsumer getBufferConsumer() {
        return bufferConsumer;
    }

    public int getPartialRecordLength() {
        return partialRecordLength;
    }

    public Buffer build() {
        return bufferConsumer.build();
    }

    public boolean cleanupPartialRecord() {

        checkState(
                partialRecordLength >= 0,
                "Approximate local recovery does not yet work with unaligned checkpoint!");

        // partial record can happen only at the beginning of a buffer, because a buffer can end
        // with
        // either a full record or full buffer after each write and read. Partial records occur only
        // when a
        // bufferBuilder ends with a full buffer but not a full record (a record spanning multiple
        // buffers).
        if (partialRecordLength == 0 || !bufferConsumer.isStartOfDataBuffer()) {
            return true;
        }

        // partial data is appendAndCommit before bufferConsumer is created,
        // so we do not have the case that data is written but not visible.

        checkState(
                partialRecordLength <= bufferConsumer.getBufferSize(),
                "Partial record length beyond max buffer capacity!");
        bufferConsumer.skip(partialRecordLength);
        // CASE: partialRecordLength < max buffer size,
        // partial record ends within the buffer, cleanup successful, skip the partial record
        if (partialRecordLength < bufferConsumer.getBufferSize()) {
            return true;
        } else {
            // two cases: 1). long record spanning multiple buffers, cleanup not successful, return
            // an empty buffer
            //            2). partial record ending at the end of the buffer (full record, full
            // buffer)
            //            		cleaned up not successful, return an empty buffer. Notice that next
            // buffer
            //            		will start with a full record, and cleanup will be success in the next
            // call.
            return false;
        }
    }
}
