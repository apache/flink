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

package org.apache.flink.runtime.io.network.api.serialization;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.util.CloseableIterator;

import java.io.IOException;

/** Interface for turning sequences of memory segments into records. */
public interface RecordDeserializer<T extends IOReadableWritable> {

    /** Status of the deserialization result. */
    enum DeserializationResult {
        PARTIAL_RECORD(false, true),
        INTERMEDIATE_RECORD_FROM_BUFFER(true, false),
        LAST_RECORD_FROM_BUFFER(true, true);

        private final boolean isFullRecord;

        private final boolean isBufferConsumed;

        private DeserializationResult(boolean isFullRecord, boolean isBufferConsumed) {
            this.isFullRecord = isFullRecord;
            this.isBufferConsumed = isBufferConsumed;
        }

        public boolean isFullRecord() {
            return this.isFullRecord;
        }

        public boolean isBufferConsumed() {
            return this.isBufferConsumed;
        }
    }

    DeserializationResult getNextRecord(T target) throws IOException;

    void setNextBuffer(Buffer buffer) throws IOException;

    Buffer getCurrentBuffer();

    void clear();

    boolean hasUnfinishedData();

    /**
     * Gets the unconsumed buffer which needs to be persisted in unaligned checkpoint scenario.
     *
     * <p>Note that the unconsumed buffer might be null if the whole buffer was already consumed
     * before and there are no partial length or data remained in the end of buffer.
     */
    CloseableIterator<Buffer> getUnconsumedBuffer() throws IOException;
}
