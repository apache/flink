/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.changelog.fs;

import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.changelog.SequenceNumber;

import static org.apache.flink.util.Preconditions.checkNotNull;

final class UploadResult {
    public final StreamStateHandle streamStateHandle;
    public final long offset;
    public final SequenceNumber sequenceNumber;
    public final long size;

    public UploadResult(
            StreamStateHandle streamStateHandle,
            long offset,
            SequenceNumber sequenceNumber,
            long size) {
        this.streamStateHandle = checkNotNull(streamStateHandle);
        this.offset = offset;
        this.sequenceNumber = checkNotNull(sequenceNumber);
        this.size = size;
    }

    public static UploadResult of(StreamStateHandle handle, StateChangeSet changeSet, long offset) {
        return new UploadResult(handle, offset, changeSet.getSequenceNumber(), changeSet.getSize());
    }

    public StreamStateHandle getStreamStateHandle() {
        return streamStateHandle;
    }

    public long getOffset() {
        return offset;
    }

    public SequenceNumber getSequenceNumber() {
        return sequenceNumber;
    }

    public long getSize() {
        return size;
    }

    @Override
    public String toString() {
        return "streamStateHandle="
                + streamStateHandle
                + ", size="
                + size
                + ", offset="
                + offset
                + ", sequenceNumber="
                + sequenceNumber;
    }
}
