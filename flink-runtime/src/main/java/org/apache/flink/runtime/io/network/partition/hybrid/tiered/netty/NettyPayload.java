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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty;

import org.apache.flink.runtime.io.network.buffer.Buffer;

import javax.annotation.Nullable;

import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The {@link NettyPayload} represents the payload that will be transferred to netty connection. It
 * could indicate a combination of buffer, buffer index, and its subpartition id, and it could also
 * indicate an error or a segment id.
 */
public class NettyPayload {

    /**
     * The data buffer. If the buffer is not null, bufferIndex and subpartitionId will be
     * non-negative, error will be null, segmentId will be -1;
     */
    @Nullable private final Buffer buffer;

    /**
     * The error information. If the error is not null, buffer will be null, segmentId and
     * bufferIndex and subpartitionId will be -1.
     */
    @Nullable private final Throwable error;

    /**
     * The index of buffer. If the buffer index is non-negative, buffer won't be null, error will be
     * null, subpartitionId will be non-negative, segmentId will be -1.
     */
    private final int bufferIndex;

    /**
     * The id of subpartition. If the subpartition id is non-negative, buffer won't be null, error
     * will be null, bufferIndex will be non-negative, segmentId will be -1.
     */
    private final int subpartitionId;

    /**
     * The id of segment. If the segment id is non-negative, buffer and error will be null,
     * bufferIndex and subpartitionId will be -1.
     */
    private final int segmentId;

    private NettyPayload(
            @Nullable Buffer buffer,
            int bufferIndex,
            int subpartitionId,
            @Nullable Throwable error,
            int segmentId) {
        this.buffer = buffer;
        this.bufferIndex = bufferIndex;
        this.subpartitionId = subpartitionId;
        this.error = error;
        this.segmentId = segmentId;
    }

    public static NettyPayload newBuffer(Buffer buffer, int bufferIndex, int subpartitionId) {
        checkState(buffer != null && bufferIndex != -1 && subpartitionId != -1);
        return new NettyPayload(buffer, bufferIndex, subpartitionId, null, -1);
    }

    public static NettyPayload newError(Throwable error) {
        return new NettyPayload(null, -1, -1, checkNotNull(error), -1);
    }

    public static NettyPayload newSegment(int segmentId) {
        checkState(segmentId != -1);
        return new NettyPayload(null, -1, -1, null, segmentId);
    }

    public Optional<Buffer> getBuffer() {
        return buffer != null ? Optional.of(buffer) : Optional.empty();
    }

    public Optional<Throwable> getError() {
        return error != null ? Optional.of(error) : Optional.empty();
    }

    public int getBufferIndex() {
        return bufferIndex;
    }

    public int getSubpartitionId() {
        return subpartitionId;
    }

    public int getSegmentId() {
        return segmentId;
    }
}
