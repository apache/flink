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

package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.io.network.buffer.Buffer;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * Supplies per-channel network buffers to the recovery pipeline.
 *
 * <p>Two semantics are exposed: a non-blocking request used on the fast path where the caller must
 * not stall (e.g. while writing on the recovery thread), and a blocking request used where
 * backpressure is acceptable (e.g. the close() drain).
 */
@Internal
interface BufferRequester {

    /** Non-blocking request; returns {@code null} when no buffer is currently available. */
    @Nullable
    Buffer requestBuffer(InputChannelInfo channelInfo) throws IOException;

    /** Blocking request; waits until a buffer becomes available. */
    Buffer requestBufferBlocking(InputChannelInfo channelInfo)
            throws InterruptedException, IOException;

    /**
     * Releases the exclusive buffers held by every channel served by this requester. Must be
     * called after the dispatcher's drain has finished so the underlying pools are no longer
     * being read from. Idempotent.
     *
     * <p>Symmetric tear-down counterpart of {@link #requestBuffer} / {@link #requestBufferBlocking}:
     * whoever supplies the buffers also owns their final disposal. Centralising the release here
     * lets {@code convertRecoveredInputChannels} stop calling {@code releaseAllResources()} on the
     * {@link org.apache.flink.runtime.io.network.partition.consumer.RecoveredInputChannel} —
     * which would otherwise wipe the recovered store and exclusive segments while drain is still
     * in flight.
     */
    void releaseExclusiveBuffers() throws IOException;
}
