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

import java.io.IOException;

/**
 * Dispatches filtered channel-state data across multiple channels' {@link
 * org.apache.flink.runtime.io.network.partition.consumer.RecoveredBufferStore}s. {@link
 * #write(byte[], int, InputChannelInfo)} pushes data for a target channel; the implementation
 * decides whether to use a network buffer (P1), spill to disk (P2), or replay from disk (P3).
 */
@Internal
public interface FilteredBufferDispatcher extends AutoCloseable {

    void write(byte[] data, int length, InputChannelInfo channelInfo)
            throws IOException, InterruptedException;

    /** Flushes any buffered data. After flush, no more writes are accepted. */
    void flush() throws IOException;

    /**
     * Blocking drain of all remaining disk data into target stores. Must be called after {@link
     * #flush()} so all Readers are frozen. Skipped on the abort path: callers that are cancelling
     * go straight to {@link #close()}. Does not hold the dispatcher monitor while blocking;
     * coordinator callbacks remain free to acquire it.
     */
    void drainPendingSpill() throws IOException, InterruptedException;

    /**
     * Releases resources held by this dispatcher. Idempotent. Pure resource release: does NOT drain
     * remaining disk data — call {@link #drainPendingSpill()} first on the normal path.
     */
    @Override
    void close() throws IOException;
}
