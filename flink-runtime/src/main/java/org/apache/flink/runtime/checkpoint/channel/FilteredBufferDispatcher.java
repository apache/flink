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
 * Dispatches filtered channel state data across multiple channels' {@link
 * org.apache.flink.runtime.io.network.partition.consumer.RecoveredBufferStore}s, managing the
 * buffer/disk backend transparently. Callers use {@link #write(byte[], int, InputChannelInfo)} to
 * push data for a target channel, and the implementation decides whether to use a network buffer
 * (P1), spill to disk (P2), or replay from disk (P3).
 */
@Internal
public interface FilteredBufferDispatcher extends AutoCloseable {

    /**
     * Writes data for the given channel.
     *
     * @param data the byte array containing the data
     * @param length the number of bytes to write from the data array
     * @param channelInfo the target input channel
     * @throws IOException if an I/O error occurs
     * @throws InterruptedException if the thread is interrupted while waiting for resources
     */
    void write(byte[] data, int length, InputChannelInfo channelInfo)
            throws IOException, InterruptedException;

    /**
     * Flushes any buffered data. After flush, no more writes are accepted.
     *
     * @throws IOException if an I/O error occurs
     */
    void flush() throws IOException;

    /**
     * Blocking drain of all remaining disk data into target stores. Producer-consumer semantics:
     * blocks waiting for network buffers; Thread.interrupt() unblocks the wait.
     *
     * <p>Call sequence: {@link #flush()} -> stateHandler.finishRecovery() ->
     * {@link #drainPendingSpill()} -> {@link #close()}. Runs concurrently with Task thread
     * consumption and checkpoint on converted InputChannels.
     *
     * <p>Contract:
     * - Must be called after {@link #flush()} so all Readers are sealed.
     * - Does NOT hold the dispatcher monitor while blocking; coordinator callbacks
     *   (e.g. onChannelCheckpointStopped) remain free to acquire the monitor.
     * - Skipped on the abort path: callers that are cancelling go straight to close().
     *
     * @throws IOException if an I/O error occurs
     * @throws InterruptedException if the thread is interrupted during the blocking drain
     */
    void drainPendingSpill() throws IOException, InterruptedException;

    /**
     * Releases resources held by this dispatcher (spill file channel, internal state fields).
     * Idempotent: calling close() multiple times is safe.
     *
     * <p>Contract:
     * - Pure resource release: does NOT drain remaining disk data. Call {@link #drainPendingSpill()}
     *   first on the normal path.
     * - Short lock: may hold the dispatcher monitor briefly to protect state fields, but never
     *   blocks on I/O or buffer allocation inside the lock.
     * - Does not throw business exceptions.
     *
     * @throws IOException if closing the spill file channel fails
     */
    @Override
    void close() throws IOException;
}
