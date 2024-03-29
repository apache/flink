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

import java.io.IOException;

/** A factory for buffer pools. */
public interface BufferPoolFactory {

    /**
     * Tries to create a buffer pool, which is guaranteed to provide at least the number of required
     * buffers.
     *
     * <p>The buffer pool is of dynamic size ranges from <tt>minUsedBuffers</tt> to
     * <tt>maxUsedBuffers</tt>, with <tt>numExpectedBuffers</tt> serving as the weight.
     *
     * @param numExpectedBuffers the number of expected network buffers of this pool
     * @param minUsedBuffers minimum number of network buffers in this pool
     * @param maxUsedBuffers maximum number of network buffers this pool offers
     */
    BufferPool createBufferPool(int numExpectedBuffers, int minUsedBuffers, int maxUsedBuffers)
            throws IOException;

    /**
     * Tries to create a buffer pool with an owner, which is guaranteed to provide at least the
     * number of required buffers.
     *
     * <p>The buffer pool is of dynamic size ranges from <tt>minUsedBuffers</tt> to
     * <tt>maxUsedBuffers</tt>, with <tt>numExpectedBuffers</tt> serving as the weight.
     *
     * @param numExpectedBuffers the number of expected network buffers of this pool
     * @param minUsedBuffers minimum number of network buffers in this pool
     * @param maxUsedBuffers maximum number of network buffers this pool offers
     * @param numSubpartitions number of subpartitions in this pool
     * @param maxBuffersPerChannel maximum number of buffers to use for each channel
     * @param maxOverdraftBuffersPerGate maximum number of overdraft buffers to use for each gate
     */
    BufferPool createBufferPool(
            int numExpectedBuffers,
            int minUsedBuffers,
            int maxUsedBuffers,
            int numSubpartitions,
            int maxBuffersPerChannel,
            int maxOverdraftBuffersPerGate)
            throws IOException;

    /** Destroy callback for updating factory book keeping. */
    void destroyBufferPool(BufferPool bufferPool) throws IOException;
}
