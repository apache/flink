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

import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierProducerAgent;

import javax.annotation.Nullable;

/**
 * {@link NettyConnectionWriter} is used by {@link TierProducerAgent} to write buffers to netty
 * connection. Buffers in the writer will be written to a queue structure and netty server will send
 * buffers from it.
 */
public interface NettyConnectionWriter {
    /**
     * Write a buffer to netty connection.
     *
     * @param nettyPayload the payload send to netty connection.
     */
    void writeNettyPayload(NettyPayload nettyPayload);

    /**
     * Get the id of connection in the writer.
     *
     * @return the id of connection.
     */
    NettyConnectionId getNettyConnectionId();

    /** Notify the buffer is available in writer. */
    void notifyAvailable();

    /**
     * Get the number of written but unsent netty payloads.
     *
     * @return the buffer number.
     */
    int numQueuedPayloads();

    /**
     * Get the number of written but unsent buffer netty payloads.
     *
     * @return the buffer number.
     */
    int numQueuedBufferPayloads();

    /**
     * If error is null, remove and recycle all buffers in the writer. If error is not null, the
     * error will be written after all buffers are removed and recycled.
     *
     * @param error error represents the exception information.
     */
    void close(@Nullable Throwable error);
}
