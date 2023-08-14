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

package org.apache.flink.runtime.io.network.partition.hybrid;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.Buffer.DataType;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;

import java.util.Collection;
import java.util.Optional;

/**
 * A view for {@link HsSubpartitionConsumer} to find out what data exists in memory or disk and
 * polling the data.
 */
public interface HsDataView {

    /**
     * Try to consume next buffer.
     *
     * <p>Only invoked by consumer thread.
     *
     * @param nextBufferToConsume next buffer index to consume.
     * @param buffersToRecycle buffers to recycle if needed.
     * @return If the target buffer does exist, return buffer and next buffer's backlog, otherwise
     *     return {@link Optional#empty()}.
     */
    Optional<BufferAndBacklog> consumeBuffer(
            int nextBufferToConsume, Collection<Buffer> buffersToRecycle) throws Throwable;

    /**
     * Get dataType of next buffer to consume.
     *
     * @param nextBufferToConsume next buffer index to consume
     * @param buffersToRecycle buffers to recycle if needed.
     * @return next buffer's dataType. If not found in memory, return {@link DataType#NONE}.
     */
    DataType peekNextToConsumeDataType(
            int nextBufferToConsume, Collection<Buffer> buffersToRecycle);

    /**
     * Get the number of buffers whose {@link Buffer.DataType} is buffer.
     *
     * @return backlog of this view's corresponding subpartition.
     */
    int getBacklog();

    /** Release this {@link HsDataView} when related subpartition view is releasing. */
    void releaseDataView();
}
