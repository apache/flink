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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.BiConsumer;

/**
 * Accumulates received records into buffers. The {@link BufferAccumulator} receives the records
 * from tiered store producer and the records will accumulate and transform into buffers.
 */
public interface BufferAccumulator extends AutoCloseable {

    /**
     * Setup the accumulator.
     *
     * @param bufferFlusher accepts the accumulated buffers. The first field is the subpartition id,
     *     while the list in the second field contains accumulated buffers in order for that
     *     subpartition.
     */
    void setup(BiConsumer<TieredStorageSubpartitionId, List<Buffer>> bufferFlusher);

    /**
     * Receives the records from tiered store producer, these records will be accumulated and
     * transformed into finished buffers.
     */
    void receive(
            ByteBuffer record, TieredStorageSubpartitionId subpartitionId, Buffer.DataType dataType)
            throws IOException;

    /**
     * Close the accumulator. This will flush all the remaining data and release all the resources.
     */
    void close();
}
