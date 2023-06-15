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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.BufferAccumulator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;

/** Test implementation for {@link BufferAccumulator}. */
public class TestingBufferAccumulator implements BufferAccumulator {

    private BiConsumer<TieredStorageSubpartitionId, List<Buffer>> bufferFlusher;

    @Override
    public void setup(BiConsumer<TieredStorageSubpartitionId, List<Buffer>> bufferFlusher) {
        this.bufferFlusher = bufferFlusher;
    }

    @Override
    public void receive(
            ByteBuffer record,
            TieredStorageSubpartitionId subpartitionId,
            Buffer.DataType dataType,
            boolean isBroadcast)
            throws IOException {
        MemorySegment recordData = MemorySegmentFactory.wrap(record.array());
        bufferFlusher.accept(
                subpartitionId,
                Collections.singletonList(
                        new NetworkBuffer(
                                recordData,
                                FreeingBufferRecycler.INSTANCE,
                                dataType,
                                recordData.size())));
    }

    @Override
    public void close() {}
}
