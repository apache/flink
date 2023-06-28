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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.file;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;

import java.io.IOException;
import java.nio.file.Path;

/**
 * The implementation of {@link PartitionFileReader} with producer-merge mode. In this mode, the
 * shuffle data is written in the producer side, the consumer side need to read multiple producers
 * to get its partition data.
 *
 * <p>Note that one partition file may contain the data of multiple subpartitions.
 */
public class ProducerMergedPartitionFileReader implements PartitionFileReader {

    ProducerMergedPartitionFileReader(
            Path dataFilePath, ProducerMergedPartitionFileIndex partitionFileIndex) {
        // TODO, implement the ProducerMergePartitionFileReader
    }

    @Override
    public Buffer readBuffer(
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId,
            int segmentId,
            int bufferIndex,
            MemorySegment memorySegment,
            BufferRecycler recycler)
            throws IOException {
        // TODO, implement the ProducerMergePartitionFileReader
        return null;
    }

    @Override
    public long getPriority(
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId,
            int segmentId,
            int bufferIndex) {
        return 0;
    }

    @Override
    public void release() {}
}
