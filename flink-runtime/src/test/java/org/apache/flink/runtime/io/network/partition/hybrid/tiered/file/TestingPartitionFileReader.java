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
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.CompositeBuffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.function.BiFunction;
import java.util.function.Function;

/** Testing implementation for {@link PartitionFileReader}. */
public class TestingPartitionFileReader implements PartitionFileReader {

    private final BiFunction<Integer, Integer, ReadBufferResult> readBufferFunction;

    private final Function<Integer, Long> getPriorityFunction;

    private final Runnable releaseRunnable;

    private TestingPartitionFileReader(
            BiFunction<Integer, Integer, ReadBufferResult> readBufferFunction,
            Function<Integer, Long> getPriorityFunction,
            Runnable releaseRunnable) {
        this.readBufferFunction = readBufferFunction;
        this.getPriorityFunction = getPriorityFunction;
        this.releaseRunnable = releaseRunnable;
    }

    @Override
    public ReadBufferResult readBuffer(
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId,
            int segmentId,
            int bufferIndex,
            MemorySegment memorySegment,
            BufferRecycler recycler,
            @Nullable ReadProgress readProgress,
            @Nullable CompositeBuffer partialBuffer)
            throws IOException {
        return readBufferFunction.apply(bufferIndex, segmentId);
    }

    @Override
    public long getPriority(
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId,
            int segmentId,
            int bufferIndex,
            @Nullable ReadProgress readProgress) {
        return getPriorityFunction.apply(subpartitionId.getSubpartitionId());
    }

    @Override
    public void release() {
        releaseRunnable.run();
    }

    /** Builder for {@link TestingPartitionFileReader}. */
    public static class Builder {
        private BiFunction<Integer, Integer, ReadBufferResult> readBufferSupplier =
                (bufferIndex, segmentId) -> null;

        private Function<Integer, Long> prioritySupplier = bufferIndex -> 0L;

        private Runnable releaseNotifier = () -> {};

        public Builder setReadBufferSupplier(
                BiFunction<Integer, Integer, ReadBufferResult> readBufferSupplier) {
            this.readBufferSupplier = readBufferSupplier;
            return this;
        }

        public Builder setPrioritySupplier(Function<Integer, Long> prioritySupplier) {
            this.prioritySupplier = prioritySupplier;
            return this;
        }

        public Builder setReleaseNotifier(Runnable releaseNotifier) {
            this.releaseNotifier = releaseNotifier;
            return this;
        }

        public TestingPartitionFileReader build() {
            return new TestingPartitionFileReader(
                    readBufferSupplier, prioritySupplier, releaseNotifier);
        }
    }
}
