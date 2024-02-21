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

import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/** Testing implementation for {@link ProducerMergedPartitionFileIndex}. */
public class TestingProducerMergedPartitionFileIndex extends ProducerMergedPartitionFileIndex {

    private final Consumer<List<FlushedBuffer>> addBuffersConsumer;

    private final BiFunction<TieredStorageSubpartitionId, Integer, Optional<FixedSizeRegion>>
            getRegionFunction;

    private final Runnable releaseRunnable;

    private TestingProducerMergedPartitionFileIndex(
            int numSubpartitions,
            Path indexFilePath,
            int regionGroupSizeInBytes,
            long numRetainedInMemoryRegionsMax,
            Consumer<List<FlushedBuffer>> addBuffersConsumer,
            BiFunction<TieredStorageSubpartitionId, Integer, Optional<FixedSizeRegion>>
                    getRegionFunction,
            Runnable releaseRunnable) {
        super(
                numSubpartitions,
                indexFilePath,
                regionGroupSizeInBytes,
                numRetainedInMemoryRegionsMax);
        this.addBuffersConsumer = addBuffersConsumer;
        this.getRegionFunction = getRegionFunction;
        this.releaseRunnable = releaseRunnable;
    }

    @Override
    void addBuffers(List<FlushedBuffer> buffers) {
        addBuffersConsumer.accept(buffers);
    }

    @Override
    Optional<FixedSizeRegion> getRegion(
            TieredStorageSubpartitionId subpartitionId, int bufferIndex) {
        return getRegionFunction.apply(subpartitionId, bufferIndex);
    }

    @Override
    void release() {
        releaseRunnable.run();
    }

    /** Builder for {@link TestingProducerMergedPartitionFileIndex}. */
    public static class Builder {

        private int numSubpartitions = 1;

        private Path indexFilePath = null;

        private int regionGroupSizeInBytes = 256;

        private long numRetainedInMemoryRegionsMax = Long.MAX_VALUE;

        private Consumer<List<FlushedBuffer>> addBuffersConsumer = flushedBuffers -> {};

        private BiFunction<TieredStorageSubpartitionId, Integer, Optional<FixedSizeRegion>>
                getRegionFunction = (tieredStorageSubpartitionId, integer) -> Optional.empty();

        private Runnable releaseRunnable = () -> {};

        public Builder() {}

        public TestingProducerMergedPartitionFileIndex.Builder setNumSubpartitions(
                int numSubpartitions) {
            this.numSubpartitions = numSubpartitions;
            return this;
        }

        public TestingProducerMergedPartitionFileIndex.Builder setIndexFilePath(
                Path indexFilePath) {
            this.indexFilePath = indexFilePath;
            return this;
        }

        public TestingProducerMergedPartitionFileIndex.Builder setRegionGroupSizeInBytes(
                int regionGroupSizeInBytes) {
            this.regionGroupSizeInBytes = regionGroupSizeInBytes;
            return this;
        }

        public TestingProducerMergedPartitionFileIndex.Builder setNumRetainedInMemoryRegionsMax(
                long numRetainedInMemoryRegionsMax) {
            this.numRetainedInMemoryRegionsMax = numRetainedInMemoryRegionsMax;
            return this;
        }

        public TestingProducerMergedPartitionFileIndex.Builder setAddBuffersConsumer(
                Consumer<List<FlushedBuffer>> addBuffersConsumer) {
            this.addBuffersConsumer = addBuffersConsumer;
            return this;
        }

        public TestingProducerMergedPartitionFileIndex.Builder setGetRegionFunction(
                BiFunction<TieredStorageSubpartitionId, Integer, Optional<FixedSizeRegion>>
                        getRegionFunction) {
            this.getRegionFunction = getRegionFunction;
            return this;
        }

        public TestingProducerMergedPartitionFileIndex.Builder setReleaseRunnable(
                Runnable releaseRunnable) {
            this.releaseRunnable = releaseRunnable;
            return this;
        }

        public TestingProducerMergedPartitionFileIndex build() {
            return new TestingProducerMergedPartitionFileIndex(
                    numSubpartitions,
                    indexFilePath,
                    regionGroupSizeInBytes,
                    numRetainedInMemoryRegionsMax,
                    addBuffersConsumer,
                    getRegionFunction,
                    releaseRunnable);
        }
    }
}
