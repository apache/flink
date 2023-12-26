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

import org.apache.flink.runtime.io.network.partition.hybrid.index.FileDataIndexRegionHelper;
import org.apache.flink.runtime.io.network.partition.hybrid.index.FileDataIndexSpilledRegionManager;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.function.BiConsumer;

/** Mock {@link FileDataIndexSpilledRegionManager} for testing. */
public class TestingFileDataIndexSpilledRegionManager<T extends FileDataIndexRegionHelper.Region>
        implements FileDataIndexSpilledRegionManager<T> {
    private final List<TreeMap<Integer, T>> regions;

    private final BiConsumer<Integer, T> cacheRegionConsumer;

    private int findRegionInvoked = 0;

    public TestingFileDataIndexSpilledRegionManager(
            int numSubpartitions, BiConsumer<Integer, T> cacheRegionConsumer) {
        this.regions = new ArrayList<>();
        this.cacheRegionConsumer = cacheRegionConsumer;
        for (int i = 0; i < numSubpartitions; i++) {
            regions.add(new TreeMap<>());
        }
    }

    @Nullable
    public T getRegion(int subpartition, int bufferIndex) {
        return regions.get(subpartition).get(bufferIndex);
    }

    public int getSpilledRegionSize(int subpartition) {
        return regions.get(subpartition).size();
    }

    public int getFindRegionInvoked() {
        return findRegionInvoked;
    }

    @Override
    public void appendOrOverwriteRegion(int subpartition, T region) {
        regions.get(subpartition).put(region.getFirstBufferIndex(), region);
    }

    @Override
    public long findRegion(int subpartition, int bufferIndex, boolean loadToCache) {
        findRegionInvoked++;
        T region = regions.get(subpartition).get(bufferIndex);
        if (region == null) {
            return -1;
        } else {
            // return non -1 value indicates this region is exists.
            if (loadToCache) {
                cacheRegionConsumer.accept(subpartition, region);
            }
            return 1;
        }
    }

    @Override
    public void close() throws IOException {
        // do nothing.
    }

    /** Factory of {@link TestingFileDataIndexSpilledRegionManager}. */
    public static class Factory<T extends FileDataIndexRegionHelper.Region>
            implements FileDataIndexSpilledRegionManager.Factory<T> {

        public TestingFileDataIndexSpilledRegionManager<T> lastSpilledRegionManager;

        public TestingFileDataIndexSpilledRegionManager<T> getLastSpilledRegionManager() {
            return lastSpilledRegionManager;
        }

        @Override
        public FileDataIndexSpilledRegionManager<T> create(
                int numSubpartitions,
                Path indexFilePath,
                BiConsumer<Integer, T> cacheRegionConsumer) {
            TestingFileDataIndexSpilledRegionManager<T> testingFileDataIndexSpilledRegionManager =
                    new TestingFileDataIndexSpilledRegionManager<>(
                            numSubpartitions, cacheRegionConsumer);
            lastSpilledRegionManager = testingFileDataIndexSpilledRegionManager;
            return testingFileDataIndexSpilledRegionManager;
        }
    }
}
