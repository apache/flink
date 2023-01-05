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

import org.apache.flink.runtime.io.network.partition.hybrid.HsFileDataIndexImpl.InternalRegion;

import java.io.IOException;
import java.nio.file.Path;
import java.util.function.BiConsumer;

/** This class is responsible for spilling region to disk and managing these spilled regions. */
public interface HsFileDataIndexSpilledRegionManager extends AutoCloseable {
    /**
     * Write this region to index file. If target region already spilled, overwrite it.
     *
     * @param subpartition the subpartition id of this region.
     * @param region the region to be spilled to index file.
     */
    void appendOrOverwriteRegion(int subpartition, InternalRegion region) throws IOException;

    /**
     * Find the region contains target bufferIndex and belong to target subpartition.
     *
     * @param subpartition the subpartition id that target region belong to.
     * @param bufferIndex the buffer index that target region contains.
     * @param loadToCache whether to load the found region into the cache.
     * @return if target region can be founded, return it's offset in index file. Otherwise, return
     *     -1.
     */
    long findRegion(int subpartition, int bufferIndex, boolean loadToCache);

    /** Close this spilled region manager. */
    void close() throws IOException;

    /** Factory of {@link HsFileDataIndexSpilledRegionManager}. */
    interface Factory {
        HsFileDataIndexSpilledRegionManager create(
                int numSubpartitions,
                Path indexFilePath,
                int segmentSize,
                BiConsumer<Integer, InternalRegion> cacheRegionConsumer);
    }
}
