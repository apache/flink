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

package org.apache.flink.state.forst.fs.cache;

import org.apache.flink.metrics.MetricGroup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * A space based cache limit policy that leverages the system to check the free space dynamically,
 * the free space may fluctuate in size due to other components. This class is not thread-safe, and
 * the thread safe should be ensured by the invoker.
 */
public class SpaceBasedCacheLimitPolicy implements CacheLimitPolicy {
    private static final Logger LOG = LoggerFactory.getLogger(SpaceBasedCacheLimitPolicy.class);

    /** The reserved size of space that should always be available. */
    private final long reservedSize;

    /** The base path of the instance where the cache is stored. */
    private final File instanceBasePath;

    /** The estimated size of the SST file. */
    private final long sstFileSize;

    /** The current usage size of the cache. */
    private long usageSize;

    public SpaceBasedCacheLimitPolicy(
            File instanceBasePath, long reservedSize, long baseTargetFileSize) {
        this.reservedSize = reservedSize;
        this.instanceBasePath = instanceBasePath;
        this.sstFileSize = baseTargetFileSize;
        this.usageSize = 0;
        long initFreeSpace = instanceBasePath.getFreeSpace();
        if (initFreeSpace < reservedSize || reservedSize < baseTargetFileSize) {
            LOG.warn(
                    "Illegal configuration of preserved space, current free space {}, reserve space {} "
                            + "and base targetFile size {} on instance base path {}.",
                    initFreeSpace,
                    reservedSize,
                    baseTargetFileSize,
                    instanceBasePath);
        }
        LOG.info(
                "Creating SpaceBasedCacheLimitPolicy with initFreeSpace {} and preserved space {}",
                initFreeSpace,
                reservedSize);
    }

    public static boolean worksOn(File instanceBasePath) {
        // We could detect the free space of the instance base path to determine whether the cache
        // limit policy works.
        return instanceBasePath.getFreeSpace() > 0;
    }

    private boolean isOverSpace(long toAddSize, long leftSpace) {
        return toAddSize > instanceBasePath.getFreeSpace() - leftSpace;
    }

    @Override
    public boolean directWriteInCache() {
        return isSafeToAdd(sstFileSize);
    }

    @Override
    public boolean isSafeToAdd(long toAddSize) {
        return toAddSize <= instanceBasePath.getFreeSpace() - reservedSize + usageSize;
    }

    @Override
    public boolean isOverflow(long toAddSize, boolean hasFile) {
        // hasFile = true means the file is already in cache, so the toAddSize is 0 considering the
        // reserved size.
        return isOverSpace(hasFile ? 0 : toAddSize, reservedSize);
    }

    @Override
    public void acquire(long toAddSize) {
        usageSize += toAddSize;
    }

    @Override
    public void release(long toReleaseSize) {
        usageSize -= Math.min(usageSize, toReleaseSize);
    }

    @Override
    public long usedBytes() {
        return usageSize;
    }

    @Override
    public void registerCustomizedMetrics(String prefix, MetricGroup metricGroup) {
        metricGroup.gauge(
                prefix + ".remainingBytes", () -> instanceBasePath.getFreeSpace() - reservedSize);
    }

    @Override
    public String toString() {
        return "SpaceBasedCacheLimitPolicy{"
                + "reservedSize="
                + reservedSize
                + ", instanceBasePath="
                + instanceBasePath
                + ", usageSize="
                + usageSize
                + '}';
    }
}
