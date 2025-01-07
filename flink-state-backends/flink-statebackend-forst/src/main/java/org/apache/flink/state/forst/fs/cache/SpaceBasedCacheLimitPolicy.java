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
    private final long reservedSize;
    private final File instanceBasePath;
    private long usageSize;

    public SpaceBasedCacheLimitPolicy(
            File instanceBasePath, long reservedSize, long baseTargetFileSize) {
        this.reservedSize = reservedSize;
        this.instanceBasePath = instanceBasePath;
        this.usageSize = 0;
        long initFreeSpace = instanceBasePath.getFreeSpace();
        if (initFreeSpace < reservedSize || reservedSize < baseTargetFileSize) {
            LOG.warn(
                    "Illegal configuration of preserved space, current free space %s, reserve space %s "
                            + "and base targetFile size %s on instance base path %s.",
                    initFreeSpace, reservedSize, baseTargetFileSize, instanceBasePath);
        }
        LOG.info(
                "Creating SpaceBasedCacheLimitPolicy with initFreeSpace {} and preserved space {}",
                initFreeSpace,
                reservedSize);
    }

    private boolean isOverSpace(long toAddSize, long leftSpace) {
        return toAddSize > instanceBasePath.getFreeSpace() - leftSpace;
    }

    @Override
    public boolean isSafeToAdd(long toAddSize) {
        return toAddSize < instanceBasePath.getFreeSpace() - reservedSize + usageSize;
    }

    @Override
    public boolean isOverflow(long toAddSize) {
        return isOverSpace(toAddSize, reservedSize);
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
