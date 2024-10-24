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
 * A system space checker that leverages the system to check the free space dynamically, the free
 * space may fluctuate in size due to other components.
 */
public class SystemSpaceChecker implements CacheLimiter {

    private static final Logger LOG = LoggerFactory.getLogger(SystemSpaceChecker.class);
    private final long capacity;
    private final long reservedSize;
    private final File instanceBasePath;
    private long usageResource;

    public SystemSpaceChecker(
            File instanceBasePath, long reservedSize, long baseTargetFileSize, long capacity) {
        this.capacity = capacity;
        this.reservedSize = reservedSize;
        this.instanceBasePath = instanceBasePath;
        long initFreeSpace = instanceBasePath.getFreeSpace();
        long usageResource = 0;
        if (initFreeSpace < reservedSize || reservedSize < baseTargetFileSize) {

            LOG.warn(
                    "Illegal configuration of preserved space, current free space %s, reserve space %s "
                            + "and base targetFile size %s on instance base path %s.",
                    initFreeSpace, reservedSize, baseTargetFileSize, instanceBasePath);
        }
        LOG.info(
                "Creating os file space checker with initFreeSpace {} and preserved space {}",
                initFreeSpace,
                reservedSize);
    }

    private boolean isOverSpace(long toAddSize, long leftSpace) {
        return instanceBasePath.getFreeSpace() - toAddSize < leftSpace
                && usageResource + toAddSize < capacity - leftSpace;
    }

    @Override
    public boolean isOverflow(long toAddSize) {
        return isOverSpace(toAddSize, reservedSize);
    }

    @Override
    public boolean acquire(long toAddSize) {
        if (isOverSpace(toAddSize, reservedSize)) {
            return false;
        }
        usageResource += toAddSize;
        return true;
    }

    @Override
    public void release(long toReleaseSize) {
        usageResource -= Math.min(usageResource, toReleaseSize);
    }
}
