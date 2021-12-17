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

package org.apache.flink.runtime.scheduler.adaptivebatch;

import java.util.List;

/** The blocking result info, which will be used to calculate the vertex parallelism. */
public class BlockingResultInfo {

    private final List<Long> blockingPartitionSizes;

    private final boolean isBroadcast;

    private BlockingResultInfo(List<Long> blockingPartitionSizes, boolean isBroadcast) {
        this.blockingPartitionSizes = blockingPartitionSizes;
        this.isBroadcast = isBroadcast;
    }

    public List<Long> getBlockingPartitionSizes() {
        return blockingPartitionSizes;
    }

    public boolean isBroadcast() {
        return isBroadcast;
    }

    public static BlockingResultInfo createFromBroadcastResult(List<Long> blockingPartitionSizes) {
        return new BlockingResultInfo(blockingPartitionSizes, true);
    }

    public static BlockingResultInfo createFromNonBroadcastResult(
            List<Long> blockingPartitionSizes) {
        return new BlockingResultInfo(blockingPartitionSizes, false);
    }
}
