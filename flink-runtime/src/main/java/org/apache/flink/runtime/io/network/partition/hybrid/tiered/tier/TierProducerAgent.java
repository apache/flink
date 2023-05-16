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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;

import java.io.IOException;

/** The producer-side agent of a Tier. */
public interface TierProducerAgent {

    /**
     * Try to start a new segment in the Tier.
     *
     * @param subpartitionId subpartition id that the new segment belongs to
     * @param segmentId id of the new segment
     * @return true if the segment can be started, false otherwise.
     */
    boolean tryStartNewSegment(TieredStorageSubpartitionId subpartitionId, int segmentId);

    /** Writes the finished {@link Buffer} to the consumer. */
    boolean write(TieredStorageSubpartitionId subpartitionId, Buffer finishedBuffer)
            throws IOException;

    /**
     * Close the agent.
     *
     * <p>Note this only releases resources directly hold by the agent, which excludes resources
     * managed by the resource registry.
     */
    void close();
}
