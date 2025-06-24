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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierConsumerAgent;

import java.io.IOException;
import java.util.Optional;

/** {@link NettyConnectionReader} is used by {@link TierConsumerAgent} to read buffer from netty. */
public interface NettyConnectionReader {
    /**
     * Returns the index of the subpartition where the next buffer locates, or -1 if there is no
     * buffer available and the subpartition to be consumed is not determined.
     */
    int peekNextBufferSubpartitionId() throws IOException;

    /**
     * Read a buffer from netty connection.
     *
     * @param subpartitionId the subpartition where the buffer locates.
     * @param segmentId segment id indicates the id of segment.
     * @return {@link Optional#empty()} will be returned if there is no buffer sent from netty
     *     connection otherwise a buffer will be returned.
     */
    Optional<Buffer> readBuffer(int subpartitionId, int segmentId);
}
