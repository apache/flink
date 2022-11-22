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

import java.util.Deque;
import java.util.List;

/** This component is responsible for providing some information needed for the spill decision. */
public interface HsSpillingInfoProvider {
    /**
     * Get the number of downstream consumers.
     *
     * @return Number of subpartitions.
     */
    int getNumSubpartitions();

    /**
     * Get all subpartition's next buffer index to consume of specific consumer.
     *
     * @param consumerId of the target downstream consumer.
     * @return A list containing all subpartition's next buffer index to consume of specific
     *     consumer, if the downstream subpartition view has not been registered, the corresponding
     *     return value is -1.
     */
    List<Integer> getNextBufferIndexToConsume(HsConsumerId consumerId);

    /**
     * Get all buffers with the expected status from the subpartition.
     *
     * @param subpartitionId target buffers belong to.
     * @param spillStatus expected buffer spill status.
     * @param consumeStatusWithId expected buffer consume status and consumer id.
     * @return all buffers satisfy specific status of this subpartition, This queue must be sorted
     *     according to bufferIndex from small to large, in other words, head is the buffer with the
     *     minimum bufferIndex in the current subpartition.
     */
    Deque<BufferIndexAndChannel> getBuffersInOrder(
            int subpartitionId, SpillStatus spillStatus, ConsumeStatusWithId consumeStatusWithId);

    /** Get total number of not decided to spill buffers. */
    int getNumTotalUnSpillBuffers();

    /** Get total number of buffers requested from buffer pool. */
    int getNumTotalRequestedBuffers();

    /** Get the current size of buffer pool. */
    int getPoolSize();

    /** This enum represents buffer status of spill in hybrid shuffle mode. */
    enum SpillStatus {
        /** The buffer is spilling or spilled already. */
        SPILL,
        /** The buffer not start spilling. */
        NOT_SPILL,
        /** The buffer is either spill or not spill. */
        ALL
    }

    /** This enum represents buffer status of consume in hybrid shuffle mode. */
    enum ConsumeStatus {
        /** The buffer has already consumed by downstream. */
        CONSUMED,
        /** The buffer is not consumed by downstream. */
        NOT_CONSUMED,
        /** The buffer is either consumed or not consumed. */
        ALL
    }

    /** This class represents a pair of {@link ConsumeStatus} and consumer id. */
    class ConsumeStatusWithId {
        public static final ConsumeStatusWithId ALL_ANY =
                new ConsumeStatusWithId(ConsumeStatus.ALL, HsConsumerId.ANY);

        ConsumeStatus status;

        HsConsumerId consumerId;

        private ConsumeStatusWithId(ConsumeStatus status, HsConsumerId consumerId) {
            this.status = status;
            this.consumerId = consumerId;
        }

        public static ConsumeStatusWithId fromStatusAndConsumerId(
                ConsumeStatus consumeStatus, HsConsumerId consumerId) {
            return new ConsumeStatusWithId(consumeStatus, consumerId);
        }
    }
}
