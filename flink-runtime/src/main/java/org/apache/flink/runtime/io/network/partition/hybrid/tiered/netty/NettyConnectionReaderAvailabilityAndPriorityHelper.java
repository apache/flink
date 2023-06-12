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

/**
 * {@link NettyConnectionReaderAvailabilityAndPriorityHelper} is used to help the reader notify the
 * available and priority status of {@link NettyConnectionReader}, and update the priority sequence
 * number of priority buffer.
 */
public interface NettyConnectionReaderAvailabilityAndPriorityHelper {

    /**
     * Notify the availability and priority status of {@link NettyConnectionReader}.
     *
     * @param channelIndex the index of input channel related to the connection.
     * @param isPriority the value is to indicate the priority of reader.
     */
    void notifyReaderAvailableAndPriority(int channelIndex, boolean isPriority);

    /**
     * Update the latest sequence number of priority buffer read by the {@link
     * NettyConnectionReader}, which helps determine if the reader should have priority.
     *
     * @param channelIndex the index of input channel related to the connection.
     * @param sequenceNumber the sequence number of priority buffer.
     */
    void updatePrioritySequenceNumber(int channelIndex, int sequenceNumber);
}
