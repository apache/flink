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

package org.apache.flink.runtime.io.network;

import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;

import java.io.IOException;

/** Client to send messages or task events via network for {@link RemoteInputChannel}. */
public interface PartitionRequestClient {

    /**
     * Requests a remote sub partition.
     *
     * @param partitionId The identifier of result partition to be requested.
     * @param subpartitionIndex The sub partition index in the requested result partition.
     * @param inputChannel The remote input channel for requesting the sub partition.
     * @param delayMs The request is scheduled within a delay time.
     */
    void requestSubpartition(
            ResultPartitionID partitionId,
            int subpartitionIndex,
            RemoteInputChannel inputChannel,
            int delayMs)
            throws IOException;

    /**
     * Notifies available credits from one remote input channel.
     *
     * @param inputChannel The remote input channel who announces the available credits.
     */
    void notifyCreditAvailable(RemoteInputChannel inputChannel);

    /**
     * Notifies new buffer size from one remote input channel.
     *
     * @param inputChannel The remote input channel who announces the new buffer size.
     * @param bufferSize The new buffer size.
     */
    void notifyNewBufferSize(RemoteInputChannel inputChannel, int bufferSize);

    /**
     * Requests to resume data consumption from one remote input channel.
     *
     * @param inputChannel The remote input channel who is ready to resume data consumption.
     */
    void resumeConsumption(RemoteInputChannel inputChannel);

    /**
     * Acknowledges all user records are processed for this channel.
     *
     * @param inputChannel The input channel to resume data consumption.
     */
    void acknowledgeAllRecordsProcessed(RemoteInputChannel inputChannel);

    /**
     * Sends a task event backwards to an intermediate result partition.
     *
     * @param partitionId The identifier of result partition.
     * @param event The task event to be sent.
     * @param inputChannel The remote input channel for sending this event.
     */
    void sendTaskEvent(
            ResultPartitionID partitionId, TaskEvent event, RemoteInputChannel inputChannel)
            throws IOException;

    /**
     * Cancels the partition request for the given remote input channel and removes this client from
     * factory if it is not referenced by any other input channels.
     *
     * @param inputChannel The remote input channel for canceling partition and to be removed from
     *     network stack.
     */
    void close(RemoteInputChannel inputChannel) throws IOException;
}
