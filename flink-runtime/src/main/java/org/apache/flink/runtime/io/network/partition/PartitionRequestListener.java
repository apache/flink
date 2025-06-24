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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.runtime.io.network.NetworkSequenceViewReader;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;

import java.io.IOException;

/**
 * When the netty server receives a downstream task's partition request event and finds its upstream
 * task doesn't register its partition yet, the netty server will construct a {@link
 * PartitionRequestListener} and notify the listener when the task deploys itself and registers its
 * partition to {@link ResultPartitionManager}.
 */
public interface PartitionRequestListener {

    /**
     * The creation timestamp of this notifier, it's used to check whether the notifier is timeout.
     *
     * @return the creation timestamp
     */
    long getCreateTimestamp();

    /**
     * Get the result partition id of the notifier.
     *
     * @return the result partition id
     */
    ResultPartitionID getResultPartitionId();

    /**
     * Get the view reader of the notifier.
     *
     * @return the view reader
     */
    NetworkSequenceViewReader getViewReader();

    /**
     * Get the input channel id of the notifier.
     *
     * @return the input channel id
     */
    InputChannelID getReceiverId();

    /**
     * Notify the partition request listener when the given partition is registered.
     *
     * @param partition The registered partition.
     */
    void notifyPartitionCreated(ResultPartition partition) throws IOException;

    /**
     * When the partition request listener is timeout, it will be notified to send {@link
     * PartitionNotFoundException}.
     */
    void notifyPartitionCreatedTimeout();

    /** Release this listener. */
    void releaseListener();
}
