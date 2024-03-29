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
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionIndexSet;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;

/**
 * A dummy implementation of the {@link PartitionRequestClient} instance which is mainly used for
 * tests to avoid mock.
 */
public class TestingPartitionRequestClient implements PartitionRequestClient {

    @Override
    public void requestSubpartition(
            ResultPartitionID partitionId,
            ResultSubpartitionIndexSet subpartitionIndexSet,
            RemoteInputChannel inputChannel,
            int delayMs) {}

    @Override
    public void notifyCreditAvailable(RemoteInputChannel inputChannel) {}

    @Override
    public void notifyNewBufferSize(RemoteInputChannel inputChannel, int bufferSize) {}

    @Override
    public void resumeConsumption(RemoteInputChannel inputChannel) {}

    @Override
    public void acknowledgeAllRecordsProcessed(RemoteInputChannel inputChannel) {}

    @Override
    public void notifyRequiredSegmentId(
            RemoteInputChannel inputChannel, int subpartitionIndex, int segmentId) {}

    @Override
    public void sendTaskEvent(
            ResultPartitionID partitionId, TaskEvent event, RemoteInputChannel inputChannel) {}

    @Override
    public void close(RemoteInputChannel inputChannel) {}
}
