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

package org.apache.flink.runtime.util;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor.LocalExecutionPartitionConnectionInfo;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor.NetworkPartitionConnectionInfo;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import java.net.InetAddress;
import java.net.InetSocketAddress;

/** Builder to mock {@link NettyShuffleDescriptor} in tests. */
public class NettyShuffleDescriptorBuilder {
    private ResourceID producerLocation = ResourceID.generate();
    private ResultPartitionID id = new ResultPartitionID();
    private InetAddress address = InetAddress.getLoopbackAddress();
    private int dataPort;
    private int connectionIndex;

    public NettyShuffleDescriptorBuilder setProducerLocation(ResourceID producerLocation) {
        this.producerLocation = producerLocation;
        return this;
    }

    public NettyShuffleDescriptorBuilder setId(ResultPartitionID id) {
        this.id = id;
        return this;
    }

    public NettyShuffleDescriptorBuilder setAddress(InetAddress address) {
        this.address = address;
        return this;
    }

    public NettyShuffleDescriptorBuilder setDataPort(int dataPort) {
        this.dataPort = dataPort;
        return this;
    }

    public NettyShuffleDescriptorBuilder setProducerInfoFromTaskManagerLocation(
            TaskManagerLocation producerTaskManagerLocation) {
        return setProducerLocation(producerTaskManagerLocation.getResourceID())
                .setAddress(producerTaskManagerLocation.address())
                .setDataPort(producerTaskManagerLocation.dataPort());
    }

    public NettyShuffleDescriptorBuilder setConnectionIndex(int connectionIndex) {
        this.connectionIndex = connectionIndex;
        return this;
    }

    public NettyShuffleDescriptor buildRemote() {
        ConnectionID connectionID =
                new ConnectionID(new InetSocketAddress(address, dataPort), connectionIndex);
        return new NettyShuffleDescriptor(
                producerLocation, new NetworkPartitionConnectionInfo(connectionID), id);
    }

    public NettyShuffleDescriptor buildLocal() {
        return new NettyShuffleDescriptor(
                producerLocation, LocalExecutionPartitionConnectionInfo.INSTANCE, id);
    }

    public static NettyShuffleDescriptorBuilder newBuilder() {
        return new NettyShuffleDescriptorBuilder();
    }

    public static NettyShuffleDescriptor createRemoteWithIdAndLocation(
            IntermediateResultPartitionID partitionId, ResourceID producerLocation) {
        return newBuilder()
                .setId(new ResultPartitionID(partitionId, new ExecutionAttemptID()))
                .setProducerLocation(producerLocation)
                .buildRemote();
    }
}
