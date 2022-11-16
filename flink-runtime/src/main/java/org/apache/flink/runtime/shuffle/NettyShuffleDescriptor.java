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

package org.apache.flink.runtime.shuffle;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Optional;

/** Default implementation of {@link ShuffleDescriptor} for {@link NettyShuffleMaster}. */
public class NettyShuffleDescriptor implements ShuffleDescriptor {

    private static final long serialVersionUID = 852181945034989215L;

    private final ResourceID producerLocation;

    private final PartitionConnectionInfo partitionConnectionInfo;

    private final ResultPartitionID resultPartitionID;

    public NettyShuffleDescriptor(
            ResourceID producerLocation,
            PartitionConnectionInfo partitionConnectionInfo,
            ResultPartitionID resultPartitionID) {
        this.producerLocation = producerLocation;
        this.partitionConnectionInfo = partitionConnectionInfo;
        this.resultPartitionID = resultPartitionID;
    }

    public ConnectionID getConnectionId() {
        return new ConnectionID(
                producerLocation,
                partitionConnectionInfo.getAddress(),
                partitionConnectionInfo.getConnectionIndex());
    }

    @Override
    public ResultPartitionID getResultPartitionID() {
        return resultPartitionID;
    }

    @Override
    public Optional<ResourceID> storesLocalResourcesOn() {
        return Optional.of(producerLocation);
    }

    public boolean isLocalTo(ResourceID consumerLocation) {
        return producerLocation.equals(consumerLocation);
    }

    /** Information for connection to partition producer for shuffle exchange. */
    public interface PartitionConnectionInfo extends Serializable {
        InetSocketAddress getAddress();

        int getConnectionIndex();
    }

    /**
     * Remote partition connection information with index to query partition.
     *
     * <p>Normal connection information with network address and port for connection in case of
     * distributed execution.
     */
    public static class NetworkPartitionConnectionInfo implements PartitionConnectionInfo {

        private static final long serialVersionUID = 5992534320110743746L;

        private final InetSocketAddress address;

        private final int connectionIndex;

        @VisibleForTesting
        public NetworkPartitionConnectionInfo(InetSocketAddress address, int connectionIndex) {
            this.address = address;
            this.connectionIndex = connectionIndex;
        }

        public InetSocketAddress getAddress() {
            return address;
        }

        public int getConnectionIndex() {
            return connectionIndex;
        }

        static NetworkPartitionConnectionInfo fromProducerDescriptor(
                ProducerDescriptor producerDescriptor, int connectionIndex) {
            InetSocketAddress address =
                    new InetSocketAddress(
                            producerDescriptor.getAddress(), producerDescriptor.getDataPort());
            return new NetworkPartitionConnectionInfo(address, connectionIndex);
        }
    }

    /**
     * Local partition connection information.
     *
     * <p>Does not have any network connection information in case of local execution.
     */
    public enum LocalExecutionPartitionConnectionInfo implements PartitionConnectionInfo {
        INSTANCE;

        @Override
        public InetSocketAddress getAddress() {
            throw new UnsupportedOperationException(
                    "Local execution does not support shuffle connection.");
        }

        @Override
        public int getConnectionIndex() {
            throw new UnsupportedOperationException(
                    "Local execution does not support shuffle connection.");
        }
    }
}
