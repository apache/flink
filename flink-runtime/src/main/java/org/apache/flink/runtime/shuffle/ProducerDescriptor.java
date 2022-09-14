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
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import java.net.InetAddress;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Partition producer descriptor for {@link ShuffleMaster} to obtain {@link ShuffleDescriptor}.
 *
 * <p>The producer descriptor contains general producer specific information relevant for the
 * shuffle service: the producer location as {@link ResourceID}, {@link ExecutionAttemptID} and the
 * network connection information for shuffle data exchange (address and port).
 */
public class ProducerDescriptor {
    /** The resource ID to identify the container where the producer execution is deployed. */
    private final ResourceID producerLocation;

    /** The ID of the producer execution attempt. */
    private final ExecutionAttemptID producerExecutionId;

    /** The address to connect to the producer. */
    private final InetAddress address;

    /**
     * The port to connect to the producer for shuffle exchange.
     *
     * <p>Negative value means local execution.
     */
    private final int dataPort;

    @VisibleForTesting
    public ProducerDescriptor(
            ResourceID producerLocation,
            ExecutionAttemptID producerExecutionId,
            InetAddress address,
            int dataPort) {
        this.producerLocation = checkNotNull(producerLocation);
        this.producerExecutionId = checkNotNull(producerExecutionId);
        this.address = checkNotNull(address);
        this.dataPort = dataPort;
    }

    public ResourceID getProducerLocation() {
        return producerLocation;
    }

    public ExecutionAttemptID getProducerExecutionId() {
        return producerExecutionId;
    }

    public InetAddress getAddress() {
        return address;
    }

    public int getDataPort() {
        return dataPort;
    }

    public static ProducerDescriptor create(
            TaskManagerLocation producerLocation, ExecutionAttemptID attemptId) {
        return new ProducerDescriptor(
                producerLocation.getResourceID(),
                attemptId,
                producerLocation.address(),
                producerLocation.dataPort());
    }
}
