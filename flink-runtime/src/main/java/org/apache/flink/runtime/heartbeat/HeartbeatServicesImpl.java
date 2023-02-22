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

package org.apache.flink.runtime.heartbeat;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.slf4j.Logger;

import static org.apache.flink.configuration.HeartbeatManagerOptions.FAILED_RPC_DETECTION_DISABLED;

/** A default {@link HeartbeatServices} implementation. */
public final class HeartbeatServicesImpl implements HeartbeatServices {

    /** Heartbeat interval for the created services. */
    private final long heartbeatInterval;

    /** Heartbeat timeout for the created services. */
    private final long heartbeatTimeout;

    private final int failedRpcRequestsUntilUnreachable;

    public HeartbeatServicesImpl(long heartbeatInterval, long heartbeatTimeout) {
        this(heartbeatInterval, heartbeatTimeout, FAILED_RPC_DETECTION_DISABLED);
    }

    public HeartbeatServicesImpl(
            long heartbeatInterval, long heartbeatTimeout, int failedRpcRequestsUntilUnreachable) {
        Preconditions.checkArgument(
                0L < heartbeatInterval, "The heartbeat interval must be larger than 0.");
        Preconditions.checkArgument(
                heartbeatInterval <= heartbeatTimeout,
                "The heartbeat timeout should be larger or equal than the heartbeat interval.");
        Preconditions.checkArgument(
                failedRpcRequestsUntilUnreachable > 0
                        || failedRpcRequestsUntilUnreachable == FAILED_RPC_DETECTION_DISABLED,
                "The number of failed heartbeat RPC requests has to be larger than 0 or -1 (deactivated).");

        this.heartbeatInterval = heartbeatInterval;
        this.heartbeatTimeout = heartbeatTimeout;
        this.failedRpcRequestsUntilUnreachable = failedRpcRequestsUntilUnreachable;
    }

    @Override
    public <I, O> HeartbeatManager<I, O> createHeartbeatManager(
            ResourceID resourceId,
            HeartbeatListener<I, O> heartbeatListener,
            ScheduledExecutor mainThreadExecutor,
            Logger log) {

        return new HeartbeatManagerImpl<>(
                heartbeatTimeout,
                failedRpcRequestsUntilUnreachable,
                resourceId,
                heartbeatListener,
                mainThreadExecutor,
                log);
    }

    @Override
    public <I, O> HeartbeatManager<I, O> createHeartbeatManagerSender(
            ResourceID resourceId,
            HeartbeatListener<I, O> heartbeatListener,
            ScheduledExecutor mainThreadExecutor,
            Logger log) {

        return new HeartbeatManagerSenderImpl<>(
                heartbeatInterval,
                heartbeatTimeout,
                failedRpcRequestsUntilUnreachable,
                resourceId,
                heartbeatListener,
                mainThreadExecutor,
                log);
    }
}
