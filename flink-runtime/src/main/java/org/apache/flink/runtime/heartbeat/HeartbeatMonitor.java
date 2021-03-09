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
import org.apache.flink.runtime.concurrent.ScheduledExecutor;

/**
 * Heartbeat monitor which manages the heartbeat state of the associated heartbeat target. The
 * monitor notifies the {@link HeartbeatListener} whenever it has not seen a heartbeat signal in the
 * specified heartbeat timeout interval. Each heartbeat signal resets this timer.
 *
 * @param <O> Type of the payload being sent to the associated heartbeat target
 */
public interface HeartbeatMonitor<O> {

    /**
     * Gets heartbeat target.
     *
     * @return the heartbeat target
     */
    HeartbeatTarget<O> getHeartbeatTarget();

    /**
     * Gets heartbeat target id.
     *
     * @return the heartbeat target id
     */
    ResourceID getHeartbeatTargetId();

    /** Report heartbeat from the monitored target. */
    void reportHeartbeat();

    /** Cancel this monitor. */
    void cancel();

    /**
     * Gets the last heartbeat.
     *
     * @return the last heartbeat
     */
    long getLastHeartbeat();

    /**
     * This factory provides an indirection way to create {@link HeartbeatMonitor}.
     *
     * @param <O> Type of the outgoing heartbeat payload
     */
    interface Factory<O> {
        /**
         * Create heartbeat monitor heartbeat monitor.
         *
         * @param resourceID the resource id
         * @param heartbeatTarget the heartbeat target
         * @param mainThreadExecutor the main thread executor
         * @param heartbeatListener the heartbeat listener
         * @param heartbeatTimeoutIntervalMs the heartbeat timeout interval ms
         * @return the heartbeat monitor
         */
        HeartbeatMonitor<O> createHeartbeatMonitor(
                ResourceID resourceID,
                HeartbeatTarget<O> heartbeatTarget,
                ScheduledExecutor mainThreadExecutor,
                HeartbeatListener<?, O> heartbeatListener,
                long heartbeatTimeoutIntervalMs);
    }
}
