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

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link HeartbeatServices} implementation for testing purposes. This implementation is able to
 * trigger a timeout of specific component manually.
 */
public class TestingHeartbeatServices extends HeartbeatServices {

    private static final long DEFAULT_HEARTBEAT_TIMEOUT = 10000L;

    private static final long DEFAULT_HEARTBEAT_INTERVAL = 1000L;

    private final Map<ResourceID, Collection<HeartbeatManagerImpl>> heartbeatManagers =
            new ConcurrentHashMap<>();

    private final Map<ResourceID, Collection<HeartbeatManagerSenderImpl>> heartbeatManagerSenders =
            new ConcurrentHashMap<>();

    public TestingHeartbeatServices() {
        super(DEFAULT_HEARTBEAT_INTERVAL, DEFAULT_HEARTBEAT_TIMEOUT);
    }

    public TestingHeartbeatServices(long heartbeatInterval) {
        super(heartbeatInterval, DEFAULT_HEARTBEAT_TIMEOUT);
    }

    public TestingHeartbeatServices(long heartbeatInterval, long heartbeatTimeout) {
        super(heartbeatInterval, heartbeatTimeout);
    }

    @Override
    public <I, O> HeartbeatManager<I, O> createHeartbeatManager(
            ResourceID resourceId,
            HeartbeatListener<I, O> heartbeatListener,
            ScheduledExecutor mainThreadExecutor,
            Logger log) {

        HeartbeatManagerImpl<I, O> heartbeatManager =
                new HeartbeatManagerImpl<>(
                        heartbeatTimeout,
                        resourceId,
                        heartbeatListener,
                        mainThreadExecutor,
                        log,
                        new TestingHeartbeatMonitorFactory<>());

        heartbeatManagers.compute(
                resourceId,
                (resourceID, heartbeatManagers) -> {
                    final Collection<HeartbeatManagerImpl> result;

                    if (heartbeatManagers != null) {
                        result = heartbeatManagers;
                    } else {
                        result = new ArrayList<>();
                    }

                    result.add(heartbeatManager);
                    return result;
                });

        return heartbeatManager;
    }

    @Override
    public <I, O> HeartbeatManager<I, O> createHeartbeatManagerSender(
            ResourceID resourceId,
            HeartbeatListener<I, O> heartbeatListener,
            ScheduledExecutor mainThreadExecutor,
            Logger log) {

        HeartbeatManagerSenderImpl<I, O> heartbeatManager =
                new HeartbeatManagerSenderImpl<>(
                        heartbeatInterval,
                        heartbeatTimeout,
                        resourceId,
                        heartbeatListener,
                        mainThreadExecutor,
                        log,
                        new TestingHeartbeatMonitorFactory<>());

        heartbeatManagerSenders.compute(
                resourceId,
                (resourceID, heartbeatManagers) -> {
                    final Collection<HeartbeatManagerSenderImpl> result;

                    if (heartbeatManagers != null) {
                        result = heartbeatManagers;
                    } else {
                        result = new ArrayList<>();
                    }

                    result.add(heartbeatManager);
                    return result;
                });

        return heartbeatManager;
    }

    public void triggerHeartbeatTimeout(ResourceID managerResourceId, ResourceID targetResourceId) {

        boolean triggered = false;
        Collection<HeartbeatManagerImpl> heartbeatManagerList =
                heartbeatManagers.get(managerResourceId);
        if (heartbeatManagerList != null) {
            for (HeartbeatManagerImpl heartbeatManager : heartbeatManagerList) {
                final TestingHeartbeatMonitor monitor =
                        (TestingHeartbeatMonitor)
                                heartbeatManager.getHeartbeatTargets().get(targetResourceId);
                if (monitor != null) {
                    monitor.triggerHeartbeatTimeout();
                    triggered = true;
                }
            }
        }

        final Collection<HeartbeatManagerSenderImpl> heartbeatManagerSenderList =
                this.heartbeatManagerSenders.get(managerResourceId);
        if (heartbeatManagerSenderList != null) {
            for (HeartbeatManagerSenderImpl heartbeatManagerSender : heartbeatManagerSenderList) {
                final TestingHeartbeatMonitor monitor =
                        (TestingHeartbeatMonitor)
                                heartbeatManagerSender.getHeartbeatTargets().get(targetResourceId);
                if (monitor != null) {
                    monitor.triggerHeartbeatTimeout();
                    triggered = true;
                }
            }
        }

        checkState(
                triggered,
                "There is no target "
                        + targetResourceId
                        + " monitored under Heartbeat manager "
                        + managerResourceId);
    }

    /**
     * Factory instantiates testing monitor instance.
     *
     * @param <O> Type of the outgoing heartbeat payload
     */
    static class TestingHeartbeatMonitorFactory<O> implements HeartbeatMonitor.Factory<O> {

        @Override
        public HeartbeatMonitor<O> createHeartbeatMonitor(
                ResourceID resourceID,
                HeartbeatTarget<O> heartbeatTarget,
                ScheduledExecutor mainThreadExecutor,
                HeartbeatListener<?, O> heartbeatListener,
                long heartbeatTimeoutIntervalMs) {

            return new TestingHeartbeatMonitor<>(
                    resourceID,
                    heartbeatTarget,
                    mainThreadExecutor,
                    heartbeatListener,
                    heartbeatTimeoutIntervalMs);
        }
    }

    /**
     * A heartbeat monitor for testing which supports triggering timeout manually.
     *
     * @param <O> Type of the outgoing heartbeat payload
     */
    static class TestingHeartbeatMonitor<O> extends HeartbeatMonitorImpl<O> {

        private volatile boolean timeoutTriggered = false;

        TestingHeartbeatMonitor(
                ResourceID resourceID,
                HeartbeatTarget<O> heartbeatTarget,
                ScheduledExecutor scheduledExecutor,
                HeartbeatListener<?, O> heartbeatListener,
                long heartbeatTimeoutIntervalMs) {

            super(
                    resourceID,
                    heartbeatTarget,
                    scheduledExecutor,
                    heartbeatListener,
                    heartbeatTimeoutIntervalMs);
        }

        @Override
        public void reportHeartbeat() {
            if (!timeoutTriggered) {
                super.reportHeartbeat();
            }
            // just swallow the heartbeat report
        }

        @Override
        void resetHeartbeatTimeout(long heartbeatTimeout) {
            synchronized (this) {
                if (timeoutTriggered) {
                    super.resetHeartbeatTimeout(0);
                } else {
                    super.resetHeartbeatTimeout(heartbeatTimeout);
                }
            }
        }

        void triggerHeartbeatTimeout() {
            synchronized (this) {
                timeoutTriggered = true;
                resetHeartbeatTimeout(0);
            }
        }
    }
}
