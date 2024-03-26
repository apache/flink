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

package org.apache.flink.runtime.leaderretrieval;

import org.apache.flink.runtime.rpc.FatalErrorHandler;

import org.apache.flink.shaded.curator5.org.apache.curator.framework.CuratorFramework;

import java.util.concurrent.atomic.AtomicInteger;

/** {@link LeaderRetrievalDriverFactory} implementation for Zookeeper. */
public class ZooKeeperLeaderRetrievalDriverFactory implements LeaderRetrievalDriverFactory {

    private final CuratorFramework client;

    private final String retrievalPath;

    private final ZooKeeperLeaderRetrievalDriver.LeaderInformationClearancePolicy
            leaderInformationClearancePolicy;

    private final DriverReferenceCounter driverReferenceCounter = new DriverReferenceCounter();

    public ZooKeeperLeaderRetrievalDriverFactory(
            CuratorFramework client,
            String retrievalPath,
            ZooKeeperLeaderRetrievalDriver.LeaderInformationClearancePolicy
                    leaderInformationClearancePolicy) {
        this.client = client;
        this.retrievalPath = retrievalPath;
        this.leaderInformationClearancePolicy = leaderInformationClearancePolicy;
    }

    @Override
    public ZooKeeperLeaderRetrievalDriver createLeaderRetrievalDriver(
            LeaderRetrievalEventHandler leaderEventHandler, FatalErrorHandler fatalErrorHandler)
            throws Exception {
        driverReferenceCounter.incrementReferenceCounter();
        return new ZooKeeperLeaderRetrievalDriver(
                client,
                retrievalPath,
                leaderEventHandler,
                leaderInformationClearancePolicy,
                driverReferenceCounter,
                fatalErrorHandler);
    }

    /**
     * Each {@code ZooKeeperLeaderRetrievalDriver} has its own watcher initialized. There is a bug
     * in ZooKeeper where watcher threads are not properly cleaned up. No selective cleanup of
     * watcher threads is supported on the ZK server side (ZOOKEEPER-4625). This can lead to thread
     * leaks in test scenarios where ZooKeeper's TestServer is utilized within the VM (see
     * FLINK-33053).
     *
     * <p>Therefore, we need to maintain an external reference counter that is used to trigger the
     * release of all watchers if the reference count reaches 1.
     */
    private static class DriverReferenceCounter
            implements ZooKeeperLeaderRetrievalDriver.RemoveAllWatchers {

        private final AtomicInteger referenceCounter = new AtomicInteger(0);

        public void incrementReferenceCounter() {
            referenceCounter.incrementAndGet();
        }

        @Override
        public void closeCalled() {
            referenceCounter.decrementAndGet();
        }

        @Override
        public boolean shouldRemoveAllWatchers() {
            return referenceCounter.get() == 0;
        }
    }
}
