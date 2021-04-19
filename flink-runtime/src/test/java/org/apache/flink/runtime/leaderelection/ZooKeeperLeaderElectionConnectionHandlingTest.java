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

package org.apache.flink.runtime.leaderelection;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.util.TestingFatalErrorHandlerResource;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.runtime.zookeeper.ZooKeeperResource;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.curator4.org.apache.curator.framework.CuratorFramework;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.state.ConnectionState;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.state.ConnectionStateListener;

import org.junit.Rule;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertFalse;

/**
 * Test behaviors of {@link ZooKeeperLeaderElectionDriver} when losing the connection to ZooKeeper.
 */
public class ZooKeeperLeaderElectionConnectionHandlingTest extends TestLogger {

    private static final String PATH = "/path";

    @Rule public final ZooKeeperResource zooKeeperResource = new ZooKeeperResource();

    @Rule
    public final TestingFatalErrorHandlerResource fatalErrorHandlerResource =
            new TestingFatalErrorHandlerResource();

    @Test
    public void testKeepLeadershipOnConnectionSuspended() throws Exception {
        final Configuration configuration = new Configuration();
        configuration.setString(
                HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, zooKeeperResource.getConnectString());

        CuratorFramework client = ZooKeeperUtils.startCuratorFramework(configuration);
        LeaderElectionDriverFactory leaderElectionDriverFactory =
                new ZooKeeperLeaderElectionDriverFactory(client, PATH);
        DefaultLeaderElectionService leaderElectionService =
                new DefaultLeaderElectionService(leaderElectionDriverFactory);

        try {
            final TestingConnectionStateListener connectionStateListener =
                    new TestingConnectionStateListener();
            client.getConnectionStateListenable().addListener(connectionStateListener);

            final TestingContender contender = new TestingContender();
            leaderElectionService.start(contender);

            contender.awaitGrantLeadership();
            zooKeeperResource.restart();
            connectionStateListener.awaitSuspendedConnection();
            connectionStateListener.awaitReconnectedConnection();
            assertFalse(contender.hasRevokeLeadershipBeenTriggered());
        } finally {
            leaderElectionService.stop();
            client.close();
        }
    }

    private final class TestingContender implements LeaderContender {

        private final OneShotLatch grantLeadershipLatch;
        private final OneShotLatch revokeLeadershipLatch;

        private TestingContender() {
            this.grantLeadershipLatch = new OneShotLatch();
            this.revokeLeadershipLatch = new OneShotLatch();
        }

        @Override
        public void grantLeadership(UUID leaderSessionID) {
            grantLeadershipLatch.trigger();
        }

        @Override
        public void revokeLeadership() {
            revokeLeadershipLatch.trigger();
        }

        @Override
        public void handleError(Exception exception) {
            fatalErrorHandlerResource.getFatalErrorHandler().onFatalError(exception);
        }

        public void awaitGrantLeadership() throws InterruptedException {
            grantLeadershipLatch.await();
        }

        public boolean hasRevokeLeadershipBeenTriggered() {
            return revokeLeadershipLatch.isTriggered();
        }
    }

    private static final class TestingConnectionStateListener implements ConnectionStateListener {
        private final OneShotLatch connectionSuspendedLatch;
        private final OneShotLatch reconnectedLatch;

        public TestingConnectionStateListener() {
            this.connectionSuspendedLatch = new OneShotLatch();
            this.reconnectedLatch = new OneShotLatch();
        }

        @Override
        public void stateChanged(
                CuratorFramework curatorFramework, ConnectionState connectionState) {
            if (connectionState == ConnectionState.SUSPENDED) {
                connectionSuspendedLatch.trigger();
            }

            if (connectionState == ConnectionState.RECONNECTED) {
                reconnectedLatch.trigger();
            }
        }

        public void awaitSuspendedConnection() throws InterruptedException {
            connectionSuspendedLatch.await();
        }

        public void awaitReconnectedConnection() throws InterruptedException {
            reconnectedLatch.await();
        }
    }
}
