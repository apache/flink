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
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.runtime.zookeeper.ZooKeeperResource;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.curator4.org.apache.curator.framework.CuratorFramework;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.state.ConnectionState;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.state.ConnectionStateListener;

import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.junit.Rule;
import org.junit.Test;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

/** Test behaviors of {@link ZooKeeperLeaderElectionDriver} on {@link ConnectionLossException} */
public class ZooKeeperLeaderElectionConnectionLossTest extends TestLogger {

    private static final String LATCH_PATH = "/latch";
    private static final String LEADER_PATH = "/leader";

    private static final Duration TIMEOUT = Duration.ofMillis(2000L);

    @Rule
    public final ZooKeeperResource zooKeeperResource = new ZooKeeperResource();

    @Test
    public void testKeepLeadershipOnConnectionLoss() throws Exception {
        final Configuration configuration = new Configuration();
        configuration.setString(
                HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM,
                zooKeeperResource.getConnectString());

        CuratorFramework client = ZooKeeperUtils.startCuratorFramework(configuration);
        LeaderElectionDriverFactory leaderElectionDriverFactory = new ZooKeeperLeaderElectionDriverFactory(
                client,
                LATCH_PATH,
                LEADER_PATH);
        DefaultLeaderElectionService leaderElectionService = new DefaultLeaderElectionService(
                leaderElectionDriverFactory);

        try {
            final OneShotLatch connectionLossLatch = new OneShotLatch();
            final OneShotLatch reconnectedLatch = new OneShotLatch();
            client
                    .getConnectionStateListenable()
                    .addListener(new TestingConnectionStateListener(
                            connectionLossLatch,
                            reconnectedLatch));

            final OneShotLatch grantLeadershipLatch = new OneShotLatch();
            final OneShotLatch revokeLeadershipLatch = new OneShotLatch();
            leaderElectionService.start(new TestingContender(
                    grantLeadershipLatch,
                    revokeLeadershipLatch));

            grantLeadershipLatch.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            zooKeeperResource.restart();
            connectionLossLatch.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            reconnectedLatch.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            assertFalse(revokeLeadershipLatch.isTriggered());
        } finally {
            leaderElectionService.stop();
            client.close();
        }
    }

    private static final class TestingContender implements LeaderContender {

        private final OneShotLatch grantLeadershipLatch;
        private final OneShotLatch revokeLeadershipLatch;

        public TestingContender(
                OneShotLatch grantLeadershipLatch,
                OneShotLatch revokeLeadershipLatch) {
            this.grantLeadershipLatch = grantLeadershipLatch;
            this.revokeLeadershipLatch = revokeLeadershipLatch;
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
            exception.printStackTrace();
            fail(exception.getMessage());
        }
    }

    private static final class TestingConnectionStateListener implements ConnectionStateListener {
        private final OneShotLatch connectionLossLatch;
        private final OneShotLatch reconnectedLatch;

        public TestingConnectionStateListener(
                OneShotLatch connectionLossLatch,
                OneShotLatch reconnectedLatch) {
            this.connectionLossLatch = connectionLossLatch;
            this.reconnectedLatch = reconnectedLatch;
        }

        @Override
        public void stateChanged(
                CuratorFramework curatorFramework,
                ConnectionState connectionState) {
            if (connectionState == ConnectionState.SUSPENDED) {
                connectionLossLatch.trigger();
            }

            if (connectionState == ConnectionState.RECONNECTED) {
                reconnectedLatch.trigger();
            }
        }

    }

}
