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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.core.testutils.EachCallbackWrapper;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.zookeeper.ZooKeeperLeaderElectionHaServices;
import org.apache.flink.runtime.jobmaster.JobMaster;
import org.apache.flink.runtime.leaderelection.LeaderElection;
import org.apache.flink.runtime.leaderelection.LeaderInformation;
import org.apache.flink.runtime.leaderelection.TestingContender;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionListener;
import org.apache.flink.runtime.leaderelection.ZooKeeperLeaderElectionDriver;
import org.apache.flink.runtime.rpc.AddressResolution;
import org.apache.flink.runtime.rpc.RpcSystem;
import org.apache.flink.runtime.util.LeaderRetrievalUtils;
import org.apache.flink.runtime.util.TestingFatalErrorHandlerExtension;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.runtime.zookeeper.ZooKeeperExtension;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the ZooKeeper based leader election and retrieval. */
class ZooKeeperLeaderRetrievalTest {

    private static final RpcSystem RPC_SYSTEM = RpcSystem.load();

    @RegisterExtension
    private static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

    private final ZooKeeperExtension zooKeeperExtension = new ZooKeeperExtension();

    @RegisterExtension
    private final EachCallbackWrapper<ZooKeeperExtension> eachWrapper =
            new EachCallbackWrapper<>(zooKeeperExtension);

    @RegisterExtension
    private final TestingFatalErrorHandlerExtension testingFatalErrorHandlerResource =
            new TestingFatalErrorHandlerExtension();

    private Configuration config;

    private HighAvailabilityServices highAvailabilityServices;

    @BeforeEach
    void before() throws Exception {
        config = new Configuration();
        config.setString(HighAvailabilityOptions.HA_MODE, "zookeeper");
        config.setString(
                HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, zooKeeperExtension.getConnectString());

        highAvailabilityServices =
                new ZooKeeperLeaderElectionHaServices(
                        ZooKeeperUtils.startCuratorFramework(
                                config,
                                testingFatalErrorHandlerResource.getTestingFatalErrorHandler()),
                        config,
                        EXECUTOR_RESOURCE.getExecutor(),
                        new VoidBlobStore());
    }

    @AfterEach
    void after() throws Exception {
        if (highAvailabilityServices != null) {
            highAvailabilityServices.closeWithOptionalClean(true);
            highAvailabilityServices = null;
        }
    }

    /**
     * Tests that LeaderRetrievalUtils.findConnectingAddress finds the correct connecting address in
     * case of an old leader address in ZooKeeper and a subsequent election of a new leader. The
     * findConnectingAddress should block until the new leader has been elected and his address has
     * been written to ZooKeeper.
     */
    @Test
    void testConnectingAddressRetrievalWithDelayedLeaderElection() throws Exception {
        Duration timeout = Duration.ofMinutes(1L);

        long sleepingTime = 1000;

        LeaderElection leaderElection = null;

        Thread thread;

        final InetAddress localHost;
        try {
            localHost = InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
            // may happen if disconnected. skip test.
            System.err.println("Skipping 'testNetworkInterfaceSelection' test.");
            return;
        }

        try (ServerSocket serverSocket = new ServerSocket(0, 50, localHost)) {
            String wrongAddress =
                    RPC_SYSTEM.getRpcUrl(
                            "1.1.1.1",
                            1234,
                            "foobar",
                            AddressResolution.NO_ADDRESS_RESOLUTION,
                            config);

            final TestingLeaderElectionListener listener = new TestingLeaderElectionListener();
            try {
                InetSocketAddress correctInetSocketAddress =
                        new InetSocketAddress(localHost, serverSocket.getLocalPort());

                String correctAddress =
                        RPC_SYSTEM.getRpcUrl(
                                localHost.getHostName(),
                                correctInetSocketAddress.getPort(),
                                JobMaster.JOB_MANAGER_NAME,
                                AddressResolution.NO_ADDRESS_RESOLUTION,
                                config);

                // create driver to simulate a separate Flink process having leadership that writes
                // its leader information to the ZooKeeper backend and gets lost afterward
                final ZooKeeperLeaderElectionDriver externalProcessDriver =
                        new ZooKeeperLeaderElectionDriver(
                                ZooKeeperUtils.useNamespaceAndEnsurePath(
                                        zooKeeperExtension.getZooKeeperClient(
                                                testingFatalErrorHandlerResource
                                                        .getTestingFatalErrorHandler()),
                                        ZooKeeperUtils.generateLeaderLatchPath("")),
                                listener);
                externalProcessDriver.isLeader();

                externalProcessDriver.publishLeaderInformation(
                        HighAvailabilityServices.DEFAULT_JOB_ID.toString(),
                        LeaderInformation.known(UUID.randomUUID(), wrongAddress));

                FindConnectingAddress findConnectingAddress =
                        new FindConnectingAddress(
                                timeout,
                                highAvailabilityServices.getJobManagerLeaderRetriever(
                                        HighAvailabilityServices.DEFAULT_JOB_ID,
                                        "unused-default-address"));

                thread = new Thread(findConnectingAddress);

                thread.start();

                leaderElection =
                        highAvailabilityServices.getJobManagerLeaderElection(
                                HighAvailabilityServices.DEFAULT_JOB_ID);
                TestingContender correctLeaderAddressContender =
                        new TestingContender(correctAddress, leaderElection);

                Thread.sleep(sleepingTime);

                externalProcessDriver.notLeader();
                externalProcessDriver.close();

                correctLeaderAddressContender.startLeaderElection();

                thread.join();

                InetAddress result = findConnectingAddress.getInetAddress();

                // check that we can connect to the localHost
                try (Socket socket = new Socket()) {
                    // port 0 = let the OS choose the port
                    SocketAddress bindP = new InetSocketAddress(result, 0);
                    // machine
                    socket.bind(bindP);
                    socket.connect(correctInetSocketAddress, 1000);
                }
            } finally {
                if (leaderElection != null) {
                    leaderElection.close();
                }
                listener.failIfErrorEventHappened();
            }
        } catch (IOException e) {
            // may happen in certain test setups, skip test.
            System.err.println("Skipping 'testNetworkInterfaceSelection' test.");
        }
    }

    /**
     * Tests that the LeaderRetrievalUtils.findConnectingAddress stops trying to find the connecting
     * address if no leader address has been specified. The call should return then
     * InetAddress.getLocalHost().
     */
    @Test
    void testTimeoutOfFindConnectingAddress() throws Exception {
        Duration timeout = Duration.ofSeconds(1L);

        LeaderRetrievalService leaderRetrievalService =
                highAvailabilityServices.getJobManagerLeaderRetriever(
                        HighAvailabilityServices.DEFAULT_JOB_ID, "unused-default-address");
        InetAddress result =
                LeaderRetrievalUtils.findConnectingAddress(
                        leaderRetrievalService, timeout, RPC_SYSTEM);

        assertThat(InetAddress.getLocalHost()).isEqualTo(result);
    }

    static class FindConnectingAddress implements Runnable {

        private final Duration timeout;
        private final LeaderRetrievalService leaderRetrievalService;

        private InetAddress result;
        private Exception exception;

        public FindConnectingAddress(
                Duration timeout, LeaderRetrievalService leaderRetrievalService) {
            this.timeout = timeout;
            this.leaderRetrievalService = leaderRetrievalService;
        }

        @Override
        public void run() {
            try {
                result =
                        LeaderRetrievalUtils.findConnectingAddress(
                                leaderRetrievalService, timeout, RPC_SYSTEM);
            } catch (Exception e) {
                exception = e;
            }
        }

        public InetAddress getInetAddress() throws Exception {
            if (exception != null) {
                throw exception;
            } else {
                return result;
            }
        }
    }
}
