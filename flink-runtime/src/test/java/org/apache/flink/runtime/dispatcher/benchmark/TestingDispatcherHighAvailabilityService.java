/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.dispatcher.benchmark;

import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.runtime.blob.BlobStoreService;
import org.apache.flink.runtime.blob.BlobUtils;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.JobResultStoreOptions;
import org.apache.flink.runtime.highavailability.nonha.standalone.StandaloneHaServices;
import org.apache.flink.runtime.highavailability.zookeeper.CuratorFrameworkWithUnhandledErrorListener;
import org.apache.flink.runtime.highavailability.zookeeper.ZooKeeperLeaderElectionHaServices;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.runtime.testutils.ZooKeeperTestUtils;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.runtime.util.ZooKeeperUtils;

import org.apache.curator.test.TestingServer;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.util.Preconditions.checkState;

/** Testing util class for {@link DispatcherHighAvailabilityServiceBenchmark}. */
interface TestingDispatcherHighAvailabilityService {

    HighAvailabilityServices highAvailabilityService();

    void close() throws Exception;

    static TestingDispatcherHighAvailabilityService fromConfiguration(
            Configuration configuration, File highAvailabilityPath) throws Exception {
        configuration.set(
                HighAvailabilityOptions.HA_STORAGE_PATH,
                createAndGetFile(highAvailabilityPath, "ha").toURI().toString());
        configuration.set(
                BlobServerOptions.STORAGE_DIRECTORY,
                createAndGetFile(highAvailabilityPath, "blob").toURI().toString());
        configuration.set(
                JobResultStoreOptions.STORAGE_PATH,
                createAndGetFile(highAvailabilityPath, "resultstore").toURI().toString());

        HighAvailabilityMode highAvailabilityMode = HighAvailabilityMode.fromConfig(configuration);
        if (highAvailabilityMode == HighAvailabilityMode.NONE) {
            return new DispatcherNoneHighAvailabilityService();
        } else if (highAvailabilityMode == HighAvailabilityMode.ZOOKEEPER) {
            return new DispatcherZookeeperHighAvailabilityService(configuration);
        } else {
            throw new UnsupportedOperationException(
                    "Only support none or zookeeper high availability service for benchmark.");
        }
    }

    static File createAndGetFile(File baseFile, String child) {
        File file = new File(baseFile, child);
        checkState(file.mkdir());
        return file;
    }

    /** None high availability service. */
    class DispatcherNoneHighAvailabilityService
            implements TestingDispatcherHighAvailabilityService {
        private final HighAvailabilityServices standaloneServices;

        private DispatcherNoneHighAvailabilityService() {
            String dispatcherAddress = "dispatcher";
            String resourceManagerAddress = "resourceManager";
            String webMonitorAddress = "webMonitor";
            standaloneServices =
                    new StandaloneHaServices(
                            resourceManagerAddress, dispatcherAddress, webMonitorAddress);
        }

        @Override
        public HighAvailabilityServices highAvailabilityService() {
            return standaloneServices;
        }

        @Override
        public void close() throws Exception {
            standaloneServices.close();
        }
    }

    /** Zookeeper high availability service. */
    class DispatcherZookeeperHighAvailabilityService
            implements TestingDispatcherHighAvailabilityService {
        private final TestingServer zooKeeperServer;
        private final ExecutorService executorService;

        private final BlobStoreService blobStoreService;
        private final CuratorFrameworkWithUnhandledErrorListener curatorFrameworkListener;
        private final HighAvailabilityServices zookeeperService;

        public DispatcherZookeeperHighAvailabilityService(Configuration configuration)
                throws Exception {
            this.executorService = Executors.newSingleThreadExecutor();
            this.zooKeeperServer = ZooKeeperTestUtils.createAndStartZookeeperTestingServer();
            this.zooKeeperServer.start();

            configuration.setString(
                    HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM,
                    zooKeeperServer.getConnectString());

            blobStoreService = BlobUtils.createBlobStoreFromConfig(configuration);
            curatorFrameworkListener =
                    ZooKeeperUtils.startCuratorFramework(
                            configuration, new TestingFatalErrorHandler());
            zookeeperService =
                    new ZooKeeperLeaderElectionHaServices(
                            curatorFrameworkListener,
                            configuration,
                            executorService,
                            blobStoreService);
        }

        @Override
        public HighAvailabilityServices highAvailabilityService() {
            return zookeeperService;
        }

        @Override
        public void close() throws IOException {
            zooKeeperServer.close();
            executorService.shutdown();

            blobStoreService.closeAndCleanupAllData();
            curatorFrameworkListener.close();
        }
    }
}
