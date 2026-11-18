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

package org.apache.flink.runtime.execution.librarycache;

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.BlobStoreService;
import org.apache.flink.runtime.blob.BlobUtils;
import org.apache.flink.runtime.blob.PermanentBlobCache;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.util.FlinkUserCodeClassLoaders;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileInputStream;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.testutils.junit.utils.TempDirUtils.newFolder;
import static org.assertj.core.api.Assertions.assertThat;

/** Integration test for {@link BlobLibraryCacheManager}. */
@ExtendWith(ParameterizedTestExtension.class)
class BlobLibraryCacheRecoveryITCase {

    @TempDir private Path temporaryFolder;

    @Parameters(name = "Use system class loader: {0}")
    private static List<Boolean> useSystemClassLoader() {
        return List.of(true, false);
    }

    @Parameter private boolean wrapsSystemClassLoader;

    /**
     * Tests that with {@link HighAvailabilityMode#ZOOKEEPER} distributed JARs are recoverable from
     * any participating BlobLibraryCacheManager.
     */
    @TestTemplate
    void testRecoveryRegisterAndDownload() throws Exception {
        Random rand = new Random();

        BlobServer[] server = new BlobServer[2];
        InetSocketAddress[] serverAddress = new InetSocketAddress[2];
        BlobLibraryCacheManager[] libServer = new BlobLibraryCacheManager[2];
        PermanentBlobCache cache = null;
        BlobStoreService blobStoreService = null;

        Configuration config = new Configuration();
        config.set(HighAvailabilityOptions.HA_MODE, "ZOOKEEPER");
        config.set(
                HighAvailabilityOptions.HA_STORAGE_PATH,
                newFolder(temporaryFolder).getAbsolutePath());
        config.set(BlobServerOptions.CLEANUP_INTERVAL, 3_600L);

        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            blobStoreService = BlobUtils.createBlobStoreFromConfig(config);

            final BlobLibraryCacheManager.ClassLoaderFactory classLoaderFactory =
                    BlobLibraryCacheManager.defaultClassLoaderFactory(
                            FlinkUserCodeClassLoaders.ResolveOrder.CHILD_FIRST,
                            new String[0],
                            null,
                            true);

            for (int i = 0; i < server.length; i++) {
                server[i] = new BlobServer(config, newFolder(temporaryFolder), blobStoreService);
                server[i].start();
                serverAddress[i] = new InetSocketAddress("localhost", server[i].getPort());
                libServer[i] =
                        new BlobLibraryCacheManager(
                                server[i], classLoaderFactory, wrapsSystemClassLoader);
            }

            // Random data
            byte[] expected = new byte[1024];
            rand.nextBytes(expected);

            ArrayList<PermanentBlobKey> keys = new ArrayList<>(2);

            JobID jobId = new JobID();
            ApplicationID applicationId = new ApplicationID();

            // Upload some data (libraries)
            keys.add(server[0].putPermanent(jobId, expected)); // Request 1
            byte[] expected2 = Arrays.copyOfRange(expected, 32, 288);
            keys.add(server[0].putPermanent(jobId, expected2)); // Request 2

            // The cache
            cache =
                    new PermanentBlobCache(
                            config, newFolder(temporaryFolder), blobStoreService, serverAddress[0]);

            // Register uploaded libraries
            final LibraryCacheManager.ClassLoaderLease classLoaderLease =
                    libServer[0].registerClassLoaderLease(jobId, applicationId);
            classLoaderLease.getOrResolveClassLoader(keys, Collections.emptyList());

            // Verify key 1
            File f = cache.getFile(jobId, keys.get(0));
            assertThat(f).hasSize(expected.length);

            try (FileInputStream fis = new FileInputStream(f)) {
                for (int i = 0; i < expected.length && fis.available() > 0; i++) {
                    assertThat((byte) fis.read()).isEqualTo(expected[i]);
                }

                assertThat(fis.available()).isZero();
            }

            // Shutdown cache and start with other server
            cache.close();

            cache =
                    new PermanentBlobCache(
                            config, newFolder(temporaryFolder), blobStoreService, serverAddress[1]);

            // Verify key 1
            f = cache.getFile(jobId, keys.get(0));
            assertThat(f).hasSize(expected.length);

            try (FileInputStream fis = new FileInputStream(f)) {
                for (int i = 0; i < expected.length && fis.available() > 0; i++) {
                    assertThat((byte) fis.read()).isEqualTo(expected[i]);
                }

                assertThat(fis.available()).isZero();
            }

            // Verify key 2
            f = cache.getFile(jobId, keys.get(1));
            assertThat(f).hasSize(expected2.length);

            try (FileInputStream fis = new FileInputStream(f)) {
                for (int i = 0; i < 256 && fis.available() > 0; i++) {
                    assertThat((byte) fis.read()).isEqualTo(expected2[i]);
                }

                assertThat(fis.available()).isZero();
            }

            // Remove blobs again
            server[1].globalCleanupAsync(jobId, executorService).join();

            // Verify everything is clean below recoveryDir/<cluster_id>
            final String clusterId = config.get(HighAvailabilityOptions.HA_CLUSTER_ID);
            String haBlobStorePath = config.get(HighAvailabilityOptions.HA_STORAGE_PATH);
            File haBlobStoreDir = new File(haBlobStorePath, clusterId);
            File[] recoveryFiles = haBlobStoreDir.listFiles();
            assertThat(recoveryFiles).as("HA storage directory does not exist").isNotNull();
            assertThat(recoveryFiles).as("Unclean state backend").isEmpty();
        } finally {
            assertThat(executorService.shutdownNow()).isEmpty();

            for (BlobLibraryCacheManager s : libServer) {
                if (s != null) {
                    s.shutdown();
                }
            }
            for (BlobServer s : server) {
                if (s != null) {
                    s.close();
                }
            }

            if (cache != null) {
                cache.close();
            }

            if (blobStoreService != null) {
                blobStoreService.cleanupAllData();
                blobStoreService.close();
            }
        }
    }

    /**
     * Integration test for application-level jar sharing across multiple jobs with the same
     * ApplicationID.
     */
    @TestTemplate
    void testRecoveryWithApplicationLevelBlob() throws Exception {
        Random rand = new Random();

        BlobServer[] server = new BlobServer[2];
        InetSocketAddress[] serverAddress = new InetSocketAddress[2];
        BlobLibraryCacheManager[] libServer = new BlobLibraryCacheManager[2];
        PermanentBlobCache cache = null;
        BlobStoreService blobStoreService = null;

        Configuration config = new Configuration();
        config.set(HighAvailabilityOptions.HA_MODE, "ZOOKEEPER");
        config.set(
                HighAvailabilityOptions.HA_STORAGE_PATH,
                newFolder(temporaryFolder).getAbsolutePath());
        config.set(BlobServerOptions.CLEANUP_INTERVAL, 3_600L);

        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            blobStoreService = BlobUtils.createBlobStoreFromConfig(config);

            final BlobLibraryCacheManager.ClassLoaderFactory classLoaderFactory =
                    BlobLibraryCacheManager.defaultClassLoaderFactory(
                            FlinkUserCodeClassLoaders.ResolveOrder.CHILD_FIRST,
                            new String[0],
                            null,
                            true);

            for (int i = 0; i < 2; i++) {
                server[i] = new BlobServer(config, newFolder(temporaryFolder), blobStoreService);
                server[i].start();
                serverAddress[i] = new InetSocketAddress("localhost", server[i].getPort());

                PermanentBlobCache blobCache =
                        new PermanentBlobCache(
                                config,
                                newFolder(temporaryFolder),
                                blobStoreService,
                                serverAddress[i]);

                libServer[i] =
                        new BlobLibraryCacheManager(
                                blobCache, classLoaderFactory, wrapsSystemClassLoader);
            }

            // Random data
            byte[] expected = new byte[1024];
            rand.nextBytes(expected);

            ArrayList<PermanentBlobKey> keys = new ArrayList<>(2);

            JobID jobId = new JobID();
            ApplicationID applicationId = new ApplicationID();

            // Upload some data (libraries)
            keys.add(server[0].putPermanent(applicationId, expected));

            // The cache
            cache =
                    new PermanentBlobCache(
                            config, newFolder(temporaryFolder), blobStoreService, serverAddress[0]);

            // Register uploaded libraries
            final LibraryCacheManager.ClassLoaderLease classLoaderLease =
                    libServer[0].registerClassLoaderLease(jobId, applicationId);
            classLoaderLease.getOrResolveClassLoader(keys, Collections.emptyList());

            // Verify file
            File f = cache.getFile(applicationId, keys.get(0));
            assertThat(f).hasSize(expected.length);

            try (FileInputStream fis = new FileInputStream(f)) {
                for (int i = 0; i < expected.length && fis.available() > 0; i++) {
                    assertThat((byte) fis.read()).isEqualTo(expected[i]);
                }

                assertThat(fis.available()).isZero();
            }

            // Shutdown cache and start with other server
            cache.close();

            cache =
                    new PermanentBlobCache(
                            config, newFolder(temporaryFolder), blobStoreService, serverAddress[1]);

            // Verify file
            f = cache.getFile(applicationId, keys.get(0));
            assertThat(f).hasSize(expected.length);

            try (FileInputStream fis = new FileInputStream(f)) {
                for (int i = 0; i < expected.length && fis.available() > 0; i++) {
                    assertThat((byte) fis.read()).isEqualTo(expected[i]);
                }

                assertThat(fis.available()).isZero();
            }

            // Remove blobs again
            server[1].globalCleanupAsync(applicationId, executorService).join();

            // Verify everything is clean below recoveryDir/<cluster_id>
            final String clusterId = config.get(HighAvailabilityOptions.HA_CLUSTER_ID);
            String haBlobStorePath = config.get(HighAvailabilityOptions.HA_STORAGE_PATH);
            File haBlobStoreDir = new File(haBlobStorePath, clusterId);
            File[] recoveryFiles = haBlobStoreDir.listFiles();
            assertThat(recoveryFiles).as("HA storage directory does not exist").isNotNull();
            assertThat(recoveryFiles).as("Unclean state backend").isEmpty();
        } finally {
            assertThat(executorService.shutdownNow()).isEmpty();

            for (BlobLibraryCacheManager s : libServer) {
                if (s != null) {
                    s.shutdown();
                }
            }
            for (BlobServer s : server) {
                if (s != null) {
                    s.close();
                }
            }

            if (cache != null) {
                cache.close();
            }

            if (blobStoreService != null) {
                blobStoreService.cleanupAllData();
                blobStoreService.close();
            }
        }
    }
}
