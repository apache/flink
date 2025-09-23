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

package org.apache.flink.runtime.blob;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.core.security.watch.LocalFSWatchService;
import org.apache.flink.core.security.watch.LocalFSWatchServiceListener;
import org.apache.flink.core.security.watch.LocalFSWatchSingleton;
import org.apache.flink.runtime.net.SSLUtilsTest;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

public class BlobClientSslReloadTest {

    private static final Logger LOG = LoggerFactory.getLogger(BlobClientSslReloadTest.class);

    private static final Duration TIMEOUT = Duration.ofSeconds(20);

    private static BlobServer blobReloadableSslServer;

    private static Configuration reloadableSslClientConfig;

    private static TestLocalFSWatchService watchService;

    @TempDir static java.nio.file.Path tempDir;

    @BeforeAll
    static void startReloadableSSLServer() throws IOException {
        Configuration config =
                SSLUtilsTest.createInternalSslConfigWithKeyAndTrustStores(
                        SecurityOptions.SSL_PROVIDER.defaultValue());
        config.set(SecurityOptions.SSL_RELOAD, true);

        blobReloadableSslServer =
                TestingBlobUtils.createServer(tempDir.resolve("realoadable_ssl"), config);
        blobReloadableSslServer.start();

        reloadableSslClientConfig = config;
    }

    @BeforeAll
    static void startLocalFSWatchService() throws InterruptedException {
        watchService = new TestLocalFSWatchService();
        watchService.start();
    }

    /** Shuts the BLOB server down. */
    @AfterAll
    static void stopServers() throws IOException {
        if (blobReloadableSslServer != null) {
            blobReloadableSslServer.close();
        }
    }

    /** Verify that blob server doesn't run watchers to watch the ssl certificates change. */
    @Test
    public void testWatchersRegistered() throws Exception {
        LocalFSWatchSingleton watchSingleton =
                (LocalFSWatchSingleton) LocalFSWatchSingleton.getInstance();
        assertThat(watchSingleton.getWatchers().size()).isGreaterThan(0);
    }

    static Stream<Arguments> sslReloadTestParameters() {
        return Stream.of(
                Arguments.of(true, true, "both keystore and truststore"),
                Arguments.of(true, false, "keystore only"),
                Arguments.of(false, true, "truststore only"));
    }

    /** Verify ssl client to ssl server upload with different certificate modification scenarios. */
    @ParameterizedTest
    @MethodSource("sslReloadTestParameters")
    public void testUploadJarFilesHelperReloadable(
            boolean touchKeyStore, boolean touchTrustStore, String description) throws Exception {
        int initialReloadCounter = prepare();
        int watchServiceReloadCounter = watchService.getServerSideReloadCounter();

        LOG.debug(
                "Testing SSL reload scenario: {}; initialReloadCounter={}",
                description,
                initialReloadCounter);

        // Touch the specified certificate files
        if (touchKeyStore) {
            SSLUtilsTest.touchKeyStore();
        }
        if (touchTrustStore) {
            SSLUtilsTest.touchTrustStore();
        }

        LOG.debug("Modified SSL certificate files for: {}", description);

        waitServerSideWatchEventReceived(watchServiceReloadCounter);
        assertSslReloaded(initialReloadCounter);
    }

    private static void assertSslReloaded(int initialReloadCounter) throws Exception {
        LOG.debug("Initiating another file upload, which should lead to the context reload");

        // Retry file upload with exponential backoff to handle SSL reload timing issues
        uploadJarFileWithRetry(blobReloadableSslServer, reloadableSslClientConfig, 3, 100);

        // wait when server reloads certificates
        assertTimeoutPreemptively(
                TIMEOUT,
                () -> {
                    while (blobReloadableSslServer.getReloadCounter() == initialReloadCounter) {
                        Thread.sleep(100);
                    }
                    assertThat(blobReloadableSslServer.getReloadCounter())
                            .withFailMessage(
                                    "Expect ssl changes to be reloaded for BlobServer in "
                                            + TIMEOUT)
                            .isGreaterThan(initialReloadCounter);
                });
    }

    private static void uploadJarFileWithRetry(
            BlobServer server, Configuration config, int maxRetries, long baseDelayMs)
            throws Exception {
        Exception lastException = null;

        for (int attempt = 0; attempt < maxRetries; attempt++) {
            try {
                LOG.debug("Upload attempt {} of {}", attempt, maxRetries);
                BlobClientTest.uploadJarFile(server, config);
                LOG.debug("Upload successful on attempt {}", attempt);
                return; // Success, exit retry loop
            } catch (Exception e) {
                lastException = e;
                String errorMsg = e.getMessage();

                // Check if this is a retryable SSL/connection error
                if (isRetryableError(e)) {
                    long delayMs = baseDelayMs * (1L << (attempt - 1)); // Exponential backoff
                    LOG.debug(
                            "Upload failed on attempt {} with retryable error: {}. Retrying in {}ms",
                            attempt,
                            errorMsg,
                            delayMs);
                    Thread.sleep(delayMs);
                } else {
                    LOG.warn(
                            "Upload failed on attempt {} with non-retryable error or max retries reached: {}",
                            attempt,
                            errorMsg);
                    break;
                }
            }
        }

        // If we get here, all retries failed
        throw new Exception("File upload failed after " + maxRetries + " attempts", lastException);
    }

    private static boolean isRetryableError(Exception e) {
        String message = e.getMessage();
        if (message == null) {
            return false;
        }

        // Check for SSL reload related errors
        return message.contains("Broken pipe")
                || message.contains("Connection reset")
                || message.contains("Connection refused")
                || message.contains("Socket closed")
                || message.contains("SSL handshake")
                || e instanceof java.net.SocketException
                || e instanceof java.io.IOException
                        && (message.contains("PUT operation failed")
                                || message.contains("Connection or inbound has closed"));
    }

    private static void waitServerSideWatchEventReceived(int watchServiceReloadCounter) {
        assertTimeoutPreemptively(
                TIMEOUT,
                () -> {
                    while (watchService.getServerSideReloadCounter() == watchServiceReloadCounter) {
                        Thread.sleep(100);
                    }
                    assertThat(watchService.getServerSideReloadCounter())
                            .withFailMessage(
                                    "Expect sll changes by FileWatcher to be reloaded in "
                                            + TIMEOUT)
                            .isGreaterThan(watchServiceReloadCounter);
                });
        LOG.debug("SSL file modifications are seen");
    }

    private static int prepare() throws Exception {
        LOG.debug("Initial upload of jar files");

        BlobClientTest.uploadJarFile(blobReloadableSslServer, reloadableSslClientConfig);

        return blobReloadableSslServer.getReloadCounter();
    }

    private static class TestLocalFSWatchService extends LocalFSWatchService {

        AtomicInteger serverSideReloadCounter = new AtomicInteger(0);

        TestLocalFSWatchService() {
            super(Duration.ofMillis(0));
        }

        protected void processWatchKey(
                Map.Entry<WatchService, LocalFSWatchServiceListener> entry, WatchKey watchKey) {
            super.processWatchKey(entry, watchKey);
            LOG.debug("Watch key has been processed for {}", entry);
            if (entry.getValue() instanceof BlobServerSocket) {
                serverSideReloadCounter.incrementAndGet();
            }
        }

        public int getServerSideReloadCounter() {
            return serverSideReloadCounter.get();
        }
    }
}
