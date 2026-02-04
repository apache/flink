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

package org.apache.flink.ssl.tests;

import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.tests.util.flink.FlinkResource;
import org.apache.flink.tests.util.flink.FlinkResourceSetup;
import org.apache.flink.tests.util.flink.LocalStandaloneFlinkResourceFactory;
import org.apache.flink.util.TestLogger;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.util.Optional;

import static org.junit.Assert.assertEquals;

/**
 * Base class for SSL end-to-end tests. Contains shared fields and utility methods for SSL testing.
 * In part repeats the bash implementation of the `common_ssl.sh`
 */
public abstract class SslEndToEndITCaseBase extends TestLogger {
    protected static final Logger LOG = LoggerFactory.getLogger(SslEndToEndITCaseBase.class);

    protected static final int INITIAL_VALIDITY_DAYS = 2;
    protected static final int NEW_VALIDITY_DAYS = 365;
    protected static final int RELOAD_CHECK_INTERVAL_MS = 10_000;
    protected static final String SSL_PASSWORD = "password";
    protected static final Duration WAIT_MS = Duration.ofMillis(60_000);
    protected static final String KEYSTORE_FILENAME = "node.keystore";
    protected static final String TRUSTSTORE_FILENAME = "ca.truststore";

    // Fixed ports for deterministic testing
    protected static final int BLOB_SERVER_PORT = 59873;
    protected static final int JOB_MANAGER_RPC_PORT = 6123;
    protected static final int NETTY_SERVER_PORT = 59874;

    @Rule public final FlinkResource flinkResource;

    protected final Path tempDir;
    protected final Path internalSslDir;

    /**
     * Ensures a clean test environment by stopping any stale Flink processes from previous test
     * runs. This prevents port conflicts and certificate confusion.
     */
    @BeforeClass
    public static void ensureCleanEnvironment() {
        LOG.info("Ensuring clean test environment - stopping any stale Flink processes");
        try {
            // Kill any running TaskManager or JobManager processes
            ProcessBuilder pb =
                    new ProcessBuilder(
                            "sh",
                            "-c",
                            "pkill -f 'TaskManagerRunner' || true; pkill -f 'StandaloneSessionClusterEntrypoint' || true");
            Process p = pb.start();
            p.waitFor();
            Thread.sleep(5000); // Wait for processes to fully terminate
            LOG.info("Cleaned up stale Flink processes");
        } catch (Exception e) {
            LOG.warn("Could not clean up stale processes, but continuing with test", e);
        }
    }

    protected SslEndToEndITCaseBase(boolean sslEnabled, boolean sslReloadEnabled)
            throws IOException {
        // Create temp directory for SSL certificates
        this.tempDir = java.nio.file.Files.createTempDirectory("flink-ssl-test-");
        this.internalSslDir = tempDir.resolve("ssl").resolve("internal");

        SslTestUtils.generateAndInstallCertificates(
                internalSslDir, SSL_PASSWORD, INITIAL_VALIDITY_DAYS);

        // Create SSL configuration
        Configuration sslConfig = createSslConfiguration(tempDir, sslEnabled, sslReloadEnabled);

        // Create FlinkResource with SSL configuration
        final FlinkResourceSetup.FlinkResourceSetupBuilder builder = FlinkResourceSetup.builder();
        builder.addConfiguration(sslConfig);
        flinkResource = new LocalStandaloneFlinkResourceFactory().create(builder.build());
    }

    private Configuration createSslConfiguration(
            Path sslDir, boolean sslEnabled, boolean sslReloadEnabled) {
        Configuration config = new Configuration();

        // Set fixed ports for deterministic testing
        config.set(BlobServerOptions.PORT, String.valueOf(BLOB_SERVER_PORT));
        config.set(JobManagerOptions.PORT, JOB_MANAGER_RPC_PORT);
        config.set(TaskManagerOptions.RPC_PORT, String.valueOf(NETTY_SERVER_PORT));
        config.set(SecurityOptions.SSL_INTERNAL_ENABLED, sslEnabled);

        if (sslEnabled) {
            config.set(SecurityOptions.SSL_PROVIDER, "JDK");
            config.set(SecurityOptions.SSL_RELOAD, sslReloadEnabled);

            Path internalSslDir = sslDir.resolve("ssl").resolve("internal");
            config.set(
                    SecurityOptions.SSL_INTERNAL_KEYSTORE,
                    internalSslDir.resolve(KEYSTORE_FILENAME).toString());
            config.set(SecurityOptions.SSL_INTERNAL_KEYSTORE_PASSWORD, SSL_PASSWORD);
            config.set(SecurityOptions.SSL_INTERNAL_KEY_PASSWORD, SSL_PASSWORD);
            config.set(
                    SecurityOptions.SSL_INTERNAL_TRUSTSTORE,
                    internalSslDir.resolve(TRUSTSTORE_FILENAME).toString());
            config.set(SecurityOptions.SSL_INTERNAL_TRUSTSTORE_PASSWORD, SSL_PASSWORD);
        }

        return config;
    }

    /**
     * Returns the configured Flink ports.
     *
     * @return FlinkPorts object containing all configured ports
     */
    protected FlinkPorts getAllPorts() {
        FlinkPorts ports =
                new FlinkPorts(BLOB_SERVER_PORT, JOB_MANAGER_RPC_PORT, NETTY_SERVER_PORT);
        LOG.info("Using configured ports: {}", ports);
        return ports;
    }

    protected Optional<String> getSslCertExpirationDate(int port) throws InterruptedException {
        LOG.info("Verifying initial certificate on port {}", port);
        String[] initialDates = waitForCertificate("localhost", port);
        if (initialDates == null) {
            return Optional.empty();
        }
        String initialNotAfter = initialDates[1];
        LOG.info("Initial certificate notAfter: {}", initialNotAfter);
        return Optional.of(initialNotAfter);
    }

    /**
     * Retrieves certificate expiration dates for all Flink ports.
     *
     * @param ports the FlinkPorts object containing all port numbers
     * @return CertificateDates object containing certificate dates for all ports
     * @throws InterruptedException if interrupted while waiting for certificates
     */
    protected CertificateDates getAllCertificateDates(FlinkPorts ports)
            throws InterruptedException {
        final Optional<String> blobServerCertDate =
                getSslCertExpirationDate(ports.getBlobServerPort());
        final Optional<String> jobManagerRpcCertDate =
                getSslCertExpirationDate(ports.getJobManagerRpcPort());
        final Optional<String> nettyServerCertDate =
                getSslCertExpirationDate(ports.getNettyServerPort());

        CertificateDates certDates =
                new CertificateDates(
                        blobServerCertDate, jobManagerRpcCertDate, nettyServerCertDate);
        LOG.info("Retrieved certificate dates: {}", certDates);
        return certDates;
    }

    /**
     * Waits for and retrieves new certificate dates for all Flink ports after reload.
     *
     * @param ports the FlinkPorts object containing all port numbers
     * @param initialCertDates the initial certificate dates to compare against
     * @return CertificateDates object containing new certificate dates for all ports
     * @throws InterruptedException if interrupted while waiting for certificates
     */
    protected CertificateDates getAllNewCertificateDates(
            FlinkPorts ports, CertificateDates initialCertDates) throws InterruptedException {
        final Optional<String> blobServerCertDate =
                getNewCertificateDate(
                        "localhost",
                        ports.getBlobServerPort(),
                        initialCertDates.getBlobServerCertDate().orElse(""));
        final Optional<String> jobManagerRpcCertDate =
                getNewCertificateDate(
                        "localhost",
                        ports.getJobManagerRpcPort(),
                        initialCertDates.getJobManagerRpcCertDate().orElse(""));
        final Optional<String> nettyServerCertDate =
                getNewCertificateDate(
                        "localhost",
                        ports.getNettyServerPort(),
                        initialCertDates.getNettyServerCertDate().orElse(""));

        CertificateDates newCertDates =
                new CertificateDates(
                        blobServerCertDate, jobManagerRpcCertDate, nettyServerCertDate);
        LOG.info("Retrieved new certificate dates: {}", newCertDates);
        return newCertDates;
    }

    /**
     * Gets the file access time for a given path.
     *
     * @param path the path to the file
     * @return the file's last access time
     * @throws IOException if unable to read file attributes
     */
    protected FileTime getFileAccessTime(Path path) throws IOException {
        return (FileTime) Files.getAttribute(path, "lastAccessTime");
    }

    /**
     * Waits for a certificate to become available on the given host and port.
     *
     * @param host the host to check
     * @param port the port to check
     * @return certificate validity dates [notBefore, notAfter]
     * @throws InterruptedException if interrupted while waiting
     */
    protected String[] waitForCertificate(String host, int port) throws InterruptedException {
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < WAIT_MS.toMillis()) {
            String[] dates = SslTestUtils.getCertificateValidityDates(host, port);
            if (dates != null) {
                return dates;
            }
            LOG.info(
                    "Certificate not yet available, waiting... {} ms left",
                    WAIT_MS.toMillis() - (System.currentTimeMillis() - startTime));
            Thread.sleep(5_000);
        }
        return null;
    }

    /**
     * Waits for the certificate to be reloaded (notAfter date changes).
     *
     * @param host the host to check
     * @param port the port to check
     * @param initialCertDate the original date to compare against
     * @return Optional containing the new ecpiration certificate date if reload occurred, empty
     *     otherwise
     * @throws InterruptedException if interrupted while waiting
     */
    protected Optional<String> getNewCertificateDate(String host, int port, String initialCertDate)
            throws InterruptedException {
        long startTime = System.currentTimeMillis();
        int checkCount = 0;
        Duration reloadWaitMs = Duration.ofMillis(3 * WAIT_MS.toMillis());

        while (System.currentTimeMillis() - startTime < reloadWaitMs.toMillis()) {
            Thread.sleep(RELOAD_CHECK_INTERVAL_MS);
            checkCount++;

            String[] dates = SslTestUtils.getCertificateValidityDates(host, port);
            if (dates != null) {
                String currentNotAfter = dates[1];
                LOG.info(
                        "Check #{}: Current certificate notAfter: {}", checkCount, currentNotAfter);

                if (!currentNotAfter.equals(initialCertDate)) {
                    LOG.info(
                            "Certificate reload detected after {} ms!",
                            System.currentTimeMillis() - startTime);
                    return Optional.of(currentNotAfter);
                }
            } else {
                LOG.warn("Could not retrieve certificate on check #{}", checkCount);
            }
        }

        LOG.warn("Certificate reload not detected within {} ms", reloadWaitMs.toMillis());
        return Optional.empty();
    }

    protected void verifyCertFilesAreNotAccessed(
            FileTime keystoreAccessTimeBefore, FileTime truststoreAccessTimeBefore)
            throws InterruptedException, IOException {
        LOG.info(
                "Waiting {} seconds to verify certificates are not accessed...",
                WAIT_MS.toSeconds());
        Thread.sleep(WAIT_MS.toMillis());

        FileTime keystoreAccessTimeAfter =
                getFileAccessTime(internalSslDir.resolve(KEYSTORE_FILENAME));
        FileTime truststoreAccessTimeAfter =
                getFileAccessTime(internalSslDir.resolve(TRUSTSTORE_FILENAME));

        assertEquals(
                "Keystore should not be accessed when SSL is disabled",
                keystoreAccessTimeBefore,
                keystoreAccessTimeAfter);
        assertEquals(
                "Truststore should not be accessed when SSL is disabled",
                truststoreAccessTimeBefore,
                truststoreAccessTimeAfter);

        LOG.info("SSL end-to-end test completed successfully (SSL disabled verified)");
    }

    /** POJO class to hold Flink port information. */
    protected static class FlinkPorts {
        private final int blobServerPort;
        private final int jobManagerRpcPort;
        private final int nettyServerPort;

        public FlinkPorts(int blobServerPort, int jobManagerRpcPort, int nettyServerPort) {
            this.blobServerPort = blobServerPort;
            this.jobManagerRpcPort = jobManagerRpcPort;
            this.nettyServerPort = nettyServerPort;
        }

        public int getBlobServerPort() {
            return blobServerPort;
        }

        public int getJobManagerRpcPort() {
            return jobManagerRpcPort;
        }

        public int getNettyServerPort() {
            return nettyServerPort;
        }

        @Override
        public String toString() {
            return String.format(
                    "FlinkPorts{blobServer=%d, jobManagerRpc=%d, nettyServer=%d}",
                    blobServerPort, jobManagerRpcPort, nettyServerPort);
        }
    }

    protected static class CertificateDates {
        private final Optional<String> blobServerCertDate;
        private final Optional<String> jobManagerRpcCertDate;
        private final Optional<String> nettyServerCertDate;

        public CertificateDates(
                Optional<String> blobServerCertDate,
                Optional<String> jobManagerRpcCertDate,
                Optional<String> nettyServerCertDate) {
            this.blobServerCertDate = blobServerCertDate;
            this.jobManagerRpcCertDate = jobManagerRpcCertDate;
            this.nettyServerCertDate = nettyServerCertDate;
        }

        public Optional<String> getBlobServerCertDate() {
            return blobServerCertDate;
        }

        public Optional<String> getJobManagerRpcCertDate() {
            return jobManagerRpcCertDate;
        }

        public Optional<String> getNettyServerCertDate() {
            return nettyServerCertDate;
        }

        /**
         * Checks if all certificate dates are present.
         *
         * @return true if all certificates are present, false otherwise
         */
        public boolean isAllPresent() {
            return blobServerCertDate.isPresent()
                    && jobManagerRpcCertDate.isPresent()
                    && nettyServerCertDate.isPresent();
        }

        /**
         * Checks if none of the certificate dates are present.
         *
         * @return true if none of the certificates are present, false otherwise
         */
        public boolean isNonePresent() {
            return !blobServerCertDate.isPresent()
                    && !jobManagerRpcCertDate.isPresent()
                    && !nettyServerCertDate.isPresent();
        }

        @Override
        public String toString() {
            return String.format(
                    "CertificateDates{blobServer=%s, jobManagerRpc=%s, nettyServer=%s}",
                    blobServerCertDate.orElse("N/A"),
                    jobManagerRpcCertDate.orElse("N/A"),
                    nettyServerCertDate.orElse("N/A"));
        }
    }
}
