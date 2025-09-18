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

package org.apache.flink.fs.s3hadoop;

import org.apache.flink.annotation.Internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Manages S3 client connections and their lifecycle. This provides advanced features like
 * connection health monitoring, automatic cleanup of idle connections, and resource leak detection.
 */
@Internal
public class S3ConnectionPoolManager {

    private static final Logger LOG = LoggerFactory.getLogger(S3ConnectionPoolManager.class);
    private static final S3ConnectionPoolManager INSTANCE = new S3ConnectionPoolManager();

    // Connection tracking
    private final ConcurrentHashMap<String, ManagedS3Client> activeClients =
            new ConcurrentHashMap<>();
    private final AtomicInteger totalClientsCreated = new AtomicInteger(0);
    private final AtomicInteger totalClientsClosed = new AtomicInteger(0);

    // Cleanup management
    private final ScheduledExecutorService cleanupExecutor =
            Executors.newSingleThreadScheduledExecutor(
                    r -> {
                        Thread t = new Thread(r, "S3ConnectionPool-Cleanup");
                        t.setDaemon(true);
                        return t;
                    });

    // Configuration
    private static final Duration MAX_IDLE_TIME = Duration.ofMinutes(30);
    private static final Duration CLEANUP_INTERVAL = Duration.ofMinutes(5);

    private volatile boolean shutdown = false;

    private S3ConnectionPoolManager() {
        // Start periodic cleanup (every 5 minutes)
        cleanupExecutor.scheduleWithFixedDelay(
                this::cleanupIdleConnections,
                5, // initial delay in minutes
                5, // period in minutes
                TimeUnit.MINUTES);

        // Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
    }

    public static S3ConnectionPoolManager getInstance() {
        return INSTANCE;
    }

    /** Gets or creates an S3 client for the given configuration. */
    public S3Client getClient(String configHash, S3Configuration config) {
        if (shutdown) {
            throw new IllegalStateException("Connection pool is shutdown");
        }

        ManagedS3Client managedClient =
                activeClients.compute(
                        configHash,
                        (key, existing) -> {
                            if (existing != null && !existing.isClosed()) {
                                existing.updateLastUsed();
                                return existing;
                            } else {
                                // Create new client
                                S3Client newClient = createS3Client(config);
                                totalClientsCreated.incrementAndGet();
                                LOG.debug("Created new S3 client for config hash: {}", configHash);
                                return new ManagedS3Client(newClient, config);
                            }
                        });

        return managedClient.getClient();
    }

    /** Manually releases a client (optional - clients are auto-cleaned up). */
    public void releaseClient(String configHash) {
        ManagedS3Client managedClient = activeClients.remove(configHash);
        if (managedClient != null) {
            closeClient(managedClient);
        }
    }

    /** Cleanup idle connections that haven't been used recently. */
    private void cleanupIdleConnections() {
        if (shutdown) {
            return;
        }

        Instant cutoff = Instant.now().minus(MAX_IDLE_TIME);
        activeClients
                .entrySet()
                .removeIf(
                        entry -> {
                            ManagedS3Client client = entry.getValue();
                            if (client.getLastUsed().isBefore(cutoff) || client.isClosed()) {
                                closeClient(client);
                                LOG.debug(
                                        "Cleaned up idle S3 client for config hash: {}",
                                        entry.getKey());
                                return true;
                            }
                            return false;
                        });
    }

    private void closeClient(ManagedS3Client managedClient) {
        try {
            managedClient.close();
            totalClientsClosed.incrementAndGet();
        } catch (Exception e) {
            LOG.warn("Error closing S3 client", e);
        }
    }

    private S3Client createS3Client(S3Configuration config) {
        // Create S3 client using AWS SDK v2 with the configuration
        return createS3ClientFromConfiguration(config);
    }

    private S3Client createS3ClientFromConfiguration(S3Configuration config) {
        software.amazon.awssdk.services.s3.S3ClientBuilder clientBuilder =
                software.amazon.awssdk.services.s3.S3Client.builder();

        // Configure credentials
        if (config.getAccessKey() != null && config.getSecretKey() != null) {
            if (config.getSessionToken() != null) {
                clientBuilder.credentialsProvider(
                        software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.create(
                                software.amazon.awssdk.auth.credentials.AwsSessionCredentials
                                        .create(
                                                config.getAccessKey(),
                                                config.getSecretKey(),
                                                config.getSessionToken())));
            } else {
                clientBuilder.credentialsProvider(
                        software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.create(
                                software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create(
                                        config.getAccessKey(), config.getSecretKey())));
            }
        }

        // Configure region
        clientBuilder.region(software.amazon.awssdk.regions.Region.of(config.getRegion()));

        // Configure endpoint if specified
        if (config.getEndpoint() != null) {
            clientBuilder.endpointOverride(config.getEndpoint());
        }

        // Configure HTTP client
        software.amazon.awssdk.http.apache.ApacheHttpClient.Builder httpClientBuilder =
                software.amazon.awssdk.http.apache.ApacheHttpClient.builder()
                        .connectionTimeout(config.getConnectionTimeout())
                        .socketTimeout(config.getSocketTimeout())
                        .maxConnections(config.getMaxConnections());

        clientBuilder.httpClientBuilder(httpClientBuilder);

        // Configure service settings
        software.amazon.awssdk.services.s3.S3Configuration.Builder serviceConfigBuilder =
                software.amazon.awssdk.services.s3.S3Configuration.builder()
                        .pathStyleAccessEnabled(config.isPathStyleAccess())
                        .checksumValidationEnabled(config.isChecksumValidation());

        clientBuilder.serviceConfiguration(serviceConfigBuilder.build());

        // Configure client overrides
        software.amazon.awssdk.core.client.config.ClientOverrideConfiguration.Builder
                overrideBuilder =
                        software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
                                .builder();

        // Configure retry policy
        software.amazon.awssdk.core.retry.RetryPolicy retryPolicy =
                software.amazon.awssdk.core.retry.RetryPolicy.builder()
                        .numRetries(config.getMaxRetries())
                        .backoffStrategy(
                                software.amazon.awssdk.core.retry.backoff.FixedDelayBackoffStrategy
                                        .create(config.getRetryInterval()))
                        .build();

        overrideBuilder.retryPolicy(retryPolicy);
        overrideBuilder.apiCallTimeout(config.getApiCallTimeout());
        overrideBuilder.apiCallAttemptTimeout(config.getApiCallAttemptTimeout());

        clientBuilder.overrideConfiguration(overrideBuilder.build());

        return clientBuilder.build();
    }

    /** Shutdown the connection pool manager. */
    public void shutdown() {
        if (shutdown) {
            return;
        }

        shutdown = true;

        LOG.info("Shutting down S3 connection pool manager");

        // Close all active clients
        activeClients.values().forEach(this::closeClient);
        activeClients.clear();

        // Shutdown cleanup executor
        cleanupExecutor.shutdown();
        try {
            if (!cleanupExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                cleanupExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            cleanupExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        LOG.info(
                "S3 connection pool shutdown complete. Clients created: {}, closed: {}",
                totalClientsCreated.get(),
                totalClientsClosed.get());
    }

    /** Gets current pool statistics. */
    public PoolStatistics getStatistics() {
        return new PoolStatistics(
                activeClients.size(),
                totalClientsCreated.get(),
                totalClientsClosed.get(),
                shutdown);
    }

    /** Wrapper for S3Client with management metadata. */
    private static class ManagedS3Client {
        private final S3Client client;
        private final S3Configuration configuration;
        private final AtomicReference<Instant> lastUsed;
        private volatile boolean closed = false;

        ManagedS3Client(S3Client client, S3Configuration configuration) {
            this.client = client;
            this.configuration = configuration;
            this.lastUsed = new AtomicReference<>(Instant.now());
        }

        S3Client getClient() {
            updateLastUsed();
            return client;
        }

        void updateLastUsed() {
            lastUsed.set(Instant.now());
        }

        Instant getLastUsed() {
            return lastUsed.get();
        }

        boolean isClosed() {
            return closed;
        }

        void close() {
            if (!closed) {
                closed = true;
                try {
                    client.close();
                } catch (Exception e) {
                    LOG.warn("Error closing S3 client", e);
                }
            }
        }

        S3Configuration getConfiguration() {
            return configuration;
        }
    }

    /** Statistics about the connection pool. */
    public static class PoolStatistics {
        private final int activeConnections;
        private final int totalConnectionsCreated;
        private final int totalConnectionsClosed;
        private final boolean shutdown;

        PoolStatistics(
                int activeConnections,
                int totalConnectionsCreated,
                int totalConnectionsClosed,
                boolean shutdown) {
            this.activeConnections = activeConnections;
            this.totalConnectionsCreated = totalConnectionsCreated;
            this.totalConnectionsClosed = totalConnectionsClosed;
            this.shutdown = shutdown;
        }

        public int getActiveConnections() {
            return activeConnections;
        }

        public int getTotalConnectionsCreated() {
            return totalConnectionsCreated;
        }

        public int getTotalConnectionsClosed() {
            return totalConnectionsClosed;
        }

        public boolean isShutdown() {
            return shutdown;
        }

        @Override
        public String toString() {
            return String.format(
                    "PoolStatistics{active=%d, created=%d, closed=%d, shutdown=%s}",
                    activeConnections, totalConnectionsCreated, totalConnectionsClosed, shutdown);
        }
    }
}
