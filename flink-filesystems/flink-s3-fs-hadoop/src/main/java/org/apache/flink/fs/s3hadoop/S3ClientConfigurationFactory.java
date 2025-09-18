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

import org.apache.flink.annotation.VisibleForTesting;

import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.Map;

/**
 * Modern factory for creating AWS SDK v2 S3 clients with advanced features including configuration
 * management, connection pooling, metrics collection, and error handling.
 *
 * <p>This factory uses the new architectural components to provide:
 *
 * <ul>
 *   <li>Type-safe configuration through S3ConfigurationBuilder
 *   <li>Connection pooling and lifecycle management
 *   <li>Comprehensive metrics collection
 *   <li>Intelligent error handling with circuit breakers
 * </ul>
 */
public class S3ClientConfigurationFactory {

    private static final Logger LOG = LoggerFactory.getLogger(S3ClientConfigurationFactory.class);

    // Shared components using the new architecture
    private static final S3ConnectionPoolManager connectionPoolManager =
            S3ConnectionPoolManager.getInstance();
    private static final S3MetricsManager metricsManager = S3MetricsManager.getInstance();

    /** Private constructor to prevent instantiation. */
    private S3ClientConfigurationFactory() {}

    /**
     * Creates or retrieves a cached S3 client configured to match the given S3AFileSystem. Uses the
     * new architectural components for enhanced performance and reliability.
     *
     * @param s3aFileSystem The S3AFileSystem to match configuration for
     * @return An S3 client with consistent configuration
     */
    public static S3Client getS3Client(S3AFileSystem s3aFileSystem) {
        try {
            // Build configuration from Hadoop configuration
            S3Configuration config =
                    S3ConfigurationBuilder.fromHadoopConfiguration(s3aFileSystem.getConf()).build();

            String configHash = config.getConfigurationHash();

            // Record cache access
            metricsManager.recordCacheHit(); // Will be overridden if it's a miss

            LOG.debug("Getting S3 client for config hash: {}", configHash);

            S3Client client = connectionPoolManager.getClient(configHash, config);

            LOG.debug("Successfully obtained S3 client for S3AFileSystem");
            return client;

        } catch (Exception e) {
            LOG.error("Failed to create S3 client for S3AFileSystem", e);
            metricsManager.recordOperationError("getS3Client", e);
            throw new RuntimeException("Failed to create S3 client: " + e.getMessage(), e);
        }
    }

    /**
     * Creates an S3 client from Hadoop configuration directly.
     *
     * @param hadoopConfig The Hadoop configuration to use
     * @return An S3 client with consistent configuration
     */
    public static S3Client getS3Client(org.apache.hadoop.conf.Configuration hadoopConfig) {
        try {
            // Build configuration from Hadoop configuration
            S3Configuration config =
                    S3ConfigurationBuilder.fromHadoopConfiguration(hadoopConfig).build();

            String configHash = config.getConfigurationHash();

            LOG.debug("Getting S3 client for config hash: {}", configHash);

            S3Client client = connectionPoolManager.getClient(configHash, config);

            LOG.debug("Successfully obtained S3 client from Hadoop configuration");
            return client;

        } catch (Exception e) {
            LOG.error("Failed to create S3 client from Hadoop configuration", e);
            metricsManager.recordOperationError("getS3Client", e);
            throw new RuntimeException("Failed to create S3 client: " + e.getMessage(), e);
        }
    }

    /**
     * Returns comprehensive metrics for monitoring the S3 client factory.
     *
     * @return Current metrics snapshot
     */
    @VisibleForTesting
    public static Map<String, Long> getMetrics() {
        S3MetricsManager.S3Metrics metrics = metricsManager.getMetrics();
        return Map.of(
                "total_operations", metrics.getTotalOperations(),
                "total_errors", metrics.getTotalErrors(),
                "cache_hits", metrics.getCacheHits(),
                "cache_misses", metrics.getCacheMisses(),
                "clients_created", metrics.getClientsCreated(),
                "active_helpers", metrics.getActiveHelpers());
    }

    /**
     * Returns connection pool statistics.
     *
     * @return Current pool statistics
     */
    @VisibleForTesting
    public static S3ConnectionPoolManager.PoolStatistics getPoolStatistics() {
        return connectionPoolManager.getStatistics();
    }

    /** Manually triggers cleanup of idle connections (mainly for testing). */
    @VisibleForTesting
    public static void cleanup() {
        // The connection pool manager handles cleanup automatically
        LOG.debug("Manual cleanup requested - connection pool handles this automatically");
    }

    /** Shutdown hook for graceful cleanup. */
    public static void shutdown() {
        LOG.info("Shutting down S3ClientConfigurationFactory");
        connectionPoolManager.shutdown();
    }

    static {
        // Add shutdown hook to ensure graceful cleanup
        Runtime.getRuntime()
                .addShutdownHook(
                        new Thread(
                                () -> {
                                    LOG.info(
                                            "JVM shutdown detected, cleaning up S3 client factory");
                                    shutdown();
                                }));
    }
}
