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

/**
 * Simple factory for creating AWS SDK v2 S3 clients with configuration management.
 *
 * <p>This factory provides:
 *
 * <ul>
 *   <li>Type-safe configuration through S3ConfigurationBuilder
 *   <li>Single cached S3 client for consistency
 * </ul>
 */
public class S3ClientConfigurationFactory {

    private static final Logger LOG = LoggerFactory.getLogger(S3ClientConfigurationFactory.class);

    // Simple client cache - we only need one client per configuration
    private static volatile S3Client cachedClient;
    private static volatile String cachedConfigHash;
    private static final Object clientLock = new Object();

    /** Private constructor to prevent instantiation. */
    private S3ClientConfigurationFactory() {}

    /**
     * Creates or retrieves a cached S3 client configured to match the given S3AFileSystem.
     *
     * @param s3aFileSystem The S3AFileSystem to match configuration for
     * @return An S3 client with consistent configuration
     */
    public static S3Client getS3Client(S3AFileSystem s3aFileSystem) {
        try {
            // Build configuration from Hadoop configuration
            S3Configuration config =
                    S3ConfigurationBuilder.fromHadoopConfiguration(s3aFileSystem.getConf()).build();

            return getOrCreateClient(config);

        } catch (Exception e) {
            LOG.error("Failed to create S3 client for S3AFileSystem", e);
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

            return getOrCreateClient(config);

        } catch (Exception e) {
            LOG.error("Failed to create S3 client from Hadoop configuration", e);
            throw new RuntimeException("Failed to create S3 client: " + e.getMessage(), e);
        }
    }

    /**
     * Gets or creates a cached S3 client for the given configuration.
     *
     * @param config The S3 configuration
     * @return A cached or newly created S3 client
     */
    private static S3Client getOrCreateClient(S3Configuration config) {
        String configHash = config.getConfigurationHash();

        // Check if we already have a client for this configuration
        if (cachedClient != null && configHash.equals(cachedConfigHash)) {
            LOG.debug("Using cached S3 client for config hash: {}", configHash);
            return cachedClient;
        }

        // Need to create a new client
        synchronized (clientLock) {
            // Double-check pattern
            if (cachedClient != null && configHash.equals(cachedConfigHash)) {
                LOG.debug("Using cached S3 client (double-check) for config hash: {}", configHash);
                return cachedClient;
            }

            LOG.debug("Creating new S3 client for config hash: {}", configHash);

            // Close existing client if any
            if (cachedClient != null) {
                try {
                    cachedClient.close();
                    LOG.debug("Closed previous S3 client for config hash: {}", cachedConfigHash);
                } catch (Exception e) {
                    LOG.warn("Failed to close previous S3 client", e);
                }
            }

            // Create new client
            S3Client newClient = createS3Client(config);

            // Update cache
            cachedClient = newClient;
            cachedConfigHash = configHash;

            LOG.debug(
                    "Successfully created and cached new S3 client for config hash: {}",
                    configHash);

            return newClient;
        }
    }

    /**
     * Creates a new S3 client with the given configuration.
     *
     * @param config The S3 configuration
     * @return A new S3 client
     */
    private static S3Client createS3Client(S3Configuration config) {
        software.amazon.awssdk.services.s3.S3ClientBuilder clientBuilder =
                software.amazon.awssdk.services.s3.S3Client.builder();

        // Configure region
        if (config.getRegion() != null) {
            clientBuilder.region(software.amazon.awssdk.regions.Region.of(config.getRegion()));
        }

        // Configure endpoint if specified
        if (config.getEndpoint() != null) {
            clientBuilder.endpointOverride(config.getEndpoint());
        }

        // Configure path style access
        clientBuilder.forcePathStyle(config.isPathStyleAccess());

        // Configure credentials if available
        if (config.getAccessKey() != null && config.getSecretKey() != null) {
            software.amazon.awssdk.auth.credentials.AwsCredentials credentials;
            if (config.getSessionToken() != null) {
                credentials =
                        software.amazon.awssdk.auth.credentials.AwsSessionCredentials.create(
                                config.getAccessKey(),
                                config.getSecretKey(),
                                config.getSessionToken());
            } else {
                credentials =
                        software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create(
                                config.getAccessKey(), config.getSecretKey());
            }
            clientBuilder.credentialsProvider(() -> credentials);
        }

        // Configure HTTP client
        software.amazon.awssdk.http.apache.ApacheHttpClient.Builder httpClientBuilder =
                software.amazon.awssdk.http.apache.ApacheHttpClient.builder();

        httpClientBuilder.connectionTimeout(config.getConnectionTimeout());
        httpClientBuilder.socketTimeout(config.getSocketTimeout());
        httpClientBuilder.maxConnections(config.getMaxConnections());

        clientBuilder.httpClient(httpClientBuilder.build());

        // Configure overrides (timeouts, retries)
        software.amazon.awssdk.core.client.config.ClientOverrideConfiguration.Builder
                overrideBuilder =
                        software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
                                .builder();

        overrideBuilder.apiCallTimeout(config.getApiCallTimeout());
        overrideBuilder.apiCallAttemptTimeout(config.getApiCallAttemptTimeout());

        // Configure retry strategy - use default retry mode with custom number of retries
        overrideBuilder.retryPolicy(
                retryPolicyBuilder -> retryPolicyBuilder.numRetries(config.getMaxRetries()));

        clientBuilder.overrideConfiguration(overrideBuilder.build());

        return clientBuilder.build();
    }

    /** Manually triggers cleanup of cached client (mainly for testing). */
    @VisibleForTesting
    public static void cleanup() {
        synchronized (clientLock) {
            if (cachedClient != null) {
                try {
                    cachedClient.close();
                    LOG.debug("Closed cached S3 client during cleanup");
                } catch (Exception e) {
                    LOG.warn("Failed to close cached S3 client during cleanup", e);
                }
                cachedClient = null;
                cachedConfigHash = null;
            }
        }
    }

    /** Shutdown hook for graceful cleanup. */
    public static void shutdown() {
        LOG.info("Shutting down S3ClientConfigurationFactory");
        cleanup();
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
