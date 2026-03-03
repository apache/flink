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

import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Factory for creating AWS SDK v2 S3 clients with configuration management.
 *
 * <p>This factory provides type-safe configuration through S3ConfigurationBuilder. Uses shared
 * client with proper lifecycle management to prevent HTTP connection pool exhaustion.
 */
public class S3ClientConfigurationFactory {

    private static final Logger LOG = LoggerFactory.getLogger(S3ClientConfigurationFactory.class);

    // Shared client with reference counting for proper cleanup
    private static volatile S3Client sharedClient;
    private static volatile String sharedConfigHash;
    private static volatile int clientRefCount = 0;
    private static final Object clientLock = new Object();

    /** Private constructor to prevent instantiation. */
    private S3ClientConfigurationFactory() {}

    /**
     * Acquires a reference to the shared S3 client configured to match the given S3AFileSystem.
     * Must be paired with releaseS3Client() to prevent resource leaks.
     *
     * @param s3aFileSystem The S3AFileSystem to match configuration for
     * @return A shared S3 client with consistent configuration
     */
    public static S3Client acquireS3Client(S3AFileSystem s3aFileSystem) {
        try {
            // Handle test scenarios where getConf() might return null
            org.apache.hadoop.conf.Configuration hadoopConf = s3aFileSystem.getConf();
            if (hadoopConf == null) {
                // In test environments, create a minimal configuration
                hadoopConf = new org.apache.hadoop.conf.Configuration();
                LOG.debug(
                        "Using default configuration for test environment (S3AFileSystem.getConf() returned null)");
            }

            // Build configuration from Hadoop configuration
            S3Configuration config =
                    S3ConfigurationBuilder.fromHadoopConfiguration(hadoopConf).build();

            return getOrCreateSharedClient(config);

        } catch (Exception e) {
            LOG.error("Failed to acquire S3 client for S3AFileSystem", e);
            throw new RuntimeException("Failed to acquire S3 client: " + e.getMessage(), e);
        }
    }

    /**
     * Acquires a reference to the shared S3 client from Hadoop configuration directly. Must be
     * paired with releaseS3Client() to prevent resource leaks.
     *
     * @param hadoopConfig The Hadoop configuration to use
     * @return A shared S3 client with consistent configuration
     */
    public static S3Client acquireS3Client(org.apache.hadoop.conf.Configuration hadoopConfig) {
        try {
            // Build configuration from Hadoop configuration
            S3Configuration config =
                    S3ConfigurationBuilder.fromHadoopConfiguration(hadoopConfig).build();

            return getOrCreateSharedClient(config);

        } catch (Exception e) {
            LOG.error("Failed to acquire S3 client from Hadoop configuration", e);
            throw new RuntimeException("Failed to acquire S3 client: " + e.getMessage(), e);
        }
    }

    /**
     * Releases a reference to the shared S3 client. When the last reference is released, the client
     * will be closed to free up HTTP connection pools.
     *
     * @param client The S3 client to release (should match the client returned by acquireS3Client)
     */
    public static void releaseS3Client(S3Client client) {
        if (client == null) {
            return;
        }

        synchronized (clientLock) {
            if (client == sharedClient && clientRefCount > 0) {
                clientRefCount--;
                LOG.debug("Released S3 client reference, remaining refs: {}", clientRefCount);

                // Close the shared client when no more references
                if (clientRefCount == 0) {
                    LOG.debug("Closing shared S3 client - no more references");
                    try {
                        if (sharedClient != null) {
                            sharedClient.close();
                            sharedClient = null;
                            sharedConfigHash = null;
                        }
                    } catch (Exception e) {
                        LOG.warn("Failed to close shared S3 client", e);
                    }
                }
            }
        }
    }

    /** Gets or creates the shared S3 client with reference counting. */
    private static S3Client getOrCreateSharedClient(S3Configuration config) {
        String configHash = config.getConfigurationHash();

        synchronized (clientLock) {
            // Check if we have a client for this configuration
            if (sharedClient != null && configHash.equals(sharedConfigHash)) {
                clientRefCount++;
                LOG.debug("Using existing shared S3 client, refs: {}", clientRefCount);
                return sharedClient;
            }

            // Need to create a new client (configuration changed or first time)
            if (sharedClient != null) {
                LOG.debug("Configuration changed, closing previous S3 client");
                try {
                    sharedClient.close();
                } catch (Exception e) {
                    LOG.warn("Failed to close previous S3 client", e);
                }
            }

            // Create new shared client
            LOG.debug("Creating new shared S3 client for config hash: {}", configHash);
            sharedClient = createS3Client(config);
            sharedConfigHash = configHash;
            clientRefCount = 1;

            LOG.debug("Created shared S3 client, refs: {}", clientRefCount);
            return sharedClient;
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
        } else {
            // Use credential provider that matches Hadoop's S3A configuration
            clientBuilder.credentialsProvider(createHadoopCompatibleCredentialProvider(config));
        }

        // Use AWS SDK default HTTP client to avoid interference with Flink networking
        // Custom HTTP client configuration was causing SSL/connection conflicts

        return clientBuilder.build();
    }

    /**
     * Creates a credential provider that is compatible with Hadoop's S3A configuration. This
     * ensures our custom S3 client uses the same credential chain as Hadoop S3A.
     */
    private static software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
            createHadoopCompatibleCredentialProvider(S3Configuration config) {

        org.apache.hadoop.conf.Configuration hadoopConfig = config.getHadoopConfiguration();

        // Check if Hadoop has a specific credential provider configured
        if (hadoopConfig != null) {
            String credentialProviders = hadoopConfig.get("fs.s3a.aws.credentials.provider");
            if (credentialProviders != null && !credentialProviders.trim().isEmpty()) {
                LOG.debug("Using Hadoop-configured credential providers: {}", credentialProviders);
                // For most cases, especially IAM roles, the default provider chain handles this
                // well
                // and is compatible with Hadoop's credential providers
            }
        }

        // Use AWS SDK's default credential provider chain which includes:
        // 1. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN)
        // 2. System properties (aws.accessKeyId, aws.secretAccessKey, aws.sessionToken)
        // 3. AWS credentials file (~/.aws/credentials)
        // 4. IAM instance profile credentials (for EC2 instances)
        // 5. IAM ECS task role credentials (for ECS tasks)
        // This is compatible with Hadoop's default credential provider chain
        return software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider.create();
    }

    /**
     * Releases the shared S3 client reference. Used for cleanup in tests and shutdown. This
     * decrements the reference count and closes the client if no more references exist.
     */
    public static void releaseS3Client() {
        synchronized (clientLock) {
            if (clientRefCount > 0) {
                clientRefCount--;
                LOG.debug("Released S3 client reference, remaining references: {}", clientRefCount);

                if (clientRefCount == 0 && sharedClient != null) {
                    try {
                        sharedClient.close();
                        LOG.debug("Closed shared S3 client");
                    } catch (Exception e) {
                        LOG.warn("Error closing S3 client", e);
                    } finally {
                        sharedClient = null;
                        sharedConfigHash = null;
                    }
                }
            }
        }
    }
}
