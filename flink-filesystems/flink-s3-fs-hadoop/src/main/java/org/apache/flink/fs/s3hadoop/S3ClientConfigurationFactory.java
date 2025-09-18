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
 * <p>This factory provides type-safe configuration through S3ConfigurationBuilder. No global
 * caching is used to avoid resource leaks and test interference.
 */
public class S3ClientConfigurationFactory {

    private static final Logger LOG = LoggerFactory.getLogger(S3ClientConfigurationFactory.class);

    /** Private constructor to prevent instantiation. */
    private S3ClientConfigurationFactory() {}

    /**
     * Creates a new S3 client configured to match the given S3AFileSystem.
     *
     * @param s3aFileSystem The S3AFileSystem to match configuration for
     * @return An S3 client with consistent configuration
     */
    public static S3Client getS3Client(S3AFileSystem s3aFileSystem) {
        try {
            // Build configuration from Hadoop configuration
            S3Configuration config =
                    S3ConfigurationBuilder.fromHadoopConfiguration(s3aFileSystem.getConf()).build();

            return createS3Client(config);

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

            return createS3Client(config);

        } catch (Exception e) {
            LOG.error("Failed to create S3 client from Hadoop configuration", e);
            throw new RuntimeException("Failed to create S3 client: " + e.getMessage(), e);
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
}
