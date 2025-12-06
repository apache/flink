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

package org.apache.flink.fs.s3native;

import org.apache.flink.annotation.Internal;
import org.apache.flink.fs.s3native.token.DynamicTemporaryAWSCredentialsProvider;
import org.apache.flink.fs.s3native.token.NativeS3DelegationTokenReceiver;
import org.apache.flink.util.AutoCloseableAsync;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.sts.model.Credentials;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

import javax.annotation.Nullable;

import java.net.URI;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Provider for S3 clients (sync and async). Handles credential management, delegation tokens, and
 * connection configuration.
 */
@Internal
public class S3ClientProvider implements AutoCloseableAsync {

    private static final Logger LOG = LoggerFactory.getLogger(S3ClientProvider.class);

    private final S3Client s3Client;
    private final S3AsyncClient s3AsyncClient;
    private final S3TransferManager transferManager;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private S3ClientProvider(
            S3Client s3Client, S3AsyncClient s3AsyncClient, S3TransferManager transferManager) {
        this.s3Client = s3Client;
        this.s3AsyncClient = s3AsyncClient;
        this.transferManager = transferManager;
    }

    public S3Client getS3Client() {
        checkNotClosed();
        return s3Client;
    }

    public S3AsyncClient getAsyncClient() {
        checkNotClosed();
        return s3AsyncClient;
    }

    public S3TransferManager getTransferManager() {
        checkNotClosed();
        return transferManager;
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        if (!closed.compareAndSet(false, true)) {
            return CompletableFuture.completedFuture(null);
        }
        LOG.info("Starting async close of S3 client provider");
        return CompletableFuture.runAsync(
                        () -> {
                            if (transferManager != null) {
                                try {
                                    // TransferManager may have in-flight uploads/downloads
                                    transferManager.close();
                                    LOG.debug("S3 TransferManager closed successfully");
                                } catch (Exception e) {
                                    LOG.warn("Error closing S3 TransferManager", e);
                                }
                            }

                            if (s3AsyncClient != null) {
                                try {
                                    // Shutdown Netty event loops gracefully
                                    s3AsyncClient.close();
                                    LOG.debug("S3 async client closed successfully");
                                } catch (Exception e) {
                                    LOG.warn("Error closing S3 async client", e);
                                }
                            }

                            if (s3Client != null) {
                                try {
                                    // Close HTTP connection pools
                                    s3Client.close();
                                    LOG.debug("S3 sync client closed successfully");
                                } catch (Exception e) {
                                    LOG.warn("Error closing S3 sync client", e);
                                }
                            }

                            LOG.info("S3 client provider closed - all resources released");
                        })
                .orTimeout(30, TimeUnit.SECONDS)
                .exceptionally(
                        ex -> {
                            LOG.error("S3 client close timed out after 30 seconds", ex);
                            return null;
                        });
    }

    private void checkNotClosed() {
        if (closed.get()) {
            throw new IllegalStateException("S3ClientProvider has been closed");
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String accessKey;
        private String secretKey;
        private String region;
        private String endpoint;
        private boolean pathStyleAccess = false;
        private int maxConnections = 50;
        private Duration connectionTimeout = Duration.ofSeconds(60);
        private Duration socketTimeout = Duration.ofSeconds(60);
        private boolean disableCertCheck = false;

        public Builder accessKey(@Nullable String accessKey) {
            this.accessKey = accessKey;
            return this;
        }

        public Builder secretKey(@Nullable String secretKey) {
            this.secretKey = secretKey;
            return this;
        }

        public Builder region(@Nullable String region) {
            this.region = region;
            return this;
        }

        public Builder endpoint(@Nullable String endpoint) {
            this.endpoint = endpoint;
            return this;
        }

        public Builder pathStyleAccess(boolean pathStyleAccess) {
            this.pathStyleAccess = pathStyleAccess;
            return this;
        }

        public Builder maxConnections(int maxConnections) {
            this.maxConnections = maxConnections;
            return this;
        }

        public Builder connectionTimeout(Duration connectionTimeout) {
            this.connectionTimeout = connectionTimeout;
            return this;
        }

        public Builder socketTimeout(Duration socketTimeout) {
            this.socketTimeout = socketTimeout;
            return this;
        }

        public Builder disableCertCheck(boolean disableCertCheck) {
            this.disableCertCheck = disableCertCheck;
            return this;
        }

        public S3ClientProvider build() {
            // Try system properties as fallback
            if (accessKey == null) {
                accessKey = System.getProperty("aws.accessKeyId");
            }
            if (secretKey == null) {
                secretKey = System.getProperty("aws.secretAccessKey");
            }
            if (endpoint == null) {
                endpoint = System.getProperty("s3.endpoint");
            }
            String pathStyleProp = System.getProperty("s3.path.style.access");
            if (pathStyleProp != null) {
                pathStyleAccess = Boolean.parseBoolean(pathStyleProp);
            }

            URI endpointUri = (endpoint != null) ? URI.create(endpoint) : null;

            // Auto-detect S3-compatible storage requirements
            boolean isS3Compatible = false;
            if (endpointUri != null) {
                isS3Compatible = true;
                // For non-AWS S3 services, always use path-style access
                if (!pathStyleAccess) {
                    LOG.info(
                            "Custom endpoint detected, enabling path-style access for S3-compatible storage");
                    pathStyleAccess = true;
                }
                // For http endpoints, we don't need cert checking
                if ("http".equalsIgnoreCase(endpointUri.getScheme())) {
                    LOG.debug("HTTP endpoint detected, disabling SSL certificate validation");
                    disableCertCheck = true;
                }
                if (region == null || region.isEmpty()) {
                    region = "us-east-1";
                    LOG.debug(
                            "Setting default region to us-east-1 for S3-compatible storage (required by AWS SDK)");
                }
            }

            Region awsRegion;
            if (region != null && !region.isEmpty()) {
                awsRegion = Region.of(region);
                LOG.debug("Using explicitly configured region: {}", region);
            } else if (!isS3Compatible) {
                try {
                    awsRegion = DefaultAwsRegionProviderChain.builder().build().getRegion();
                    LOG.info(
                            "Automatically detected AWS region: {} (via DefaultAwsRegionProviderChain)",
                            awsRegion.id());
                } catch (Exception e) {
                    awsRegion = Region.US_EAST_1;
                    LOG.warn(
                            "Failed to automatically detect AWS region, falling back to us-east-1. "
                                    + "Consider setting the s3.region configuration or AWS_REGION environment variable. "
                                    + "Error: {}",
                            e.getMessage());
                }
            } else {
                awsRegion = Region.US_EAST_1;
            }

            LOG.info(
                    "Initializing S3 client - endpoint: {}, region: {}, pathStyle: {}, s3Compatible: {}",
                    (endpoint != null ? endpoint : "default AWS S3"),
                    awsRegion.id(),
                    pathStyleAccess,
                    isS3Compatible);

            AwsCredentialsProvider credentialsProvider = buildCredentialsProvider();
            S3Configuration.Builder s3ConfigBuilder = S3Configuration.builder();
            s3ConfigBuilder.pathStyleAccessEnabled(pathStyleAccess);

            if (isS3Compatible) {
                s3ConfigBuilder.chunkedEncodingEnabled(false);
                s3ConfigBuilder.checksumValidationEnabled(false);
                LOG.debug(
                        "Applied S3-compatible storage optimizations: chunked encoding disabled, checksum validation disabled");
            }

            S3Configuration s3Config = s3ConfigBuilder.build();
            S3ClientBuilder clientBuilder = S3Client.builder();
            clientBuilder.credentialsProvider(credentialsProvider);
            clientBuilder.region(awsRegion);
            clientBuilder.serviceConfiguration(s3Config);

            if (endpointUri != null) {
                clientBuilder.endpointOverride(endpointUri);
                LOG.debug("Configured endpoint override: {}", endpointUri);
            }

            ApacheHttpClient.Builder httpClientBuilder =
                    ApacheHttpClient.builder()
                            .maxConnections(maxConnections)
                            .connectionTimeout(connectionTimeout)
                            .socketTimeout(socketTimeout)
                            .tcpKeepAlive(true)
                            .connectionMaxIdleTime(Duration.ofSeconds(60));

            clientBuilder.httpClientBuilder(httpClientBuilder);

            ClientOverrideConfiguration.Builder overrideConfigBuilder =
                    ClientOverrideConfiguration.builder()
                            .retryPolicy(RetryPolicy.builder().numRetries(3).build());

            ClientOverrideConfiguration overrideConfig = overrideConfigBuilder.build();
            clientBuilder.overrideConfiguration(overrideConfig);

            S3Client s3Client = clientBuilder.build();
            LOG.info("S3 sync client initialized successfully");

            S3AsyncClientBuilder asyncClientBuilder = S3AsyncClient.builder();
            asyncClientBuilder.credentialsProvider(credentialsProvider);
            asyncClientBuilder.region(awsRegion);
            asyncClientBuilder.serviceConfiguration(s3Config);

            if (endpointUri != null) {
                asyncClientBuilder.endpointOverride(endpointUri);
            }

            NettyNioAsyncHttpClient.Builder asyncHttpClientBuilder =
                    NettyNioAsyncHttpClient.builder()
                            .maxConcurrency(maxConnections)
                            .connectionTimeout(connectionTimeout)
                            .readTimeout(socketTimeout);

            asyncClientBuilder.httpClientBuilder(asyncHttpClientBuilder);
            asyncClientBuilder.overrideConfiguration(overrideConfig);

            S3AsyncClient s3AsyncClient = asyncClientBuilder.build();
            LOG.info("S3 async client initialized successfully");

            // Build TransferManager for high-performance async operations
            S3TransferManager transferManager =
                    S3TransferManager.builder().s3Client(s3AsyncClient).build();
            LOG.info("S3 TransferManager initialized successfully for async multipart operations");

            return new S3ClientProvider(s3Client, s3AsyncClient, transferManager);
        }

        private AwsCredentialsProvider buildCredentialsProvider() {
            Credentials delegationTokenCredentials =
                    NativeS3DelegationTokenReceiver.getCredentials();

            if (delegationTokenCredentials != null) {
                LOG.info("Using delegation token credentials for authentication");
                return AwsCredentialsProviderChain.builder()
                        .credentialsProviders(
                                new DynamicTemporaryAWSCredentialsProvider(),
                                buildFallbackProvider())
                        .build();
            }

            LOG.debug("No delegation token found, using fallback credentials provider");
            return buildFallbackProvider();
        }

        private AwsCredentialsProvider buildFallbackProvider() {
            if (accessKey != null && secretKey != null) {
                LOG.info(
                        "Using static credentials for authentication (access key: {}***)",
                        accessKey.substring(0, Math.min(4, accessKey.length())));

                AwsBasicCredentials credentials = AwsBasicCredentials.create(accessKey, secretKey);
                return StaticCredentialsProvider.create(credentials);
            }
            LOG.info(
                    "Using default AWS credentials provider chain (will try environment variables, system properties, IAM role, etc.)");
            return DefaultCredentialsProvider.create();
        }
    }
}
