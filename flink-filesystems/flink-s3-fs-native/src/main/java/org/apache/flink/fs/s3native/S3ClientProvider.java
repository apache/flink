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
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.Credentials;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.utils.SdkAutoCloseable;

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

    /** Timeout in seconds for closing S3 clients. */
    private static final long CLIENT_CLOSE_TIMEOUT_SECONDS = 30;

    private final S3Client s3Client;
    private final S3AsyncClient s3AsyncClient;
    private final S3TransferManager transferManager;
    private final S3EncryptionConfig encryptionConfig;
    @Nullable private final AwsCredentialsProvider credentialsProvider;
    @Nullable private final StsClient stsClient;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private S3ClientProvider(
            S3Client s3Client,
            S3AsyncClient s3AsyncClient,
            S3TransferManager transferManager,
            S3EncryptionConfig encryptionConfig,
            @Nullable AwsCredentialsProvider credentialsProvider,
            @Nullable StsClient stsClient) {
        this.s3Client = s3Client;
        this.s3AsyncClient = s3AsyncClient;
        this.transferManager = transferManager;
        this.encryptionConfig =
                encryptionConfig != null ? encryptionConfig : S3EncryptionConfig.none();
        this.credentialsProvider = credentialsProvider;
        this.stsClient = stsClient;
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

    /** Returns the encryption configuration for S3 operations. */
    public S3EncryptionConfig getEncryptionConfig() {
        return encryptionConfig;
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        if (!closed.compareAndSet(false, true)) {
            return CompletableFuture.completedFuture(null);
        }
        return CompletableFuture.runAsync(
                        () -> {
                            if (transferManager != null) {
                                try {
                                    transferManager.close();
                                } catch (Exception e) {
                                    LOG.warn("Error closing S3 TransferManager", e);
                                }
                            }
                            if (s3AsyncClient != null) {
                                try {
                                    s3AsyncClient.close();
                                } catch (Exception e) {
                                    LOG.warn("Error closing S3 async client", e);
                                }
                            }
                            if (s3Client != null) {
                                try {
                                    s3Client.close();
                                } catch (Exception e) {
                                    LOG.warn("Error closing S3 sync client", e);
                                }
                            }
                            if (credentialsProvider instanceof SdkAutoCloseable) {
                                try {
                                    ((SdkAutoCloseable) credentialsProvider).close();
                                } catch (Exception e) {
                                    LOG.warn("Error closing credentials provider", e);
                                }
                            }
                            if (stsClient != null) {
                                try {
                                    stsClient.close();
                                } catch (Exception e) {
                                    LOG.warn("Error closing STS client", e);
                                }
                            }
                        })
                .orTimeout(CLIENT_CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .exceptionally(
                        ex -> {
                            LOG.error(
                                    "S3 client close timed out after {} seconds",
                                    CLIENT_CLOSE_TIMEOUT_SECONDS,
                                    ex);
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

        // AssumeRole configuration
        private String assumeRoleArn;
        private String assumeRoleExternalId;
        private String assumeRoleSessionName = "flink-s3-session";
        private int assumeRoleSessionDurationSeconds = 3600;

        // Encryption configuration
        private S3EncryptionConfig encryptionConfig;

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

        public Builder assumeRoleArn(@Nullable String assumeRoleArn) {
            this.assumeRoleArn = assumeRoleArn;
            return this;
        }

        public Builder assumeRoleExternalId(@Nullable String assumeRoleExternalId) {
            this.assumeRoleExternalId = assumeRoleExternalId;
            return this;
        }

        public Builder assumeRoleSessionName(@Nullable String assumeRoleSessionName) {
            if (assumeRoleSessionName != null) {
                this.assumeRoleSessionName = assumeRoleSessionName;
            }
            return this;
        }

        public Builder assumeRoleSessionDurationSeconds(int assumeRoleSessionDurationSeconds) {
            this.assumeRoleSessionDurationSeconds = assumeRoleSessionDurationSeconds;
            return this;
        }

        public Builder encryptionConfig(@Nullable S3EncryptionConfig encryptionConfig) {
            this.encryptionConfig = encryptionConfig;
            return this;
        }

        public S3ClientProvider build() {
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
            boolean isS3Compatible = endpointUri != null;

            if (isS3Compatible && !pathStyleAccess) {
                pathStyleAccess = true;
            }
            if (isS3Compatible && "http".equalsIgnoreCase(endpointUri.getScheme())) {
                disableCertCheck = true;
            }

            Region awsRegion = resolveRegion(region);
            StsClient stsClient = null;
            AwsCredentialsProvider credentialsProvider;

            AwsCredentialsProvider baseProvider = buildBaseCredentialsProvider();
            if (assumeRoleArn != null && !assumeRoleArn.isEmpty()) {
                stsClient = buildStsClient(baseProvider, awsRegion);
                credentialsProvider = buildAssumeRoleProvider(stsClient);
            } else {
                credentialsProvider = baseProvider;
            }

            S3Configuration.Builder s3ConfigBuilder =
                    S3Configuration.builder().pathStyleAccessEnabled(pathStyleAccess);
            if (isS3Compatible) {
                s3ConfigBuilder.chunkedEncodingEnabled(false).checksumValidationEnabled(false);
            }
            S3Configuration s3Config = s3ConfigBuilder.build();

            ClientOverrideConfiguration overrideConfig =
                    ClientOverrideConfiguration.builder()
                            .retryPolicy(RetryPolicy.builder().numRetries(3).build())
                            .build();

            ApacheHttpClient.Builder httpClientBuilder =
                    ApacheHttpClient.builder()
                            .maxConnections(maxConnections)
                            .connectionTimeout(connectionTimeout)
                            .socketTimeout(socketTimeout)
                            .tcpKeepAlive(true)
                            .connectionMaxIdleTime(Duration.ofSeconds(60));

            S3ClientBuilder clientBuilder =
                    S3Client.builder()
                            .credentialsProvider(credentialsProvider)
                            .region(awsRegion)
                            .serviceConfiguration(s3Config)
                            .httpClientBuilder(httpClientBuilder)
                            .overrideConfiguration(overrideConfig);
            if (endpointUri != null) {
                clientBuilder.endpointOverride(endpointUri);
            }
            S3Client s3Client = clientBuilder.build();

            NettyNioAsyncHttpClient.Builder asyncHttpClientBuilder =
                    NettyNioAsyncHttpClient.builder()
                            .maxConcurrency(maxConnections)
                            .connectionTimeout(connectionTimeout)
                            .readTimeout(socketTimeout);

            S3AsyncClientBuilder asyncClientBuilder =
                    S3AsyncClient.builder()
                            .credentialsProvider(credentialsProvider)
                            .region(awsRegion)
                            .serviceConfiguration(s3Config)
                            .httpClientBuilder(asyncHttpClientBuilder)
                            .overrideConfiguration(overrideConfig);
            if (endpointUri != null) {
                asyncClientBuilder.endpointOverride(endpointUri);
            }
            S3AsyncClient s3AsyncClient = asyncClientBuilder.build();

            S3TransferManager transferManager =
                    S3TransferManager.builder().s3Client(s3AsyncClient).build();

            return new S3ClientProvider(
                    s3Client,
                    s3AsyncClient,
                    transferManager,
                    encryptionConfig,
                    credentialsProvider,
                    stsClient);
        }

        private AwsCredentialsProvider buildBaseCredentialsProvider() {
            Credentials delegationTokenCredentials =
                    NativeS3DelegationTokenReceiver.getCredentials();

            if (delegationTokenCredentials != null) {
                return AwsCredentialsProviderChain.builder()
                        .credentialsProviders(
                                new DynamicTemporaryAWSCredentialsProvider(),
                                buildFallbackProvider())
                        .build();
            }
            return buildFallbackProvider();
        }

        private StsClient buildStsClient(AwsCredentialsProvider baseProvider, Region awsRegion) {
            return StsClient.builder().region(awsRegion).credentialsProvider(baseProvider).build();
        }

        private AwsCredentialsProvider buildAssumeRoleProvider(StsClient stsClient) {
            AssumeRoleRequest.Builder requestBuilder =
                    AssumeRoleRequest.builder()
                            .roleArn(assumeRoleArn)
                            .roleSessionName(assumeRoleSessionName)
                            .durationSeconds(assumeRoleSessionDurationSeconds);

            if (assumeRoleExternalId != null && !assumeRoleExternalId.isEmpty()) {
                requestBuilder.externalId(assumeRoleExternalId);
            }

            return StsAssumeRoleCredentialsProvider.builder()
                    .stsClient(stsClient)
                    .refreshRequest(requestBuilder.build())
                    .build();
        }

        private AwsCredentialsProvider buildFallbackProvider() {
            if (accessKey != null && secretKey != null) {
                return StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(accessKey, secretKey));
            }
            return DefaultCredentialsProvider.create();
        }

        private Region resolveRegion(@Nullable String explicitRegion) {
            if (explicitRegion != null && !explicitRegion.isEmpty()) {
                LOG.info("Using configured AWS region: {}", explicitRegion);
                return Region.of(explicitRegion);
            }

            try {
                Region region = DefaultAwsRegionProviderChain.builder().build().getRegion();
                LOG.info("Auto-detected AWS region: {}", region.id());
                return region;
            } catch (Exception e) {
                // Fall through to error
            }

            throw new IllegalArgumentException(
                    "AWS region could not be determined. Set 's3.region' in Flink configuration, "
                            + "AWS_REGION environment variable, or configure ~/.aws/config");
        }
    }
}
