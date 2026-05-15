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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.fs.s3native.token.DynamicTemporaryAWSCredentialsProvider;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

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
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.utils.SdkAutoCloseable;

import javax.annotation.Nullable;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Provider for S3 clients (sync and async). Handles credential management, delegation tokens, and
 * connection configuration.
 *
 * <p><b>Thread Safety:</b> This class is thread-safe. All fields are immutable or safely guarded by
 * atomic operations ({@link AtomicBoolean}).
 */
@Internal
class S3ClientProvider implements AutoCloseableAsync {

    private static final Logger LOG = LoggerFactory.getLogger(S3ClientProvider.class);

    private final S3Client s3Client;
    private final S3TransferManager transferManager;
    private final S3EncryptionConfig encryptionConfig;
    private final AwsCredentialsProvider credentialsProvider;
    @Nullable private final StsClient stsClient;
    private final Duration clientCloseTimeout;
    private final Duration connectionTimeout;
    private final Duration socketTimeout;
    private final Duration connectionMaxIdleTime;
    private final boolean pathStyleAccess;
    private final boolean chunkedEncoding;
    private final boolean checksumValidation;
    private final int maxConnections;
    private final int maxRetries;
    @Nullable private final String region;
    @Nullable private final String endpoint;
    @Nullable private final String assumeRoleArn;
    @Nullable private final String assumeRoleExternalId;
    @Nullable private final String assumeRoleSessionName;
    private final int assumeRoleSessionDurationSeconds;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private S3ClientProvider(
            S3Client s3Client,
            S3TransferManager transferManager,
            S3EncryptionConfig encryptionConfig,
            AwsCredentialsProvider credentialsProvider,
            @Nullable StsClient stsClient,
            Duration clientCloseTimeout,
            Duration connectionTimeout,
            Duration socketTimeout,
            Duration connectionMaxIdleTime,
            boolean pathStyleAccess,
            boolean chunkedEncoding,
            boolean checksumValidation,
            int maxConnections,
            int maxRetries,
            @Nullable String region,
            @Nullable String endpoint,
            @Nullable String assumeRoleArn,
            @Nullable String assumeRoleExternalId,
            @Nullable String assumeRoleSessionName,
            int assumeRoleSessionDurationSeconds) {
        this.s3Client = Preconditions.checkNotNull(s3Client, "s3Client must not be null");
        this.transferManager =
                Preconditions.checkNotNull(transferManager, "transferManager must not be null");
        this.encryptionConfig =
                Preconditions.checkNotNull(encryptionConfig, "encryptionConfig must not be null");
        this.credentialsProvider =
                Preconditions.checkNotNull(
                        credentialsProvider, "credentialsProvider must not be null");
        this.stsClient = stsClient;
        this.clientCloseTimeout =
                Preconditions.checkNotNull(
                        clientCloseTimeout, "clientCloseTimeout must not be null");
        this.connectionTimeout =
                Preconditions.checkNotNull(connectionTimeout, "connectionTimeout must not be null");
        this.socketTimeout =
                Preconditions.checkNotNull(socketTimeout, "socketTimeout must not be null");
        this.connectionMaxIdleTime =
                Preconditions.checkNotNull(
                        connectionMaxIdleTime, "connectionMaxIdleTime must not be null");
        this.pathStyleAccess = pathStyleAccess;
        this.chunkedEncoding = chunkedEncoding;
        this.checksumValidation = checksumValidation;
        this.maxConnections = maxConnections;
        this.maxRetries = maxRetries;
        this.region = region;
        this.endpoint = endpoint;
        this.assumeRoleArn = assumeRoleArn;
        this.assumeRoleExternalId = assumeRoleExternalId;
        this.assumeRoleSessionName = assumeRoleSessionName;
        this.assumeRoleSessionDurationSeconds = assumeRoleSessionDurationSeconds;
    }

    public S3Client getS3Client() {
        checkNotClosed();
        return s3Client;
    }

    public S3TransferManager getTransferManager() {
        checkNotClosed();
        return transferManager;
    }

    /** Returns the encryption configuration for S3 operations. */
    public S3EncryptionConfig getEncryptionConfig() {
        return encryptionConfig;
    }

    @VisibleForTesting
    AwsCredentialsProvider getCredentialsProvider() {
        return credentialsProvider;
    }

    @VisibleForTesting
    Duration getClientCloseTimeout() {
        return clientCloseTimeout;
    }

    @VisibleForTesting
    Duration getConnectionTimeout() {
        return connectionTimeout;
    }

    @VisibleForTesting
    Duration getSocketTimeout() {
        return socketTimeout;
    }

    @VisibleForTesting
    Duration getConnectionMaxIdleTime() {
        return connectionMaxIdleTime;
    }

    @VisibleForTesting
    boolean isPathStyleAccess() {
        return pathStyleAccess;
    }

    @VisibleForTesting
    boolean isChunkedEncoding() {
        return chunkedEncoding;
    }

    @VisibleForTesting
    boolean isChecksumValidation() {
        return checksumValidation;
    }

    @VisibleForTesting
    int getMaxConnections() {
        return maxConnections;
    }

    @VisibleForTesting
    int getMaxRetries() {
        return maxRetries;
    }

    @VisibleForTesting
    @Nullable
    String getRegion() {
        return region;
    }

    @VisibleForTesting
    @Nullable
    String getEndpoint() {
        return endpoint;
    }

    @VisibleForTesting
    @Nullable
    String getAssumeRoleArn() {
        return assumeRoleArn;
    }

    @VisibleForTesting
    @Nullable
    String getAssumeRoleExternalId() {
        return assumeRoleExternalId;
    }

    @VisibleForTesting
    @Nullable
    String getAssumeRoleSessionName() {
        return assumeRoleSessionName;
    }

    @VisibleForTesting
    int getAssumeRoleSessionDurationSeconds() {
        return assumeRoleSessionDurationSeconds;
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        if (!closed.compareAndSet(false, true)) {
            return CompletableFuture.completedFuture(null);
        }
        return CompletableFuture.runAsync(
                        () -> {
                            try {
                                transferManager.close();
                            } catch (Exception e) {
                                LOG.warn("Error closing S3 TransferManager", e);
                            }
                            try {
                                s3Client.close();
                            } catch (Exception e) {
                                LOG.warn("Error closing S3 sync client", e);
                            }
                            if (getCredentialsProvider() instanceof SdkAutoCloseable) {
                                try {
                                    ((SdkAutoCloseable) getCredentialsProvider()).close();
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
                .orTimeout(clientCloseTimeout.toSeconds(), TimeUnit.SECONDS)
                .exceptionally(
                        ex -> {
                            LOG.error("S3 client close timed out after {}", clientCloseTimeout, ex);
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
        private boolean chunkedEncoding = true;
        private boolean checksumValidation = true;
        private int maxConnections = 50;
        private Duration connectionTimeout = Duration.ofSeconds(60);
        private Duration socketTimeout = Duration.ofSeconds(60);
        private Duration connectionMaxIdleTime = Duration.ofSeconds(60);
        private int maxRetries = 3;
        private Duration clientCloseTimeout = Duration.ofSeconds(30);

        // AssumeRole configuration
        private String assumeRoleArn;
        private String assumeRoleExternalId;
        private String assumeRoleSessionName = "flink-s3-session";
        private int assumeRoleSessionDurationSeconds = 3600;

        // Encryption configuration
        private S3EncryptionConfig encryptionConfig = S3EncryptionConfig.none();

        // Custom credentials provider class names (comma-separated)
        @Nullable private String credentialsProviderClasses;

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

        public Builder chunkedEncoding(boolean chunkedEncoding) {
            this.chunkedEncoding = chunkedEncoding;
            return this;
        }

        public Builder checksumValidation(boolean checksumValidation) {
            this.checksumValidation = checksumValidation;
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

        public Builder connectionMaxIdleTime(Duration connectionMaxIdleTime) {
            this.connectionMaxIdleTime = connectionMaxIdleTime;
            return this;
        }

        public Builder clientCloseTimeout(Duration clientCloseTimeout) {
            this.clientCloseTimeout = clientCloseTimeout;
            return this;
        }

        public Builder maxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
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

        public Builder assumeRoleSessionName(String assumeRoleSessionName) {
            this.assumeRoleSessionName =
                    Preconditions.checkNotNull(
                            assumeRoleSessionName, "assumeRoleSessionName must not be null");
            return this;
        }

        public Builder assumeRoleSessionDurationSeconds(int assumeRoleSessionDurationSeconds) {
            Preconditions.checkArgument(
                    assumeRoleSessionDurationSeconds > 0,
                    "assumeRoleSessionDurationSeconds must be greater than zero");
            this.assumeRoleSessionDurationSeconds = assumeRoleSessionDurationSeconds;
            return this;
        }

        public Builder encryptionConfig(S3EncryptionConfig encryptionConfig) {
            this.encryptionConfig =
                    Preconditions.checkNotNull(
                            encryptionConfig, "encryptionConfig must not be null");
            return this;
        }

        public Builder credentialsProviderClasses(@Nullable String credentialsProviderClasses) {
            this.credentialsProviderClasses = credentialsProviderClasses;
            return this;
        }

        S3ClientProvider build() {
            if (endpoint == null) {
                endpoint = System.getProperty("s3.endpoint");
            }
            String pathStyleProp = System.getProperty("s3.path.style.access");
            if (pathStyleProp != null) {
                pathStyleAccess = Boolean.parseBoolean(pathStyleProp);
            }

            URI endpointUri = (endpoint != null) ? URI.create(endpoint) : null;

            Region awsRegion = resolveRegion(region);
            StsClient stsClient = null;
            AwsCredentialsProvider credentialsProvider;

            AwsCredentialsProvider baseProvider = buildBaseCredentialsProvider();
            if (!StringUtils.isNullOrWhitespaceOnly(assumeRoleArn)) {
                stsClient = buildStsClient(baseProvider, awsRegion);
                credentialsProvider = buildAssumeRoleProvider(stsClient);
            } else {
                credentialsProvider = baseProvider;
            }

            S3Configuration s3Config =
                    S3Configuration.builder()
                            .pathStyleAccessEnabled(pathStyleAccess)
                            .chunkedEncodingEnabled(chunkedEncoding)
                            .checksumValidationEnabled(checksumValidation)
                            .build();

            ClientOverrideConfiguration overrideConfig =
                    ClientOverrideConfiguration.builder()
                            .retryPolicy(RetryPolicy.builder().numRetries(maxRetries).build())
                            .build();

            ApacheHttpClient.Builder httpClientBuilder =
                    ApacheHttpClient.builder()
                            .maxConnections(maxConnections)
                            .connectionTimeout(connectionTimeout)
                            .socketTimeout(socketTimeout)
                            .tcpKeepAlive(true)
                            .connectionMaxIdleTime(connectionMaxIdleTime);

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

            S3AsyncClientBuilder asyncClientBuilder =
                    S3AsyncClient.builder()
                            .credentialsProvider(credentialsProvider)
                            .region(awsRegion)
                            .serviceConfiguration(s3Config)
                            .httpClientBuilder(
                                    NettyNioAsyncHttpClient.builder()
                                            .maxConcurrency(maxConnections)
                                            .connectionTimeout(connectionTimeout)
                                            .readTimeout(socketTimeout)
                                            .connectionAcquisitionTimeout(connectionTimeout))
                            .overrideConfiguration(overrideConfig);
            if (endpointUri != null) {
                asyncClientBuilder.endpointOverride(endpointUri);
            }
            S3TransferManager transferManager =
                    S3TransferManager.builder().s3Client(asyncClientBuilder.build()).build();

            return new S3ClientProvider(
                    s3Client,
                    transferManager,
                    encryptionConfig,
                    credentialsProvider,
                    stsClient,
                    clientCloseTimeout,
                    connectionTimeout,
                    socketTimeout,
                    connectionMaxIdleTime,
                    pathStyleAccess,
                    chunkedEncoding,
                    checksumValidation,
                    maxConnections,
                    maxRetries,
                    region,
                    endpoint,
                    assumeRoleArn,
                    assumeRoleExternalId,
                    assumeRoleSessionName,
                    assumeRoleSessionDurationSeconds);
        }

        private AwsCredentialsProvider buildBaseCredentialsProvider() {
            List<AwsCredentialsProvider> chain = new ArrayList<>();

            if (!StringUtils.isNullOrWhitespaceOnly(credentialsProviderClasses)) {
                for (String name : credentialsProviderClasses.split(",")) {
                    String trimmed = name.trim();
                    if (!trimmed.isEmpty()) {
                        chain.add(instantiateCredentialsProvider(trimmed));
                    }
                }
                if (chain.isEmpty()) {
                    throw new IllegalArgumentException(
                            "fs.s3.aws.credentials.provider is set but contains no valid provider class names");
                }
            }

            if (accessKey != null && secretKey != null) {
                chain.add(
                        StaticCredentialsProvider.create(
                                AwsBasicCredentials.create(accessKey, secretKey)));
            }

            chain.add(new DynamicTemporaryAWSCredentialsProvider());
            chain.add(DefaultCredentialsProvider.builder().build());

            LOG.info(
                    "Using credentials provider chain: {}",
                    chain.stream()
                            .map(p -> p.getClass().getSimpleName())
                            .collect(Collectors.joining(" -> ")));

            return AwsCredentialsProviderChain.builder().credentialsProviders(chain).build();
        }

        /**
         * Instantiates an {@link AwsCredentialsProvider} from a class name. Accepts fully-qualified
         * SDK v2 class names or simple names resolved from the {@code
         * software.amazon.awssdk.auth.credentials} package (e.g. {@code
         * AnonymousCredentialsProvider}).
         */
        private AwsCredentialsProvider instantiateCredentialsProvider(String className) {
            String resolvedClassName = resolveProviderClassName(className);
            try {
                Class<?> clazz = Class.forName(resolvedClassName);
                if (!AwsCredentialsProvider.class.isAssignableFrom(clazz)) {
                    throw new IllegalArgumentException(
                            "Class "
                                    + resolvedClassName
                                    + " does not implement AwsCredentialsProvider");
                }

                try {
                    Method createMethod = clazz.getMethod("create");
                    if (Modifier.isStatic(createMethod.getModifiers())
                            && AwsCredentialsProvider.class.isAssignableFrom(
                                    createMethod.getReturnType())) {
                        return (AwsCredentialsProvider) createMethod.invoke(null);
                    }
                } catch (NoSuchMethodException ignored) {
                }

                return (AwsCredentialsProvider) clazz.getDeclaredConstructor().newInstance();
            } catch (Exception e) {
                throw new IllegalArgumentException(
                        "Failed to instantiate credentials provider: " + resolvedClassName, e);
            }
        }

        private static String resolveProviderClassName(String className) {
            if (!className.contains(".")) {
                return "software.amazon.awssdk.auth.credentials." + className;
            }
            return className;
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

            if (!StringUtils.isNullOrWhitespaceOnly(assumeRoleExternalId)) {
                requestBuilder.externalId(assumeRoleExternalId);
            }

            return StsAssumeRoleCredentialsProvider.builder()
                    .stsClient(stsClient)
                    .refreshRequest(requestBuilder.build())
                    .build();
        }

        private Region resolveRegion(@Nullable String explicitRegion) {
            if (!StringUtils.isNullOrWhitespaceOnly(explicitRegion)) {
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
