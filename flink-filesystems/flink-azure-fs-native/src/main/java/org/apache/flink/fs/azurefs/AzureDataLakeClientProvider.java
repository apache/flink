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

package org.apache.flink.fs.azurefs;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.StringUtils;

import com.azure.core.http.HttpClient;
import com.azure.core.http.okhttp.OkHttpAsyncHttpClientBuilder;
import com.azure.core.http.policy.ExponentialBackoffOptions;
import com.azure.core.http.policy.RetryOptions;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import okhttp3.ConnectionPool;
import okhttp3.OkHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

import java.net.URI;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Handles credential management and connection configuration. The underlying {@link
 * DataLakeServiceClient} is thread-safe and does not require explicit close.
 *
 * <p>This provider manages an {@link OkHttpClient} that owns background threads (connection pool
 * cleanup and dispatcher executor service). The OkHttpClient uses daemon threads by default, so it
 * will not prevent JVM shutdown. Since Flink caches FileSystem instances globally (typically one
 * per scheme+authority), the resource footprint is bounded by {@link
 * AzureFileSystemOptions#HTTP_MAX_IDLE_CONNECTIONS} and {@link
 * AzureFileSystemOptions#HTTP_KEEP_ALIVE_DURATION}. Flink's FileSystem base class does not have a
 * close lifecycle hook, so the OkHttpClient is not explicitly closed.
 */
@ThreadSafe
final class AzureDataLakeClientProvider {

    private static final Logger LOG = LoggerFactory.getLogger(AzureDataLakeClientProvider.class);

    /** Default Azure public cloud DFS endpoint URL format. */
    static final String DEFAULT_DFS_ENDPOINT_FORMAT = "https://%s.dfs.core.windows.net";

    private final DataLakeServiceClient dataLakeServiceClient;

    private AzureDataLakeClientProvider(final DataLakeServiceClient dataLakeServiceClient) {
        this.dataLakeServiceClient = checkNotNull(dataLakeServiceClient);
    }

    /**
     * Returns the account URL configured on the underlying service client.
     *
     * @return the account URL
     */
    String getAccountUrl() {
        return dataLakeServiceClient.getAccountUrl();
    }

    /**
     * Returns a synchronous DataLake file system client for the specified file system (container).
     *
     * @param fileSystemName the name of the file system (container)
     * @return the sync DataLake file system client
     */
    DataLakeFileSystemClient getFileSystemClient(final String fileSystemName) {
        return dataLakeServiceClient.getFileSystemClient(fileSystemName);
    }

    /**
     * Creates a new builder for configuring an Azure DataLake client provider.
     *
     * @return a new builder instance
     */
    static Builder builder() {
        return new Builder();
    }

    /**
     * Extracts the storage account name from an ABFSS URI.
     *
     * <p>Expected format: {@code abfss://container@account.dfs.core.windows.net/path}
     *
     * <p>Only the {@code abfss} (TLS-enabled) scheme is supported. The plain {@code abfs} scheme is
     * not supported. The storage account must have Hierarchical Namespace (HNS) enabled.
     *
     * @param fsUri the filesystem URI with the "abfss" scheme
     * @return the account name extracted from the URI host (substring before the first dot)
     * @throws NullPointerException if {@code fsUri} is null
     * @throws IllegalArgumentException if the scheme is not "abfss", the host is null, or the host
     *     contains no dot
     * @see <a
     *     href="https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction-abfs-uri">
     *     Azure Data Lake Storage Gen2 URI scheme</a>
     */
    @VisibleForTesting
    static String extractAccountNameFromUri(final URI fsUri) {
        checkNotNull(fsUri, "fsUri must not be null");
        checkArgument(
                "abfss".equals(fsUri.getScheme()),
                "Expected abfss scheme, got: %s",
                fsUri.getScheme());
        checkArgument(fsUri.getHost() != null, "URI host must not be null: %s", fsUri);
        final String host = fsUri.getHost();
        final int dotIndex = host.indexOf('.');
        checkArgument(dotIndex > 0, "URI host must contain a dot: %s", host);
        return host.substring(0, dotIndex);
    }

    /** Builder for creating {@link AzureDataLakeClientProvider} instances. */
    @NotThreadSafe
    static final class Builder {

        @Nullable private String accountName;
        @Nullable private String accountKey;

        private int maxRetries = AzureFileSystemOptions.RETRY_MAX_RETRIES.defaultValue();
        private Duration baseDelay = AzureFileSystemOptions.RETRY_BASE_DELAY.defaultValue();
        private Duration maxDelay = AzureFileSystemOptions.RETRY_MAX_DELAY.defaultValue();

        private Duration connectionTimeout =
                AzureFileSystemOptions.CONNECTION_TIMEOUT.defaultValue();
        private Duration readTimeout = AzureFileSystemOptions.READ_TIMEOUT.defaultValue();

        private int maxIdleConnections =
                AzureFileSystemOptions.HTTP_MAX_IDLE_CONNECTIONS.defaultValue();
        private Duration keepAliveDuration =
                AzureFileSystemOptions.HTTP_KEEP_ALIVE_DURATION.defaultValue();

        /**
         * Sets the Azure Storage account name.
         *
         * @param accountName the account name
         * @return this builder
         * @throws IllegalArgumentException if {@code accountName} is blank
         */
        Builder accountName(final String accountName) {
            this.accountName = requireNonBlankStripped(accountName, "accountName");
            return this;
        }

        /**
         * Sets the Azure Storage account key.
         *
         * @param accountKey the account key
         * @return this builder
         * @throws IllegalArgumentException if {@code accountKey} is blank
         */
        Builder accountKey(final String accountKey) {
            this.accountKey = requireNonBlankStripped(accountKey, "accountKey");
            return this;
        }

        /**
         * Sets the maximum number of retry attempts for transient failures.
         *
         * @param maxRetries the maximum number of retries (0 to disable)
         * @return this builder
         * @throws IllegalArgumentException if maxRetries is negative
         */
        Builder maxRetries(final int maxRetries) {
            checkArgument(maxRetries >= 0, "maxRetries must be non-negative, got: %s", maxRetries);
            this.maxRetries = maxRetries;
            return this;
        }

        /**
         * Sets the base delay for exponential backoff retry strategy.
         *
         * @param baseDelay the base delay
         * @return this builder
         * @throws IllegalArgumentException if baseDelay is not positive
         */
        Builder baseDelay(final Duration baseDelay) {
            this.baseDelay = requirePositiveDuration(baseDelay, "baseDelay");
            return this;
        }

        /**
         * Sets the maximum delay between retry attempts.
         *
         * @param maxDelay the maximum delay
         * @return this builder
         * @throws IllegalArgumentException if maxDelay is not positive
         */
        Builder maxDelay(final Duration maxDelay) {
            this.maxDelay = requirePositiveDuration(maxDelay, "maxDelay");
            return this;
        }

        /**
         * Sets the connection timeout for the HTTP client.
         *
         * @param connectionTimeout the connection timeout
         * @return this builder
         * @throws IllegalArgumentException if connectionTimeout is not positive
         */
        Builder connectionTimeout(final Duration connectionTimeout) {
            this.connectionTimeout =
                    requirePositiveDuration(connectionTimeout, "connectionTimeout");
            return this;
        }

        /**
         * Sets the read timeout for the HTTP client.
         *
         * @param readTimeout the read timeout
         * @return this builder
         * @throws IllegalArgumentException if readTimeout is not positive
         */
        Builder readTimeout(final Duration readTimeout) {
            this.readTimeout = requirePositiveDuration(readTimeout, "readTimeout");
            return this;
        }

        /**
         * Sets the maximum number of idle connections in the HTTP connection pool.
         *
         * @param maxIdleConnections the maximum number of idle connections
         * @return this builder
         * @throws IllegalArgumentException if maxIdleConnections is negative
         */
        Builder maxIdleConnections(final int maxIdleConnections) {
            checkArgument(
                    maxIdleConnections >= 0,
                    "maxIdleConnections must be non-negative, got: %s",
                    maxIdleConnections);
            this.maxIdleConnections = maxIdleConnections;
            return this;
        }

        /**
         * Sets the keep-alive duration for idle HTTP connections. Connections idle longer than this
         * are evicted.
         *
         * @param keepAliveDuration the keep-alive duration
         * @return this builder
         * @throws IllegalArgumentException if keepAliveDuration is not positive
         */
        Builder keepAliveDuration(final Duration keepAliveDuration) {
            this.keepAliveDuration =
                    requirePositiveDuration(keepAliveDuration, "keepAliveDuration");
            return this;
        }

        /**
         * Builds the Azure DataLake client provider with the configured settings.
         *
         * <p>Two authentication modes are supported:
         *
         * <ul>
         *   <li>{@code accountName + accountKey} — SharedKey authentication
         *   <li>{@code accountName} only — DefaultAzureCredential (auto-detects workload identity,
         *       managed identity, etc.)
         * </ul>
         *
         * @return a new client provider instance
         * @throws IllegalArgumentException if no valid credentials are configured
         */
        AzureDataLakeClientProvider build() {
            checkArgument(
                    baseDelay.compareTo(maxDelay) <= 0,
                    "baseDelay (%s) must not exceed maxDelay (%s)",
                    baseDelay,
                    maxDelay);

            checkArgument(
                    accountName != null,
                    "Azure Storage credentials not configured. "
                            + "Provide account-name + account-key for SharedKey auth, "
                            + "or account-name alone for DefaultAzureCredential.");

            final ConnectionPool connectionPool =
                    new ConnectionPool(
                            maxIdleConnections,
                            keepAliveDuration.toMillis(),
                            TimeUnit.MILLISECONDS);

            final OkHttpClient okHttpClient =
                    new OkHttpClient.Builder().connectionPool(connectionPool).build();

            // Disable overall response timeout (Duration.ZERO = no timeout).
            //
            // Why: non-zero values cause OkHttp to allocate a Watchdog AsyncTimeout
            // per request. Under high request volume this leads to OOM.
            //
            // Safety: stalled requests are still terminated by readTimeout (no bytes
            // received within the deadline) and connectionTimeout (TCP handshake
            // stalls). The Azure SDK retry policy (maxRetries + exponential backoff)
            // provides an additional upper bound. The only unprotected scenario is a
            // "slow drip" where the server sends just enough bytes to reset the
            // readTimeout indefinitely — this does not occur with Azure Storage.
            final HttpClient httpClient =
                    new OkHttpAsyncHttpClientBuilder(okHttpClient)
                            .connectionTimeout(connectionTimeout)
                            .readTimeout(readTimeout)
                            .responseTimeout(Duration.ZERO)
                            .build();

            final RetryOptions retryOptions =
                    new RetryOptions(
                            new ExponentialBackoffOptions()
                                    .setMaxRetries(maxRetries)
                                    .setBaseDelay(baseDelay)
                                    .setMaxDelay(maxDelay));

            LOG.debug(
                    "Configured HTTP client - retries: {}, baseDelay: {}, maxDelay: {}, "
                            + "maxIdleConnections: {}, keepAliveDuration: {}",
                    maxRetries,
                    baseDelay,
                    maxDelay,
                    maxIdleConnections,
                    keepAliveDuration);

            final DataLakeServiceClient dataLakeClient =
                    buildDataLakeClient(httpClient, retryOptions);

            LOG.info("Created Azure DataLake client provider - account: {}", accountName);

            return new AzureDataLakeClientProvider(dataLakeClient);
        }

        private DataLakeServiceClient buildDataLakeClient(
                final HttpClient httpClient, final RetryOptions retryOptions) {
            final DataLakeServiceClientBuilder builder =
                    new DataLakeServiceClientBuilder()
                            .httpClient(httpClient)
                            .retryOptions(retryOptions)
                            .endpoint(String.format(DEFAULT_DFS_ENDPOINT_FORMAT, accountName));

            if (accountKey != null) {
                final StorageSharedKeyCredential credential =
                        new StorageSharedKeyCredential(accountName, accountKey);
                builder.credential(credential);
            } else {
                LOG.info(
                        "No account key provided, using DefaultAzureCredential "
                                + "(auto-detects workload identity, managed identity, etc.)");
                builder.credential(new DefaultAzureCredentialBuilder().build());
            }

            return builder.buildClient();
        }
    }

    /**
     * Validates that a Duration value is non-null and positive (greater than zero).
     *
     * @param value the value to validate
     * @param name the parameter name for error messages
     * @return the validated duration
     * @throws NullPointerException if {@code value} is null
     * @throws IllegalArgumentException if {@code value} is zero or negative
     */
    private static Duration requirePositiveDuration(final Duration value, final String name) {
        checkNotNull(value, "%s must not be null", name);
        checkArgument(
                !value.isNegative() && !value.isZero(),
                "%s must be positive, got: %s",
                name,
                value);
        return value;
    }

    private static String requireNonBlankStripped(final String value, final String name) {
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(value), "%s must not be blank", name);
        return value.strip();
    }
}
