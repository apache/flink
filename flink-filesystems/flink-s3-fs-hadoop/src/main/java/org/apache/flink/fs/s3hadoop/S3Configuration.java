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

import javax.annotation.Nullable;

import java.net.URI;
import java.time.Duration;
import java.util.Objects;

/**
 * Immutable configuration object for S3 client settings. This centralizes all S3 configuration and
 * provides a clean, type-safe API for accessing configuration values.
 */
@Internal
public final class S3Configuration {

    // Connection settings
    private final Duration connectionTimeout;
    private final Duration socketTimeout;
    private final int maxConnections;

    // Retry settings
    private final int maxRetries;
    private final Duration retryInterval;

    // Timeouts
    private final Duration apiCallTimeout;
    private final Duration apiCallAttemptTimeout;

    // Credentials
    private final String accessKey;
    private final String secretKey;
    private final String sessionToken;

    // Regional settings
    private final String region;
    private final URI endpoint;

    // Service settings
    private final boolean pathStyleAccess;
    private final boolean checksumValidation;

    // SSL settings
    private final boolean sslEnabled;
    private final boolean verifySslCertificates;
    private final String trustStorePath;
    private final String trustStorePassword;

    // Buffer settings
    private final int bufferSize;

    S3Configuration(S3ConfigurationBuilder builder) {
        this.connectionTimeout = builder.getConnectionTimeout();
        this.socketTimeout = builder.getSocketTimeout();
        this.maxConnections = builder.getMaxConnections();
        this.maxRetries = builder.getMaxRetries();
        this.retryInterval = builder.getRetryInterval();
        this.apiCallTimeout = builder.getApiCallTimeout();
        this.apiCallAttemptTimeout = builder.getApiCallAttemptTimeout();
        this.accessKey = builder.getAccessKey();
        this.secretKey = builder.getSecretKey();
        this.sessionToken = builder.getSessionToken();
        this.region = builder.getRegion();
        this.endpoint = builder.getEndpoint();
        this.pathStyleAccess = builder.isPathStyleAccess();
        this.checksumValidation = builder.isChecksumValidation();
        this.sslEnabled = builder.isSslEnabled();
        this.verifySslCertificates = builder.isVerifySslCertificates();
        this.trustStorePath = builder.getTrustStorePath();
        this.trustStorePassword = builder.getTrustStorePassword();
        this.bufferSize = builder.getBufferSize();
    }

    // Getters
    public Duration getConnectionTimeout() {
        return connectionTimeout;
    }

    public Duration getSocketTimeout() {
        return socketTimeout;
    }

    public int getMaxConnections() {
        return maxConnections;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public Duration getRetryInterval() {
        return retryInterval;
    }

    public Duration getApiCallTimeout() {
        return apiCallTimeout;
    }

    public Duration getApiCallAttemptTimeout() {
        return apiCallAttemptTimeout;
    }

    @Nullable
    public String getAccessKey() {
        return accessKey;
    }

    @Nullable
    public String getSecretKey() {
        return secretKey;
    }

    @Nullable
    public String getSessionToken() {
        return sessionToken;
    }

    public String getRegion() {
        return region;
    }

    @Nullable
    public URI getEndpoint() {
        return endpoint;
    }

    public boolean isPathStyleAccess() {
        return pathStyleAccess;
    }

    public boolean isChecksumValidation() {
        return checksumValidation;
    }

    public boolean isSslEnabled() {
        return sslEnabled;
    }

    public boolean isVerifySslCertificates() {
        return verifySslCertificates;
    }

    @Nullable
    public String getTrustStorePath() {
        return trustStorePath;
    }

    @Nullable
    public String getTrustStorePassword() {
        return trustStorePassword;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    /**
     * Generates a configuration hash for caching purposes. This should be consistent across
     * different instances with the same configuration.
     */
    public String getConfigurationHash() {
        return Integer.toHexString(
                Objects.hash(
                        connectionTimeout,
                        socketTimeout,
                        maxConnections,
                        maxRetries,
                        retryInterval,
                        apiCallTimeout,
                        apiCallAttemptTimeout,
                        accessKey,
                        secretKey,
                        sessionToken,
                        region,
                        endpoint,
                        pathStyleAccess,
                        checksumValidation,
                        sslEnabled,
                        verifySslCertificates,
                        trustStorePath,
                        trustStorePassword,
                        bufferSize));
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        S3Configuration that = (S3Configuration) obj;
        return maxConnections == that.maxConnections
                && maxRetries == that.maxRetries
                && pathStyleAccess == that.pathStyleAccess
                && checksumValidation == that.checksumValidation
                && sslEnabled == that.sslEnabled
                && verifySslCertificates == that.verifySslCertificates
                && bufferSize == that.bufferSize
                && Objects.equals(connectionTimeout, that.connectionTimeout)
                && Objects.equals(socketTimeout, that.socketTimeout)
                && Objects.equals(retryInterval, that.retryInterval)
                && Objects.equals(apiCallTimeout, that.apiCallTimeout)
                && Objects.equals(apiCallAttemptTimeout, that.apiCallAttemptTimeout)
                && Objects.equals(accessKey, that.accessKey)
                && Objects.equals(secretKey, that.secretKey)
                && Objects.equals(sessionToken, that.sessionToken)
                && Objects.equals(region, that.region)
                && Objects.equals(endpoint, that.endpoint)
                && Objects.equals(trustStorePath, that.trustStorePath)
                && Objects.equals(trustStorePassword, that.trustStorePassword);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                connectionTimeout,
                socketTimeout,
                maxConnections,
                maxRetries,
                retryInterval,
                apiCallTimeout,
                apiCallAttemptTimeout,
                accessKey,
                secretKey,
                sessionToken,
                region,
                endpoint,
                pathStyleAccess,
                checksumValidation,
                sslEnabled,
                verifySslCertificates,
                trustStorePath,
                trustStorePassword,
                bufferSize);
    }

    @Override
    public String toString() {
        return "S3Configuration{"
                + "connectionTimeout="
                + connectionTimeout
                + ", socketTimeout="
                + socketTimeout
                + ", maxConnections="
                + maxConnections
                + ", maxRetries="
                + maxRetries
                + ", retryInterval="
                + retryInterval
                + ", region='"
                + region
                + '\''
                + ", endpoint="
                + endpoint
                + ", pathStyleAccess="
                + pathStyleAccess
                + ", checksumValidation="
                + checksumValidation
                + ", sslEnabled="
                + sslEnabled
                + ", bufferSize="
                + bufferSize
                +
                // Note: Don't include credentials in toString for security
                '}';
    }
}
