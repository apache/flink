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

import org.apache.hadoop.conf.Configuration;

import javax.annotation.Nullable;

import java.net.URI;
import java.time.Duration;

/**
 * Builder for S3 client configuration that provides a fluent API and comprehensive validation. This
 * centralizes all configuration logic and makes it easier to test and maintain.
 */
@Internal
public class S3ConfigurationBuilder {

    // Connection settings
    private Duration connectionTimeout = Duration.ofMillis(200000);
    private Duration socketTimeout = Duration.ofMillis(200000);
    private int maxConnections = 96;

    // Retry settings
    private int maxRetries = 10;
    private Duration retryInterval = Duration.ofMillis(500);

    // Timeouts
    private Duration apiCallTimeout = Duration.ofMinutes(5);
    private Duration apiCallAttemptTimeout = Duration.ofMinutes(1);

    // Credentials
    private String accessKey;
    private String secretKey;
    private String sessionToken;

    // Regional settings
    private String region = "us-east-1";
    private URI endpoint;

    // Service settings
    private boolean pathStyleAccess = false;
    private boolean checksumValidation = true;

    // SSL settings
    private boolean sslEnabled = true;
    private boolean verifySslCertificates = true;
    private String trustStorePath;
    private String trustStorePassword;

    // Buffer settings
    private int bufferSize = 32 * 1024; // 32KB default
    
    // Hadoop configuration for credential provider access
    private org.apache.hadoop.conf.Configuration hadoopConfiguration;

    public static S3ConfigurationBuilder fromHadoopConfiguration(Configuration hadoopConfig) {
        S3ConfigurationBuilder builder = new S3ConfigurationBuilder();
        return builder.loadFromHadoop(hadoopConfig);
    }

    private S3ConfigurationBuilder loadFromHadoop(Configuration hadoopConfig) {
        // Load all configuration with proper defaults and validation
        this.connectionTimeout =
                parseDuration(hadoopConfig, "fs.s3a.connection.timeout", connectionTimeout);
        this.socketTimeout = parseDuration(hadoopConfig, "fs.s3a.socket.timeout", socketTimeout);
        this.maxConnections =
                parseInteger(hadoopConfig, "fs.s3a.connection.maximum", maxConnections, 1, 1000);

        this.maxRetries = parseInteger(hadoopConfig, "fs.s3a.retry.limit", maxRetries, 0, 50);
        this.retryInterval = parseDuration(hadoopConfig, "fs.s3a.retry.interval", retryInterval);

        this.apiCallTimeout = parseDuration(hadoopConfig, "fs.s3a.api.timeout", apiCallTimeout);
        this.apiCallAttemptTimeout =
                parseDuration(hadoopConfig, "fs.s3a.api.attempt.timeout", apiCallAttemptTimeout);

        // Credentials (secure)
        this.accessKey = hadoopConfig.get("fs.s3a.access.key");
        this.secretKey = hadoopConfig.get("fs.s3a.secret.key");
        this.sessionToken = hadoopConfig.get("fs.s3a.session.token");
        
        // Store the Hadoop configuration for credential provider access
        this.hadoopConfiguration = hadoopConfig;

        // Regional settings
        this.region = hadoopConfig.get("fs.s3a.endpoint.region", region);
        String endpointStr = hadoopConfig.get("fs.s3a.endpoint");
        if (endpointStr != null && !endpointStr.trim().isEmpty()) {
            // Ensure backward compatibility: add https:// if no scheme is provided
            String trimmedEndpoint = endpointStr.trim();
            if (!trimmedEndpoint.contains("://")) {
                trimmedEndpoint = "https://" + trimmedEndpoint;
            }
            this.endpoint = URI.create(trimmedEndpoint);
        }

        // Service settings
        this.pathStyleAccess = hadoopConfig.getBoolean("fs.s3a.path.style.access", pathStyleAccess);
        this.checksumValidation =
                hadoopConfig.getBoolean("fs.s3a.checksum.validation", checksumValidation);

        // SSL settings
        this.sslEnabled = hadoopConfig.getBoolean("fs.s3a.connection.ssl.enabled", sslEnabled);
        // Note: fs.s3a.ssl.channel.mode is not a boolean - it's an enum ("default_jsse", "openssl",
        // etc.)
        // Use the correct boolean config for SSL certificate verification
        this.verifySslCertificates =
                hadoopConfig.getBoolean("fs.s3a.connection.ssl.cert.verify", verifySslCertificates);
        this.trustStorePath = hadoopConfig.get("fs.s3a.ssl.truststore.path");
        this.trustStorePassword = hadoopConfig.get("fs.s3a.ssl.truststore.password");

        // Buffer settings
        this.bufferSize =
                parseInteger(hadoopConfig, "fs.s3a.block.size", bufferSize, 4096, 1024 * 1024);

        return this;
    }

    private Duration parseDuration(Configuration config, String key, Duration defaultValue) {
        try {
            String value = config.get(key);
            if (value == null) {
                return defaultValue;
            }

            // Handle common suffixes
            if (value.endsWith("s")) {
                return Duration.ofSeconds(Long.parseLong(value.substring(0, value.length() - 1)));
            } else if (value.endsWith("ms")) {
                return Duration.ofMillis(Long.parseLong(value.substring(0, value.length() - 2)));
            } else {
                return Duration.ofMillis(Long.parseLong(value));
            }
        } catch (Exception e) {
            return defaultValue;
        }
    }

    private int parseInteger(Configuration config, String key, int defaultValue, int min, int max) {
        try {
            int value = config.getInt(key, defaultValue);
            return Math.max(min, Math.min(max, value));
        } catch (Exception e) {
            return defaultValue;
        }
    }

    public S3Configuration build() {
        validate();
        return new S3Configuration(this);
    }

    private void validate() {
        if (connectionTimeout.toMillis() < 1000 || connectionTimeout.toMillis() > 600000) {
            throw new IllegalArgumentException("Connection timeout must be between 1s and 10min");
        }
        if (socketTimeout.toMillis() < 1000 || socketTimeout.toMillis() > 600000) {
            throw new IllegalArgumentException("Socket timeout must be between 1s and 10min");
        }
        if (maxConnections < 1 || maxConnections > 1000) {
            throw new IllegalArgumentException("Max connections must be between 1 and 1000");
        }
        if (maxRetries < 0 || maxRetries > 50) {
            throw new IllegalArgumentException("Max retries must be between 0 and 50");
        }
        if (region != null && !isValidAwsRegion(region)) {
            throw new IllegalArgumentException("Invalid AWS region: " + region);
        }
        if (endpoint != null && !isValidEndpointUrl(endpoint)) {
            throw new IllegalArgumentException("Invalid endpoint URL: " + endpoint);
        }
    }

    private boolean isValidAwsRegion(String region) {
        return region.matches("^[a-z0-9-]+$") && region.length() >= 3 && region.length() <= 20;
    }

    private boolean isValidEndpointUrl(URI endpoint) {
        String scheme = endpoint.getScheme();
        return (scheme != null && (scheme.equals("http") || scheme.equals("https")))
                && endpoint.getHost() != null
                && !endpoint.getHost().trim().isEmpty();
    }

    // Getters for the S3Configuration class
    Duration getConnectionTimeout() {
        return connectionTimeout;
    }

    Duration getSocketTimeout() {
        return socketTimeout;
    }

    int getMaxConnections() {
        return maxConnections;
    }

    int getMaxRetries() {
        return maxRetries;
    }

    Duration getRetryInterval() {
        return retryInterval;
    }

    Duration getApiCallTimeout() {
        return apiCallTimeout;
    }

    Duration getApiCallAttemptTimeout() {
        return apiCallAttemptTimeout;
    }

    @Nullable
    String getAccessKey() {
        return accessKey;
    }

    @Nullable
    String getSecretKey() {
        return secretKey;
    }

    @Nullable
    String getSessionToken() {
        return sessionToken;
    }

    String getRegion() {
        return region;
    }

    @Nullable
    URI getEndpoint() {
        return endpoint;
    }

    boolean isPathStyleAccess() {
        return pathStyleAccess;
    }

    boolean isChecksumValidation() {
        return checksumValidation;
    }

    boolean isSslEnabled() {
        return sslEnabled;
    }

    boolean isVerifySslCertificates() {
        return verifySslCertificates;
    }

    @Nullable
    String getTrustStorePath() {
        return trustStorePath;
    }

    @Nullable
    String getTrustStorePassword() {
        return trustStorePassword;
    }

    int getBufferSize() {
        return bufferSize;
    }
    
    @Nullable
    org.apache.hadoop.conf.Configuration getHadoopConfiguration() {
        return hadoopConfiguration;
    }
}
