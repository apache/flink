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
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.util.StringUtils;

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * Immutable, bucket-specific S3 configuration overrides.
 *
 * <p>Null values indicate inheritance from global configuration. Only explicitly configured values
 * are non-null. Configuration format: {@code s3.bucket.<bucket-name>.<property>}
 *
 * <p>Validates that credentials (access-key/secret-key) are either both set or both absent.
 */
@Internal
final class S3BucketConfig {

    private final String bucketName;
    @Nullable private final String region;
    @Nullable private final String endpoint;
    @Nullable private final Boolean pathStyleAccess;
    @Nullable private final String accessKey;
    @Nullable private final String secretKey;
    @Nullable private final String sseType;
    @Nullable private final String sseKmsKeyId;
    @Nullable private final String assumeRoleArn;
    @Nullable private final String assumeRoleExternalId;
    @Nullable private final String assumeRoleSessionName;
    @Nullable private final Integer assumeRoleSessionDurationSeconds;
    @Nullable private final String credentialsProvider;

    private S3BucketConfig(Builder builder) {
        this.bucketName = builder.bucketName;
        this.region = builder.region;
        this.endpoint = builder.endpoint;
        this.pathStyleAccess = builder.pathStyleAccess;
        this.accessKey = builder.accessKey;
        this.secretKey = builder.secretKey;
        this.sseType = builder.sseType;
        this.sseKmsKeyId = builder.sseKmsKeyId;
        this.assumeRoleArn = builder.assumeRoleArn;
        this.assumeRoleExternalId = builder.assumeRoleExternalId;
        this.assumeRoleSessionName = builder.assumeRoleSessionName;
        this.assumeRoleSessionDurationSeconds = builder.assumeRoleSessionDurationSeconds;
        this.credentialsProvider = builder.credentialsProvider;
    }

    String getBucketName() {
        return bucketName;
    }

    @Nullable
    String getRegion() {
        return region;
    }

    @Nullable
    String getEndpoint() {
        return endpoint;
    }

    @Nullable
    Boolean getPathStyleAccess() {
        return pathStyleAccess;
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
    String getSseType() {
        return sseType;
    }

    @Nullable
    String getSseKmsKeyId() {
        return sseKmsKeyId;
    }

    @Nullable
    String getAssumeRoleArn() {
        return assumeRoleArn;
    }

    @Nullable
    String getAssumeRoleExternalId() {
        return assumeRoleExternalId;
    }

    @Nullable
    String getAssumeRoleSessionName() {
        return assumeRoleSessionName;
    }

    @Nullable
    Integer getAssumeRoleSessionDurationSeconds() {
        return assumeRoleSessionDurationSeconds;
    }

    @Nullable
    String getCredentialsProvider() {
        return credentialsProvider;
    }

    boolean hasAnyOverride() {
        return region != null
                || endpoint != null
                || pathStyleAccess != null
                || accessKey != null
                || secretKey != null
                || sseType != null
                || sseKmsKeyId != null
                || assumeRoleArn != null
                || assumeRoleExternalId != null
                || assumeRoleSessionName != null
                || assumeRoleSessionDurationSeconds != null
                || credentialsProvider != null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        S3BucketConfig that = (S3BucketConfig) o;
        return Objects.equals(bucketName, that.bucketName)
                && Objects.equals(region, that.region)
                && Objects.equals(endpoint, that.endpoint)
                && Objects.equals(pathStyleAccess, that.pathStyleAccess)
                && Objects.equals(accessKey, that.accessKey)
                && Objects.equals(secretKey, that.secretKey)
                && Objects.equals(sseType, that.sseType)
                && Objects.equals(sseKmsKeyId, that.sseKmsKeyId)
                && Objects.equals(assumeRoleArn, that.assumeRoleArn)
                && Objects.equals(assumeRoleExternalId, that.assumeRoleExternalId)
                && Objects.equals(assumeRoleSessionName, that.assumeRoleSessionName)
                && Objects.equals(
                        assumeRoleSessionDurationSeconds, that.assumeRoleSessionDurationSeconds)
                && Objects.equals(credentialsProvider, that.credentialsProvider);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                bucketName,
                region,
                endpoint,
                pathStyleAccess,
                accessKey,
                secretKey,
                sseType,
                sseKmsKeyId,
                assumeRoleArn,
                assumeRoleExternalId,
                assumeRoleSessionName,
                assumeRoleSessionDurationSeconds,
                credentialsProvider);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("S3BucketConfig{");
        sb.append("bucket='").append(bucketName).append("'");
        if (region != null) {
            sb.append(", region='").append(region).append("'");
        }
        if (endpoint != null) {
            sb.append(", endpoint='").append(endpoint).append("'");
        }
        if (pathStyleAccess != null) {
            sb.append(", pathStyleAccess=").append(pathStyleAccess);
        }
        if (accessKey != null) {
            sb.append(", credentials=").append(GlobalConfiguration.HIDDEN_CONTENT);
        }
        if (sseType != null) {
            sb.append(", sseType='").append(sseType).append("'");
        }
        if (sseKmsKeyId != null) {
            sb.append(", sseKmsKeyId=").append(GlobalConfiguration.HIDDEN_CONTENT);
        }
        if (assumeRoleArn != null) {
            sb.append(", assumeRoleArn='").append(assumeRoleArn).append("'");
        }
        if (assumeRoleExternalId != null) {
            sb.append(", assumeRoleExternalId='").append(assumeRoleExternalId).append("'");
        }
        if (assumeRoleSessionName != null) {
            sb.append(", assumeRoleSessionName='").append(assumeRoleSessionName).append("'");
        }
        if (assumeRoleSessionDurationSeconds != null) {
            sb.append(", assumeRoleSessionDurationSeconds=")
                    .append(assumeRoleSessionDurationSeconds);
        }
        if (credentialsProvider != null) {
            sb.append(", credentialsProvider='").append(credentialsProvider).append("'");
        }
        sb.append('}');
        return sb.toString();
    }

    static Builder builder(String bucketName) {
        if (StringUtils.isNullOrWhitespaceOnly(bucketName)) {
            throw new IllegalArgumentException("Bucket name must not be null or empty");
        }
        return new Builder(bucketName);
    }

    static final class Builder {
        private final String bucketName;
        @Nullable private String region;
        @Nullable private String endpoint;
        @Nullable private Boolean pathStyleAccess;
        @Nullable private String accessKey;
        @Nullable private String secretKey;
        @Nullable private String sseType;
        @Nullable private String sseKmsKeyId;
        @Nullable private String assumeRoleArn;
        @Nullable private String assumeRoleExternalId;
        @Nullable private String assumeRoleSessionName;
        @Nullable private Integer assumeRoleSessionDurationSeconds;
        @Nullable private String credentialsProvider;

        private Builder(String bucketName) {
            this.bucketName = bucketName;
        }

        String getBucketName() {
            return bucketName;
        }

        Builder region(String region) {
            this.region = region;
            return this;
        }

        Builder endpoint(String endpoint) {
            this.endpoint = endpoint;
            return this;
        }

        Builder pathStyleAccess(boolean pathStyleAccess) {
            this.pathStyleAccess = pathStyleAccess;
            return this;
        }

        Builder accessKey(String accessKey) {
            this.accessKey = accessKey;
            return this;
        }

        Builder secretKey(String secretKey) {
            this.secretKey = secretKey;
            return this;
        }

        Builder sseType(String sseType) {
            this.sseType = sseType;
            return this;
        }

        Builder sseKmsKeyId(String sseKmsKeyId) {
            this.sseKmsKeyId = sseKmsKeyId;
            return this;
        }

        Builder assumeRoleArn(String assumeRoleArn) {
            this.assumeRoleArn = assumeRoleArn;
            return this;
        }

        Builder assumeRoleExternalId(String assumeRoleExternalId) {
            this.assumeRoleExternalId = assumeRoleExternalId;
            return this;
        }

        Builder assumeRoleSessionName(String assumeRoleSessionName) {
            this.assumeRoleSessionName = assumeRoleSessionName;
            return this;
        }

        Builder assumeRoleSessionDurationSeconds(int assumeRoleSessionDurationSeconds) {
            this.assumeRoleSessionDurationSeconds = assumeRoleSessionDurationSeconds;
            return this;
        }

        Builder credentialsProvider(String credentialsProvider) {
            this.credentialsProvider = credentialsProvider;
            return this;
        }

        S3BucketConfig build() {
            boolean hasAccessKey = !StringUtils.isNullOrWhitespaceOnly(accessKey);
            boolean hasSecretKey = !StringUtils.isNullOrWhitespaceOnly(secretKey);
            if (hasAccessKey != hasSecretKey) {
                throw new IllegalConfigurationException(
                        String.format(
                                "Bucket '%s': both 's3.bucket.%s.access-key' and "
                                        + "'s3.bucket.%s.secret-key' must be set together. "
                                        + "Found only %s.",
                                bucketName,
                                bucketName,
                                bucketName,
                                hasAccessKey ? "access-key" : "secret-key"));
            }
            return new S3BucketConfig(this);
        }
    }
}
