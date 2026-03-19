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

import javax.annotation.Nullable;

import java.util.Objects;

/** Bucket-level S3 config (endpoint, path-style, credentials, SSE, assume-role). */
@Internal
class S3BucketConfig {

    private final String bucketName;

    @Nullable private final String endpoint;

    private final boolean pathStyleAccess;

    @Nullable private final String region;

    @Nullable private final String accessKey;

    @Nullable private final String secretKey;

    @Nullable private final String sseType;

    @Nullable private final String sseKmsKeyId;

    @Nullable private final String assumeRoleArn;

    @Nullable private final String assumeRoleExternalId;

    private S3BucketConfig(Builder builder) {
        this.bucketName = Objects.requireNonNull(builder.bucketName, "bucketName is required");
        this.endpoint = builder.endpoint;
        this.pathStyleAccess = builder.pathStyleAccess;
        this.region = builder.region;
        this.accessKey = builder.accessKey;
        this.secretKey = builder.secretKey;
        this.sseType = builder.sseType;
        this.sseKmsKeyId = builder.sseKmsKeyId;
        this.assumeRoleArn = builder.assumeRoleArn;
        this.assumeRoleExternalId = builder.assumeRoleExternalId;
    }

    public String getBucketName() {
        return bucketName;
    }

    @Nullable
    public String getEndpoint() {
        return endpoint;
    }

    public boolean isPathStyleAccess() {
        return pathStyleAccess;
    }

    @Nullable
    public String getRegion() {
        return region;
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
    public String getSseType() {
        return sseType;
    }

    @Nullable
    public String getSseKmsKeyId() {
        return sseKmsKeyId;
    }

    @Nullable
    public String getAssumeRoleArn() {
        return assumeRoleArn;
    }

    @Nullable
    public String getAssumeRoleExternalId() {
        return assumeRoleExternalId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof S3BucketConfig)) {
            return false;
        }
        S3BucketConfig that = (S3BucketConfig) o;
        return pathStyleAccess == that.pathStyleAccess
                && bucketName.equals(that.bucketName)
                && Objects.equals(endpoint, that.endpoint)
                && Objects.equals(region, that.region)
                && Objects.equals(accessKey, that.accessKey)
                && Objects.equals(secretKey, that.secretKey)
                && Objects.equals(sseType, that.sseType)
                && Objects.equals(sseKmsKeyId, that.sseKmsKeyId)
                && Objects.equals(assumeRoleArn, that.assumeRoleArn)
                && Objects.equals(assumeRoleExternalId, that.assumeRoleExternalId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                bucketName,
                endpoint,
                pathStyleAccess,
                region,
                accessKey,
                secretKey,
                sseType,
                sseKmsKeyId,
                assumeRoleArn,
                assumeRoleExternalId);
    }

    @Override
    public String toString() {
        return "S3BucketConfig{"
                + "bucketName='"
                + bucketName
                + '\''
                + ", endpoint='"
                + endpoint
                + '\''
                + ", pathStyleAccess="
                + pathStyleAccess
                + ", region='"
                + region
                + '\''
                + '}';
    }

    public static Builder builder(String bucketName) {
        return new Builder(bucketName);
    }

    public static class Builder {
        private final String bucketName;

        @Nullable private String endpoint;

        private boolean pathStyleAccess = false;

        @Nullable private String region;

        @Nullable private String accessKey;

        @Nullable private String secretKey;

        @Nullable private String sseType;

        @Nullable private String sseKmsKeyId;

        @Nullable private String assumeRoleArn;

        @Nullable private String assumeRoleExternalId;

        public Builder(String bucketName) {
            this.bucketName = bucketName;
        }

        public Builder endpoint(@Nullable String endpoint) {
            this.endpoint = endpoint;
            return this;
        }

        public Builder pathStyleAccess(boolean pathStyleAccess) {
            this.pathStyleAccess = pathStyleAccess;
            return this;
        }

        public Builder region(@Nullable String region) {
            this.region = region;
            return this;
        }

        public Builder accessKey(@Nullable String accessKey) {
            this.accessKey = accessKey;
            return this;
        }

        public Builder secretKey(@Nullable String secretKey) {
            this.secretKey = secretKey;
            return this;
        }

        public Builder sseType(@Nullable String sseType) {
            this.sseType = sseType;
            return this;
        }

        public Builder sseKmsKeyId(@Nullable String sseKmsKeyId) {
            this.sseKmsKeyId = sseKmsKeyId;
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

        public S3BucketConfig build() {
            return new S3BucketConfig(this);
        }
    }
}
