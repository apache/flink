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

package org.apache.flink.fs.s3.common.model;

import org.apache.flink.annotation.Internal;

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * SDK-agnostic representation of the result of completing a multipart upload.
 *
 * <p>This class maintains compatibility with both AWS SDK v1 {@code CompleteMultipartUploadResult}
 * and AWS SDK v2 {@code CompleteMultipartUploadResponse}, allowing the base S3 filesystem
 * implementation to work with either SDK version.
 */
@Internal
public final class FlinkCompleteMultipartUploadResult {

    @Nullable private final String bucket;
    @Nullable private final String key;
    @Nullable private final String eTag;
    @Nullable private final String location;
    @Nullable private final String versionId;

    private FlinkCompleteMultipartUploadResult(Builder builder) {
        this.bucket = builder.bucket;
        this.key = builder.key;
        this.eTag = builder.eTag;
        this.location = builder.location;
        this.versionId = builder.versionId;
    }

    /** Returns the bucket name. */
    @Nullable
    public String getBucket() {
        return bucket;
    }

    /** Returns the object key. */
    @Nullable
    public String getKey() {
        return key;
    }

    /** Returns the ETag of the completed object. */
    @Nullable
    public String getETag() {
        return eTag;
    }

    /** Returns the location (URL) of the completed object. */
    @Nullable
    public String getLocation() {
        return location;
    }

    /** Returns the version ID of the object (if versioning is enabled). */
    @Nullable
    public String getVersionId() {
        return versionId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FlinkCompleteMultipartUploadResult that = (FlinkCompleteMultipartUploadResult) o;
        return Objects.equals(bucket, that.bucket)
                && Objects.equals(key, that.key)
                && Objects.equals(eTag, that.eTag)
                && Objects.equals(location, that.location)
                && Objects.equals(versionId, that.versionId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bucket, key, eTag, location, versionId);
    }

    @Override
    public String toString() {
        return "FlinkCompleteMultipartUploadResult{"
                + "bucket='"
                + bucket
                + '\''
                + ", key='"
                + key
                + '\''
                + ", eTag='"
                + eTag
                + '\''
                + ", location='"
                + location
                + '\''
                + ", versionId='"
                + versionId
                + '\''
                + '}';
    }

    /** Creates a new builder. */
    public static Builder builder() {
        return new Builder();
    }

    /** Builder for {@link FlinkCompleteMultipartUploadResult}. */
    public static class Builder {
        private String bucket;
        private String key;
        private String eTag;
        private String location;
        private String versionId;

        private Builder() {}

        public Builder bucket(String bucket) {
            this.bucket = bucket;
            return this;
        }

        public Builder key(String key) {
            this.key = key;
            return this;
        }

        public Builder eTag(String eTag) {
            this.eTag = eTag;
            return this;
        }

        public Builder location(String location) {
            this.location = location;
            return this;
        }

        public Builder versionId(String versionId) {
            this.versionId = versionId;
            return this;
        }

        public FlinkCompleteMultipartUploadResult build() {
            return new FlinkCompleteMultipartUploadResult(this);
        }
    }
}
