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

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * SDK-agnostic representation of S3 object metadata.
 *
 * <p>This class maintains compatibility with both AWS SDK v1 {@code ObjectMetadata} and AWS SDK v2
 * {@code HeadObjectResponse}/{@code GetObjectResponse}, allowing the base S3 filesystem
 * implementation to work with either SDK version.
 */
@Internal
public final class FlinkObjectMetadata {

    private final long contentLength;
    @Nullable private final String contentType;
    @Nullable private final String eTag;
    @Nullable private final Date lastModified;
    private final Map<String, String> userMetadata;

    private FlinkObjectMetadata(Builder builder) {
        this.contentLength = builder.contentLength;
        this.contentType = builder.contentType;
        this.eTag = builder.eTag;
        this.lastModified = builder.lastModified;
        this.userMetadata =
                builder.userMetadata != null
                        ? Collections.unmodifiableMap(new HashMap<>(builder.userMetadata))
                        : Collections.emptyMap();
    }

    /** Returns the size of the object in bytes. */
    public long getContentLength() {
        return contentLength;
    }

    /** Returns the content type of the object. */
    @Nullable
    public String getContentType() {
        return contentType;
    }

    /** Returns the ETag of the object. */
    @Nullable
    public String getETag() {
        return eTag;
    }

    /** Returns the last modified date of the object. */
    @Nullable
    public Date getLastModified() {
        return lastModified;
    }

    /** Returns the user-defined metadata for the object. */
    public Map<String, String> getUserMetadata() {
        return userMetadata;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FlinkObjectMetadata that = (FlinkObjectMetadata) o;
        return contentLength == that.contentLength
                && Objects.equals(contentType, that.contentType)
                && Objects.equals(eTag, that.eTag)
                && Objects.equals(lastModified, that.lastModified)
                && userMetadata.equals(that.userMetadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(contentLength, contentType, eTag, lastModified, userMetadata);
    }

    @Override
    public String toString() {
        return "FlinkObjectMetadata{"
                + "contentLength="
                + contentLength
                + ", contentType='"
                + contentType
                + '\''
                + ", eTag='"
                + eTag
                + '\''
                + ", lastModified="
                + lastModified
                + ", userMetadata="
                + userMetadata
                + '}';
    }

    /** Creates a new builder. */
    public static Builder builder() {
        return new Builder();
    }

    /** Builder for {@link FlinkObjectMetadata}. */
    public static class Builder {
        private long contentLength;
        private String contentType;
        private String eTag;
        private Date lastModified;
        private Map<String, String> userMetadata;

        private Builder() {}

        public Builder contentLength(long contentLength) {
            this.contentLength = contentLength;
            return this;
        }

        public Builder contentType(String contentType) {
            this.contentType = contentType;
            return this;
        }

        public Builder eTag(String eTag) {
            this.eTag = eTag;
            return this;
        }

        public Builder lastModified(Date lastModified) {
            this.lastModified = lastModified;
            return this;
        }

        public Builder userMetadata(Map<String, String> userMetadata) {
            this.userMetadata = userMetadata;
            return this;
        }

        public FlinkObjectMetadata build() {
            return new FlinkObjectMetadata(this);
        }
    }
}
