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
 * SDK-agnostic representation of the result of putting an object to S3.
 *
 * <p>This class maintains compatibility with both AWS SDK v1 {@code PutObjectResult} and AWS SDK v2
 * {@code PutObjectResponse}, allowing the base S3 filesystem implementation to work with either SDK
 * version.
 */
@Internal
public final class FlinkPutObjectResult {

    @Nullable private final String eTag;
    @Nullable private final String versionId;

    /**
     * Creates a new FlinkPutObjectResult.
     *
     * @param eTag the ETag of the uploaded object
     * @param versionId the version ID of the object (if versioning is enabled)
     */
    public FlinkPutObjectResult(@Nullable String eTag, @Nullable String versionId) {
        this.eTag = eTag;
        this.versionId = versionId;
    }

    /**
     * Returns the ETag of the uploaded object.
     *
     * @return the ETag
     */
    @Nullable
    public String getETag() {
        return eTag;
    }

    /**
     * Returns the version ID of the object (if versioning is enabled).
     *
     * @return the version ID, or null if versioning is not enabled
     */
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
        FlinkPutObjectResult that = (FlinkPutObjectResult) o;
        return Objects.equals(eTag, that.eTag) && Objects.equals(versionId, that.versionId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eTag, versionId);
    }

    @Override
    public String toString() {
        return "FlinkPutObjectResult{eTag='" + eTag + "', versionId='" + versionId + "'}";
    }
}
