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

import java.util.Objects;

/**
 * SDK-agnostic representation of the result of uploading a part in a multipart upload.
 *
 * <p>This class maintains compatibility with both AWS SDK v1 {@code UploadPartResult} and AWS SDK
 * v2 {@code UploadPartResponse}, allowing the base S3 filesystem implementation to work with either
 * SDK version.
 */
@Internal
public final class FlinkUploadPartResult {

    private final int partNumber;
    private final String eTag;

    /**
     * Creates a new FlinkUploadPartResult.
     *
     * @param partNumber the part number of the uploaded part
     * @param eTag the ETag of the uploaded part
     */
    public FlinkUploadPartResult(int partNumber, String eTag) {
        this.partNumber = partNumber;
        this.eTag = Objects.requireNonNull(eTag, "eTag must not be null");
    }

    /**
     * Returns the part number of the uploaded part.
     *
     * @return the part number
     */
    public int getPartNumber() {
        return partNumber;
    }

    /**
     * Returns the ETag of the uploaded part.
     *
     * @return the ETag
     */
    public String getETag() {
        return eTag;
    }

    /**
     * Converts this result to a {@link FlinkPartETag} for use in multipart upload completion.
     *
     * @return a FlinkPartETag with the same part number and ETag
     */
    public FlinkPartETag toPartETag() {
        return new FlinkPartETag(partNumber, eTag);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FlinkUploadPartResult that = (FlinkUploadPartResult) o;
        return partNumber == that.partNumber && eTag.equals(that.eTag);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partNumber, eTag);
    }

    @Override
    public String toString() {
        return "FlinkUploadPartResult{partNumber=" + partNumber + ", eTag='" + eTag + "'}";
    }
}
