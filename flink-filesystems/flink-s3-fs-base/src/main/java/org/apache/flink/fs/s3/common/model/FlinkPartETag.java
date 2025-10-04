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

import java.io.Serializable;
import java.util.Objects;

/**
 * SDK-agnostic representation of a part uploaded in a multipart upload.
 *
 * <p>This class maintains compatibility with both AWS SDK v1 {@code PartETag} and AWS SDK v2 {@code
 * CompletedPart}, allowing the base S3 filesystem implementation to work with either SDK version.
 *
 * <p>This class is serializable to support checkpoint recovery. The serialization format is
 * independent of any AWS SDK version, ensuring backward compatibility across SDK upgrades.
 */
@Internal
public final class FlinkPartETag implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int partNumber;
    private final String eTag;

    /**
     * Creates a new FlinkPartETag.
     *
     * @param partNumber the part number (must be between 1 and 10000)
     * @param eTag the ETag of the uploaded part
     */
    public FlinkPartETag(int partNumber, String eTag) {
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FlinkPartETag that = (FlinkPartETag) o;
        return partNumber == that.partNumber && eTag.equals(that.eTag);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partNumber, eTag);
    }

    @Override
    public String toString() {
        return "FlinkPartETag{partNumber=" + partNumber + ", eTag='" + eTag + "'}";
    }
}
