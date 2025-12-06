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

package org.apache.flink.fs.s3native.commit;

import org.apache.flink.annotation.Internal;

import javax.annotation.concurrent.Immutable;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Commit marker for atomic visibility of multipart uploads on S3.
 
 *
 * <p>Key principles:
 *
 * <ul>
 *   <li>MPU completion is a non-visible operation until marker is written
 *   <li>Commit marker is the single source of truth for upload completion
 *   <li>Never rely on S3 listings to determine completeness
 *   <li>Enables deterministic recovery from partial uploads
 * </ul>

 */
@Immutable
@Internal
public class S3CommitMarker implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final int VERSION = 1;
    private final String bucket;
    private final String key;
    private final String uploadId;
    private final List<String> eTags;
    private final String checksum;
    private final long uploadStartTime;
    private final long uploadCompleteTime;
    private final long objectSize;
    private final long checkpointId;

    public S3CommitMarker(
            String bucket,
            String key,
            String uploadId,
            List<String> eTags,
            String checksum,
            long uploadStartTime,
            long uploadCompleteTime,
            long objectSize,
            long checkpointId) {
        this.bucket = checkNotNull(bucket, "bucket must not be null");
        this.key = checkNotNull(key, "key must not be null");
        this.uploadId = checkNotNull(uploadId, "uploadId must not be null");
        this.eTags = Collections.unmodifiableList(checkNotNull(eTags, "eTags must not be null"));
        checkArgument(!eTags.isEmpty(), "eTags list must not be empty");
        checkArgument(
                uploadCompleteTime >= uploadStartTime,
                "uploadCompleteTime must be >= uploadStartTime");
        checkArgument(objectSize >= 0, "objectSize must be non-negative");
        checkArgument(checkpointId >= 0, "checkpointId must be non-negative");

        this.checksum = checksum;
        this.uploadStartTime = uploadStartTime;
        this.uploadCompleteTime = uploadCompleteTime;
        this.objectSize = objectSize;
        this.checkpointId = checkpointId;
    }

    public String getBucket() {
        return bucket;
    }

    public String getKey() {
        return key;
    }

    public String getUploadId() {
        return uploadId;
    }

    public List<String> getETags() {
        return eTags;
    }

    public String getChecksum() {
        return checksum;
    }

    public long getUploadStartTime() {
        return uploadStartTime;
    }

    public long getUploadCompleteTime() {
        return uploadCompleteTime;
    }

    public long getObjectSize() {
        return objectSize;
    }

    public long getCheckpointId() {
        return checkpointId;
    }

    public int getVersion() {
        return VERSION;
    }

    /**
     * Gets the S3 path for this commit marker file.
     *
     * <p>Convention: {original-key}.commit
     */
    public String getMarkerPath() {
        return key + ".commit";
    }

    /**
     * Validates that this commit marker is complete and consistent.
     *
     * @return true if the marker is valid
     */
    public boolean isValid() {
        return bucket != null
                && !bucket.isEmpty()
                && key != null
                && !key.isEmpty()
                && uploadId != null
                && !uploadId.isEmpty()
                && eTags != null
                && !eTags.isEmpty()
                && uploadCompleteTime >= uploadStartTime
                && objectSize >= 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        S3CommitMarker that = (S3CommitMarker) o;
        return uploadStartTime == that.uploadStartTime
                && uploadCompleteTime == that.uploadCompleteTime
                && objectSize == that.objectSize
                && checkpointId == that.checkpointId
                && Objects.equals(bucket, that.bucket)
                && Objects.equals(key, that.key)
                && Objects.equals(uploadId, that.uploadId)
                && Objects.equals(eTags, that.eTags)
                && Objects.equals(checksum, that.checksum);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                bucket,
                key,
                uploadId,
                eTags,
                checksum,
                uploadStartTime,
                uploadCompleteTime,
                objectSize,
                checkpointId);
    }

    @Override
    public String toString() {
        return "S3CommitMarker{"
                + "bucket='"
                + bucket
                + '\''
                + ", key='"
                + key
                + '\''
                + ", uploadId='"
                + uploadId
                + '\''
                + ", parts="
                + eTags.size()
                + ", checkpointId="
                + checkpointId
                + ", size="
                + objectSize
                + '}';
    }
}
