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

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Structured representation of an in-progress multipart upload for deterministic recovery.
 *
 * <p>This context enables Flink to reconstruct the exact state of an upload after failure,
 * supporting checkpoint-during-recovery and concurrent checkpointing scenarios that were impossible
 * with the previous unstructured approach.
 *
 * <p>Key capabilities:
 *
 * <ul>
 *   <li>Deterministic recovery from partial uploads
 *   <li>Support for checkpoint-during-recovery
 *   <li>Checkpoint directory isolation
 *   <li>Idempotent completion and abort operations
 * </ul>
 *
 * <p>This addresses FLIP-547's requirements for transactional semantics in Flink's recovery model.
 * @see org.apache.flink.core.fs.FileSystem Thread Safety requirements
 */
@ThreadSafe
@Internal
public class S3UploadContext implements Serializable {

    private static final long serialVersionUID = 1L;

    /** The bucket for this upload. */
    private final String bucket;

    /** The final object key (target path). */
    private final String targetKey;

    /** The temporary/staging key where data is uploaded. */
    private final String stagingKey;

    /** The S3 multipart upload ID. */
    private final String uploadId;

    /** List of completed part ETags, in order. */
    @GuardedBy("this")
    private final List<PartETag> completedParts;

    /** Checkpoint ID this upload belongs to. */
    private final long checkpointId;

    /** Timestamp when upload was initiated. */
    private final long uploadStartTime;

    /** Total bytes uploaded so far. */
    @GuardedBy("this")
    private volatile long bytesUploaded;

    /** Number of parts uploaded. */
    @GuardedBy("this")
    private volatile int partCount;

    /** Whether this upload has been committed. */
    @GuardedBy("this")
    private volatile boolean committed;

    /** Whether this upload has been aborted. */
    @GuardedBy("this")
    private volatile boolean aborted;

    public S3UploadContext(
            String bucket,
            String targetKey,
            String stagingKey,
            String uploadId,
            long checkpointId) {
        this.bucket = checkNotNull(bucket, "bucket must not be null");
        this.targetKey = checkNotNull(targetKey, "targetKey must not be null");
        this.stagingKey = checkNotNull(stagingKey, "stagingKey must not be null");
        this.uploadId = checkNotNull(uploadId, "uploadId must not be null");
        checkArgument(checkpointId >= 0, "checkpointId must be non-negative");
        this.checkpointId = checkpointId;
        this.completedParts = new ArrayList<>();
        this.uploadStartTime = System.currentTimeMillis();
        this.bytesUploaded = 0;
        this.partCount = 0;
        this.committed = false;
        this.aborted = false;
    }

    /**
     * Adds a completed part to this upload context.
     *
     * @param partNumber the part number (1-based)
     * @param eTag the ETag returned by S3
     * @param size the size of this part in bytes
     * @throws IllegalStateException if upload is already committed or aborted
     */
    public synchronized void addCompletedPart(int partNumber, String eTag, long size) {
        checkState(!committed, "Cannot add part to committed upload");
        checkState(!aborted, "Cannot add part to aborted upload");
        checkArgument(partNumber > 0, "partNumber must be positive");
        checkNotNull(eTag, "eTag must not be null");
        checkArgument(size >= 0, "size must be non-negative");

        completedParts.add(new PartETag(partNumber, eTag, size));
        bytesUploaded += size;
        partCount++;
    }

    /**
     * Marks this upload as committed.
     *
     * <p>Idempotent: multiple calls are safe.
     */
    public synchronized void markCommitted() {
        this.committed = true;
    }

    /**
     * Marks this upload as aborted.
     *
     * <p>Idempotent: multiple calls are safe.
     */
    public synchronized void markAborted() {
        this.aborted = true;
    }

    /**
     * Gets the staging path for checkpoint isolation.
     *
     * <p>Format: {bucket}/.flink-staging/{checkpoint-id}/{target-key}
     */
    public String getStagingPath() {
        return String.format("s3://%s/.flink-staging/%d/%s", bucket, checkpointId, targetKey);
    }

    /**
     * Gets the commit marker path for this upload.
     *
     * <p>Format: {staging-key}.commit
     */
    public String getCommitMarkerPath() {
        return stagingKey + ".commit";
    }

    /**
     * Checks if this upload can be safely recovered.
     *
     * @return true if upload is in a recoverable state
     */
    public synchronized boolean isRecoverable() {
        return !aborted && uploadId != null && !uploadId.isEmpty() && partCount > 0;
    }

    /**
     * Checks if this upload is complete and ready for commit.
     *
     * @return true if upload can be committed
     */
    public synchronized boolean isReadyForCommit() {
        return !committed && !aborted && partCount > 0;
    }

    // Getters

    public String getBucket() {
        return bucket;
    }

    public String getTargetKey() {
        return targetKey;
    }

    public String getStagingKey() {
        return stagingKey;
    }

    public String getUploadId() {
        return uploadId;
    }

    public synchronized List<PartETag> getCompletedParts() {
        return new ArrayList<>(completedParts);
    }

    public long getCheckpointId() {
        return checkpointId;
    }

    public long getUploadStartTime() {
        return uploadStartTime;
    }

    public synchronized long getBytesUploaded() {
        return bytesUploaded;
    }

    public synchronized int getPartCount() {
        return partCount;
    }

    public synchronized boolean isCommitted() {
        return committed;
    }

    public synchronized boolean isAborted() {
        return aborted;
    }

    @Override
    public String toString() {
        return "S3UploadContext{"
                + "bucket='"
                + bucket
                + '\''
                + ", targetKey='"
                + targetKey
                + '\''
                + ", uploadId='"
                + uploadId
                + '\''
                + ", checkpointId="
                + checkpointId
                + ", parts="
                + partCount
                + ", bytes="
                + bytesUploaded
                + ", committed="
                + committed
                + ", aborted="
                + aborted
                + '}';
    }

    public static class PartETag implements Serializable {
        private static final long serialVersionUID = 1L;

        private final int partNumber;
        private final String eTag;
        private final long size;

        public PartETag(int partNumber, String eTag, long size) {
            this.partNumber = partNumber;
            this.eTag = Objects.requireNonNull(eTag, "eTag");
            this.size = size;
        }

        public int getPartNumber() {
            return partNumber;
        }

        public String getETag() {
            return eTag;
        }

        public long getSize() {
            return size;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PartETag partETag = (PartETag) o;
            return partNumber == partETag.partNumber
                    && size == partETag.size
                    && Objects.equals(eTag, partETag.eTag);
        }

        @Override
        public int hashCode() {
            return Objects.hash(partNumber, eTag, size);
        }

        @Override
        public String toString() {
            return "PartETag{"
                    + "partNumber="
                    + partNumber
                    + ", eTag='"
                    + eTag
                    + '\''
                    + ", size="
                    + size
                    + '}';
        }
    }
}
