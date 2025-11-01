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

package org.apache.flink.fs.s3native.writer;

import org.apache.flink.core.fs.RecoverableWriter;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Captures the state of an S3 multipart upload for recovery purposes.
 *
 * <p><b>Valid States:</b>
 *
 * <ul>
 *   <li><b>No trailing part:</b> {@code lastPartObject == null} AND {@code lastPartObjectLength} is
 *       ignored (set to -1). This represents a clean state where all data has been uploaded as
 *       complete parts.
 *   <li><b>With trailing part:</b> {@code lastPartObject != null} AND {@code lastPartObjectLength >
 *       0}. The trailing part is a temporary S3 object holding data that hasn't yet been uploaded
 *       as a multipart part (smaller than minimum part size or pending flush).
 * </ul>
 *
 * <p><b>Invalid States (rejected by constructor):</b>
 *
 * <ul>
 *   <li>{@code lastPartObject != null} with {@code lastPartObjectLength <= 0} - having a trailing
 *       object with no content makes no sense
 *   <li>{@code numBytesInParts < 0} - negative byte count is invalid
 * </ul>
 *
 * <p>The integrity of {@code lastPartObject} and {@code lastPartObjectLength} is maintained by
 * always updating them together through the recoverable's construction. The {@code lastPartObject}
 * is final, while {@code lastPartObjectLength} is not (for serialization compatibility), but in
 * practice both are set once during construction and not modified afterwards.
 */
public final class NativeS3Recoverable
        implements RecoverableWriter.ResumeRecoverable, RecoverableWriter.CommitRecoverable {

    private final String uploadId;
    private final String objectName;
    private final List<PartETag> parts;

    /**
     * The S3 key for a temporary object holding data smaller than the minimum part size. Will be
     * null if there's no pending data.
     */
    @Nullable private final String lastPartObject;

    private long numBytesInParts;

    /** The length of data in {@link #lastPartObject}. Must be > 0 if lastPartObject is not null. */
    private long lastPartObjectLength;

    public NativeS3Recoverable(
            String objectName, String uploadId, List<PartETag> parts, long numBytesInParts) {
        this(objectName, uploadId, parts, numBytesInParts, null, -1L);
    }

    public NativeS3Recoverable(
            String objectName,
            String uploadId,
            List<PartETag> parts,
            long numBytesInParts,
            @Nullable String lastPartObject,
            long lastPartObjectLength) {
        checkArgument(numBytesInParts >= 0L);
        checkArgument(lastPartObject == null || lastPartObjectLength > 0L);

        this.objectName = checkNotNull(objectName);
        this.uploadId = checkNotNull(uploadId);
        this.parts = new ArrayList<>(checkNotNull(parts));
        this.numBytesInParts = numBytesInParts;
        this.lastPartObject = lastPartObject;
        this.lastPartObjectLength = lastPartObjectLength;
    }

    public String uploadId() {
        return uploadId;
    }

    public String getObjectName() {
        return objectName;
    }

    public List<PartETag> parts() {
        return parts;
    }

    public long numBytesInParts() {
        return numBytesInParts;
    }

    @Nullable
    public String incompleteObjectName() {
        return lastPartObject;
    }

    public long incompleteObjectLength() {
        return lastPartObjectLength;
    }

    public static class PartETag {
        private final int partNumber;
        private final String eTag;

        public PartETag(int partNumber, String eTag) {
            this.partNumber = partNumber;
            this.eTag = eTag;
        }

        public int getPartNumber() {
            return partNumber;
        }

        public String getETag() {
            return eTag;
        }
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder(256);
        buf.append("NativeS3Recoverable: ");
        buf.append("key=").append(objectName);
        buf.append(", uploadId=").append(uploadId);
        buf.append(", bytesInParts=").append(numBytesInParts);
        buf.append(", parts=[");
        int num = 0;
        for (PartETag part : parts) {
            if (0 != num++) {
                buf.append(", ");
            }
            buf.append(part.getPartNumber()).append('=').append(part.getETag());
        }
        buf.append("], trailingPart=").append(lastPartObject);
        buf.append(", trailingPartLen=").append(lastPartObjectLength);
        return buf.toString();
    }
}
