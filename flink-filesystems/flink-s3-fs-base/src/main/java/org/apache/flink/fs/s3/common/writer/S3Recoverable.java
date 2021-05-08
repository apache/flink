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

package org.apache.flink.fs.s3.common.writer;

import org.apache.flink.core.fs.RecoverableWriter;

import com.amazonaws.services.s3.model.PartETag;

import javax.annotation.Nullable;

import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Data object to recover an S3 MultiPartUpload for a recoverable output stream. */
public final class S3Recoverable implements RecoverableWriter.ResumeRecoverable {

    private final String uploadId;

    private final String objectName;

    private final List<PartETag> parts;

    @Nullable private final String lastPartObject;

    private long numBytesInParts;

    private long lastPartObjectLength;

    S3Recoverable(String objectName, String uploadId, List<PartETag> parts, long numBytesInParts) {
        this(objectName, uploadId, parts, numBytesInParts, null, -1L);
    }

    S3Recoverable(
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
        this.parts = checkNotNull(parts);
        this.numBytesInParts = numBytesInParts;

        this.lastPartObject = lastPartObject;
        this.lastPartObjectLength = lastPartObjectLength;
    }

    // ------------------------------------------------------------------------

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

    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder(128);
        buf.append("S3Recoverable: ");
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
        buf.append("trailingPartLen=").append(lastPartObjectLength);

        return buf.toString();
    }
}
