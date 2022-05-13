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

import org.apache.flink.annotation.Internal;

import com.amazonaws.services.s3.model.PartETag;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** A data structure containing information concerning the in-flight MPU. */
@Internal
final class MultiPartUploadInfo {

    private final String objectName;

    private final String uploadId;

    private final List<PartETag> completeParts;

    private final Optional<File> incompletePart;

    /**
     * This contains both the parts that are already uploaded but also the ones that are queued to
     * be uploaded at the {@link RecoverableMultiPartUpload}.
     */
    private int numberOfRegisteredParts;

    /**
     * This is the total size of the upload, i.e. also with the parts that are queued but not
     * uploaded yet.
     */
    private long expectedSizeInBytes;

    MultiPartUploadInfo(
            final String objectName,
            final String uploadId,
            final List<PartETag> completeParts,
            final long numBytes,
            final Optional<File> incompletePart) {

        checkArgument(numBytes >= 0L);

        this.objectName = checkNotNull(objectName);
        this.uploadId = checkNotNull(uploadId);
        this.completeParts = checkNotNull(completeParts);
        this.incompletePart = checkNotNull(incompletePart);

        this.numberOfRegisteredParts = completeParts.size();
        this.expectedSizeInBytes = numBytes;
    }

    String getObjectName() {
        return objectName;
    }

    String getUploadId() {
        return uploadId;
    }

    int getNumberOfRegisteredParts() {
        return numberOfRegisteredParts;
    }

    long getExpectedSizeInBytes() {
        return expectedSizeInBytes;
    }

    Optional<File> getIncompletePart() {
        return incompletePart;
    }

    List<PartETag> getCopyOfEtagsOfCompleteParts() {
        return new ArrayList<>(completeParts);
    }

    void registerNewPart(long length) {
        this.expectedSizeInBytes += length;
        this.numberOfRegisteredParts++;
    }

    void registerCompletePart(PartETag eTag) {
        completeParts.add(eTag);
    }

    int getRemainingParts() {
        return numberOfRegisteredParts - completeParts.size();
    }
}
