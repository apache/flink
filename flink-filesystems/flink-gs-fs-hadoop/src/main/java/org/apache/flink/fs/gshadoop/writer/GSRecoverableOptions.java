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

package org.apache.flink.fs.gshadoop.writer;

import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava18.com.google.common.base.Strings;

import com.google.cloud.storage.BlobId;
import org.apache.hadoop.fs.Path;

import javax.annotation.Nullable;

import java.util.UUID;

/**
 * Data structure holding options for the Google Storage recoverable writer. These options are
 * determined by the factory and are common to all instances of the recoverable writer.
 */
public class GSRecoverableOptions {

    /** The recoverable writer helper to use. */
    public final GSRecoverableWriterHelper helper;

    /** The content type for uploaded blobs. */
    public final String uploadContentType;

    /**
     * The bucket to use for temporary blobs; if null, temporary blobs are put into the same bucket
     * as the "final" blobs being uploaded.
     */
    @Nullable public final String uploadTempBucket;

    /** The prefix to use in constructing temporary blob names. */
    public final String uploadTempPrefix;

    /** The minimum chunk size to use when uploading files; this controls flushing. */
    public final int uploadChunkSize;

    /**
     * Creates a GSRecoverableOptions object.
     *
     * @param helper The helper, which wraps the interactions with Google Storage or a mock
     * @param uploadContentType The upload content type
     * @param uploadTempBucket The bucket to use for temporary upload files. If null, the bucket of
     *     the target file is used.
     * @param uploadTempPrefix The prefix to use for uploaded files, which must not have leading or
     *     trailing slashes.
     * @param uploadChunkSize The chunk size to use for uploads
     */
    public GSRecoverableOptions(
            GSRecoverableWriterHelper helper,
            String uploadContentType,
            @Nullable String uploadTempBucket,
            String uploadTempPrefix,
            int uploadChunkSize) {

        this.helper = Preconditions.checkNotNull(helper);
        this.uploadContentType = Preconditions.checkNotNull(Strings.emptyToNull(uploadContentType));
        this.uploadTempBucket = Strings.emptyToNull(uploadTempBucket);
        this.uploadTempPrefix = Preconditions.checkNotNull(Strings.emptyToNull(uploadTempPrefix));
        Preconditions.checkArgument(
                !uploadTempPrefix.startsWith(Path.SEPARATOR),
                "Upload temp prefix must not start with a separator: %s",
                uploadTempPrefix);
        Preconditions.checkArgument(
                !uploadTempPrefix.endsWith(Path.SEPARATOR),
                "Upload temp prefix must not end with a separator: %s",
                uploadTempPrefix);

        this.uploadChunkSize = uploadChunkSize;
        Preconditions.checkArgument(
                uploadChunkSize >= 0,
                "upload chunk size must be non-negative: %s",
                uploadChunkSize);
    }

    /**
     * Helper method to create a {@link org.apache.flink.fs.gshadoop.writer.GSRecoverablePlan} for a
     * given final bucket and object name, taking into account the options.
     *
     * @param finalBucketName The name of the bucket for the final uploaded blob
     * @param finalObjectName The name of the object for the final uploaded blob
     * @return The plan
     */
    GSRecoverablePlan createPlan(String finalBucketName, String finalObjectName) {
        Preconditions.checkNotNull(Strings.emptyToNull(finalBucketName));
        Preconditions.checkNotNull(Strings.emptyToNull(finalObjectName));

        String tempBucketName = uploadTempBucket;
        if (Strings.isNullOrEmpty(tempBucketName)) {
            tempBucketName = finalBucketName;
        }

        UUID tempUniqueId = UUID.randomUUID();
        String tempObjectName =
                String.join(
                        org.apache.flink.core.fs.Path.SEPARATOR,
                        uploadTempPrefix,
                        finalObjectName,
                        tempUniqueId.toString());

        BlobId tempBlobId = BlobId.of(tempBucketName, tempObjectName);
        BlobId finalBlobId = BlobId.of(finalBucketName, finalObjectName);
        return new GSRecoverablePlan(tempBlobId, finalBlobId);
    }
}
