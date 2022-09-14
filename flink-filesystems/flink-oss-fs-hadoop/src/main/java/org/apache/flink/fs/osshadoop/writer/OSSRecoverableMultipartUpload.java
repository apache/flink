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

package org.apache.flink.fs.osshadoop.writer;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.fs.RefCountedFSOutputStream;
import org.apache.flink.fs.osshadoop.OSSAccessor;

import com.aliyun.oss.model.PartETag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** Uploader for OSS multi part upload. */
public class OSSRecoverableMultipartUpload {
    private static final Logger LOG = LoggerFactory.getLogger(OSSRecoverableMultipartUpload.class);

    private String objectName;
    private String uploadId;
    private List<PartETag> completeParts;
    private Optional<File> incompletePart;
    private Executor uploadThreadPool;
    private OSSAccessor ossAccessor;
    private Deque<CompletableFuture<PartETag>> uploadsInProgress;
    private int numberOfRegisteredParts;
    private long expectedSizeInBytes;
    private final String namePrefixForTempObjects;

    public OSSRecoverableMultipartUpload(
            String objectName,
            Executor uploadThreadPool,
            OSSAccessor ossAccessor,
            Optional<File> incompletePart,
            String uploadId,
            List<PartETag> completeParts,
            long expectedSizeInBytes) {
        this.objectName = objectName;
        if (completeParts != null) {
            this.completeParts = completeParts;
        } else {
            this.completeParts = new ArrayList<>();
        }

        this.incompletePart = incompletePart;
        if (uploadId != null) {
            this.uploadId = uploadId;
        } else {
            this.uploadId = ossAccessor.startMultipartUpload(objectName);
        }
        this.uploadThreadPool = uploadThreadPool;
        this.ossAccessor = ossAccessor;
        this.uploadsInProgress = new ArrayDeque<>();
        this.numberOfRegisteredParts = this.completeParts.size();
        this.expectedSizeInBytes = expectedSizeInBytes;
        this.namePrefixForTempObjects = createIncompletePartObjectNamePrefix(objectName);
    }

    public Optional<File> getIncompletePart() {
        return incompletePart;
    }

    public void uploadPart(RefCountedFSOutputStream file) throws IOException {
        checkState(file.isClosed());

        final CompletableFuture<PartETag> future = new CompletableFuture<>();
        uploadsInProgress.add(future);

        numberOfRegisteredParts += 1;
        expectedSizeInBytes += file.getPos();

        file.retain();
        uploadThreadPool.execute(
                new UploadTask(
                        ossAccessor, objectName, uploadId, numberOfRegisteredParts, file, future));
    }

    public OSSRecoverable getRecoverable(RefCountedFSOutputStream file) throws IOException {
        String incompletePartObjectName = uploadSmallPart(file);

        checkState(numberOfRegisteredParts - completeParts.size() == uploadsInProgress.size());

        while (numberOfRegisteredParts - completeParts.size() > 0) {
            CompletableFuture<PartETag> next = uploadsInProgress.peekFirst();
            PartETag nextPart;
            try {
                nextPart = next.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while waiting for part uploads to complete");
            } catch (ExecutionException e) {
                throw new IOException("Uploading parts failed ", e.getCause());
            }

            completeParts.add(nextPart);
            uploadsInProgress.removeFirst();
        }

        if (file == null) {
            return new OSSRecoverable(
                    uploadId, objectName, completeParts, null, expectedSizeInBytes, 0);
        } else {
            return new OSSRecoverable(
                    uploadId,
                    objectName,
                    completeParts,
                    incompletePartObjectName,
                    expectedSizeInBytes,
                    file.getPos());
        }
    }

    private String uploadSmallPart(@Nullable RefCountedFSOutputStream file) throws IOException {
        if (file == null || file.getPos() == 0L) {
            return null;
        }

        String incompletePartObjectName = createIncompletePartObjectName();
        file.retain();

        try {
            ossAccessor.putObject(incompletePartObjectName, file.getInputFile());
        } finally {
            file.release();
        }
        return incompletePartObjectName;
    }

    private String createIncompletePartObjectName() {
        return namePrefixForTempObjects + UUID.randomUUID().toString();
    }

    @VisibleForTesting
    static String createIncompletePartObjectNamePrefix(String objectName) {
        checkNotNull(objectName);

        final int lastSlash = objectName.lastIndexOf('/');
        final String parent;
        final String child;

        if (lastSlash == -1) {
            parent = "";
            child = objectName;
        } else {
            parent = objectName.substring(0, lastSlash + 1);
            child = objectName.substring(lastSlash + 1);
        }
        return parent + (child.isEmpty() ? "" : '_') + child + "_tmp_";
    }

    public OSSCommitter getCommitter() throws IOException {
        final OSSRecoverable recoverable = getRecoverable(null);
        return new OSSCommitter(
                ossAccessor,
                recoverable.getObjectName(),
                recoverable.getUploadId(),
                recoverable.getPartETags(),
                recoverable.getNumBytesInParts());
    }

    private static class UploadTask implements Runnable {
        private final OSSAccessor ossAccessor;

        private final String objectName;

        private final String uploadId;

        private final int partNumber;

        private final RefCountedFSOutputStream file;

        private final CompletableFuture<PartETag> future;

        UploadTask(
                OSSAccessor ossAccessor,
                String objectName,
                String uploadId,
                int partNumber,
                RefCountedFSOutputStream file,
                CompletableFuture<PartETag> future) {
            this.ossAccessor = ossAccessor;
            this.objectName = objectName;
            this.uploadId = uploadId;

            checkArgument(partNumber >= 1 && partNumber <= 10_000);
            this.partNumber = partNumber;

            this.file = file;
            this.future = future;
        }

        @Override
        public void run() {
            try {
                PartETag partETag =
                        ossAccessor.uploadPart(
                                file.getInputFile(), objectName, uploadId, partNumber);
                future.complete(partETag);
                file.release();
            } catch (Throwable t) {
                future.completeExceptionally(t);
            }
        }
    }
}
