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

import org.apache.flink.annotation.Experimental;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Recoverable writer for S3 using multipart uploads for exactly-once semantics. */
@Experimental
public class NativeS3RecoverableWriter implements RecoverableWriter, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(NativeS3RecoverableWriter.class);

    private final NativeS3ObjectOperations s3AccessHelper;
    private final String localTmpDir;
    private final long userDefinedMinPartSize;
    private final int maxConcurrentUploadsPerStream;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private NativeS3RecoverableWriter(
            NativeS3ObjectOperations s3AccessHelper,
            String localTmpDir,
            long userDefinedMinPartSize,
            int maxConcurrentUploadsPerStream) {
        this.s3AccessHelper = checkNotNull(s3AccessHelper);
        this.localTmpDir = checkNotNull(localTmpDir);
        this.userDefinedMinPartSize = userDefinedMinPartSize;
        this.maxConcurrentUploadsPerStream = maxConcurrentUploadsPerStream;

        LOG.debug(
                "Created S3 recoverable writer - minPartSize: {} bytes, tmpDir: {}",
                userDefinedMinPartSize,
                localTmpDir);
    }

    @Override
    public RecoverableFsDataOutputStream open(Path path) throws IOException {
        checkNotClosed();
        String key = NativeS3ObjectOperations.extractKey(path);
        LOG.debug("Opening recoverable stream for key: {}", key);

        String uploadId = s3AccessHelper.startMultiPartUpload(key);
        LOG.debug("Started multipart upload - key: {}, uploadId: {}", key, uploadId);

        return new NativeS3RecoverableFsDataOutputStream(
                s3AccessHelper, key, uploadId, localTmpDir, userDefinedMinPartSize);
    }

    @Override
    public RecoverableFsDataOutputStream.Committer recoverForCommit(CommitRecoverable recoverable)
            throws IOException {
        checkNotClosed();
        NativeS3Recoverable s3recoverable = castToNativeS3Recoverable(recoverable);
        return new NativeS3Committer(s3AccessHelper, s3recoverable);
    }

    @Override
    public RecoverableFsDataOutputStream recover(ResumeRecoverable recoverable) throws IOException {
        checkNotClosed();
        NativeS3Recoverable s3recoverable = castToNativeS3Recoverable(recoverable);

        File seedTail = null;
        long seedTailLength = 0L;
        if (s3recoverable.incompleteObjectName() != null) {
            seedTail = downloadIncompleteTail(s3recoverable);
            seedTailLength = s3recoverable.incompleteObjectLength();
        }

        try {
            LOG.debug(
                    "Resuming stream - key: {}, uploadId: {}, parts: {}, bytesInParts: {}, seedTail: {} ({} bytes)",
                    s3recoverable.getObjectName(),
                    s3recoverable.uploadId(),
                    s3recoverable.parts().size(),
                    s3recoverable.numBytesInParts(),
                    s3recoverable.incompleteObjectName(),
                    seedTailLength);
            return new NativeS3RecoverableFsDataOutputStream(
                    s3AccessHelper,
                    s3recoverable.getObjectName(),
                    s3recoverable.uploadId(),
                    localTmpDir,
                    userDefinedMinPartSize,
                    s3recoverable.parts(),
                    s3recoverable.numBytesInParts(),
                    seedTail,
                    seedTailLength);
        } catch (Throwable t) {
            // The downloaded seed file is owned by recover() until the constructor takes
            // ownership. If construction fails, drop the local file so we don't leak it.
            if (seedTail != null) {
                try {
                    Files.deleteIfExists(seedTail.toPath());
                } catch (IOException cleanup) {
                    t.addSuppressed(cleanup);
                }
            }
            throw t;
        }
    }

    /**
     * Downloads the side object holding the previously-persisted sub-part-size tail into a fresh
     * file under {@link #localTmpDir}. The side object itself is left in place so that a repeated
     * recovery from the same checkpoint remains correct; cleanup is the responsibility of {@link
     * #cleanupRecoverableState(ResumeRecoverable)} which Flink invokes when the checkpoint is
     * retired.
     */
    private File downloadIncompleteTail(NativeS3Recoverable s3recoverable) throws IOException {
        File tmpDir = new File(localTmpDir);
        if (!tmpDir.exists() && !tmpDir.mkdirs()) {
            throw new IOException("Cannot create local tmp dir: " + localTmpDir);
        }
        File target = new File(tmpDir, "s3-resume-" + UUID.randomUUID());
        try {
            long downloaded =
                    s3AccessHelper.getObject(s3recoverable.incompleteObjectName(), target);
            if (downloaded != s3recoverable.incompleteObjectLength()) {
                throw new IOException(
                        "Incomplete-tail object "
                                + s3recoverable.incompleteObjectName()
                                + " has unexpected length: expected "
                                + s3recoverable.incompleteObjectLength()
                                + " got "
                                + downloaded);
            }
            return target;
        } catch (IOException e) {
            try {
                Files.deleteIfExists(target.toPath());
            } catch (IOException cleanup) {
                LOG.warn(
                        "Failed to delete partial download {} after error: {}",
                        target,
                        cleanup.getMessage());
                e.addSuppressed(cleanup);
            }
            throw e;
        }
    }

    @Override
    public boolean requiresCleanupOfRecoverableState() {
        return true;
    }

    @Override
    public boolean cleanupRecoverableState(ResumeRecoverable resumable) throws IOException {
        checkNotClosed();
        NativeS3Recoverable s3recoverable = castToNativeS3Recoverable(resumable);
        String smallPartObjectToDelete = s3recoverable.incompleteObjectName();
        return smallPartObjectToDelete != null
                && s3AccessHelper.deleteObject(smallPartObjectToDelete);
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public SimpleVersionedSerializer<CommitRecoverable> getCommitRecoverableSerializer() {
        return (SimpleVersionedSerializer) NativeS3RecoverableSerializer.INSTANCE;
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public SimpleVersionedSerializer<ResumeRecoverable> getResumeRecoverableSerializer() {
        return (SimpleVersionedSerializer) NativeS3RecoverableSerializer.INSTANCE;
    }

    @Override
    public boolean supportsResume() {
        return true;
    }

    private static NativeS3Recoverable castToNativeS3Recoverable(CommitRecoverable recoverable) {
        if (recoverable instanceof NativeS3Recoverable) {
            return (NativeS3Recoverable) recoverable;
        }
        throw new IllegalArgumentException(
                "Native S3 File System cannot recover recoverable for other file system: "
                        + recoverable);
    }

    private static NativeS3Recoverable castToNativeS3Recoverable(ResumeRecoverable recoverable) {
        if (recoverable instanceof NativeS3Recoverable) {
            return (NativeS3Recoverable) recoverable;
        }
        throw new IllegalArgumentException(
                "Native S3 File System cannot recover recoverable for other file system: "
                        + recoverable);
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        LOG.debug("Closing S3 recoverable writer");
    }

    private void checkNotClosed() {
        if (closed.get()) {
            throw new IllegalStateException("RecoverableWriter has been closed");
        }
    }

    public static NativeS3RecoverableWriter writer(
            NativeS3ObjectOperations s3AccessHelper,
            String localTmpDir,
            long userDefinedMinPartSize,
            int maxConcurrentUploadsPerStream) {

        return new NativeS3RecoverableWriter(
                s3AccessHelper, localTmpDir, userDefinedMinPartSize, maxConcurrentUploadsPerStream);
    }
}
