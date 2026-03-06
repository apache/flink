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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Recoverable writer for S3 using multipart uploads for exactly-once semantics. */
@PublicEvolving
public class NativeS3RecoverableWriter implements RecoverableWriter, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(NativeS3RecoverableWriter.class);

    private final NativeS3AccessHelper s3AccessHelper;
    private final String localTmpDir;
    private final long userDefinedMinPartSize;
    private final int maxConcurrentUploadsPerStream;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private NativeS3RecoverableWriter(
            NativeS3AccessHelper s3AccessHelper,
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
        String key = NativeS3AccessHelper.extractKey(path);
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
        return new NativeS3RecoverableFsDataOutputStream(
                s3AccessHelper,
                s3recoverable.getObjectName(),
                s3recoverable.uploadId(),
                localTmpDir,
                userDefinedMinPartSize,
                s3recoverable.parts(),
                s3recoverable.numBytesInParts());
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
            NativeS3AccessHelper s3AccessHelper,
            String localTmpDir,
            long userDefinedMinPartSize,
            int maxConcurrentUploadsPerStream) {

        return new NativeS3RecoverableWriter(
                s3AccessHelper, localTmpDir, userDefinedMinPartSize, maxConcurrentUploadsPerStream);
    }
}
