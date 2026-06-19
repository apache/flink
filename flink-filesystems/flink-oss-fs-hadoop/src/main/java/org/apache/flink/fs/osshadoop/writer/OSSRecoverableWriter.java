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

import org.apache.flink.core.fs.BackPressuringExecutor;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.core.fs.RefCountedFileWithStream;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.fs.osshadoop.OSSAccessor;
import org.apache.flink.util.function.FunctionWithException;

import java.io.File;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.Executor;

/**
 * An implementation of the {@link RecoverableWriter} against OSS.
 *
 * <p>This implementation makes heavy use of MultiPart Uploads in OSS to persist intermediate data
 * as soon as possible.
 */
public class OSSRecoverableWriter implements RecoverableWriter {

    private OSSAccessor ossAccessor;
    private long ossUploadPartSize;
    private int streamConcurrentUploads;
    private Executor executor;

    private final FunctionWithException<File, RefCountedFileWithStream, IOException>
            cachedFileCreator;

    public OSSRecoverableWriter(
            OSSAccessor ossAccessor,
            long ossUploadPartSize,
            int streamConcurrentUploads,
            Executor executor,
            FunctionWithException<File, RefCountedFileWithStream, IOException> cachedFileCreator) {
        this.ossAccessor = ossAccessor;
        this.ossUploadPartSize = ossUploadPartSize;
        this.streamConcurrentUploads = streamConcurrentUploads;
        this.executor = executor;
        this.cachedFileCreator = cachedFileCreator;
    }

    @Override
    public RecoverableFsDataOutputStream open(Path path) throws IOException {
        return new OSSRecoverableFsDataOutputStream(
                ossUploadPartSize,
                cachedFileCreator,
                new OSSRecoverableMultipartUpload(
                        ossAccessor.pathToObject(path),
                        getExecutor(),
                        ossAccessor,
                        Optional.empty(),
                        null,
                        null,
                        0),
                0L);
    }

    @Override
    public OSSRecoverableFsDataOutputStream recover(ResumeRecoverable resumable)
            throws IOException {
        final OSSRecoverable recoverable = (OSSRecoverable) resumable;
        OSSRecoverableMultipartUpload upload =
                new OSSRecoverableMultipartUpload(
                        recoverable.getObjectName(),
                        getExecutor(),
                        ossAccessor,
                        recoverInProgressPart(recoverable),
                        recoverable.getUploadId(),
                        recoverable.getPartETags(),
                        recoverable.getNumBytesInParts());
        return new OSSRecoverableFsDataOutputStream(
                ossUploadPartSize, cachedFileCreator, upload, recoverable.getNumBytesInParts());
    }

    private Optional<File> recoverInProgressPart(OSSRecoverable recoverable) throws IOException {
        String objectKey = recoverable.getLastPartObject();
        if (objectKey == null) {
            return Optional.empty();
        }

        final RefCountedFileWithStream refCountedFile = cachedFileCreator.apply(null);
        final File file = refCountedFile.getFile();

        ossAccessor.getObject(
                objectKey, file.getAbsolutePath(), recoverable.getLastPartObjectLength());

        return Optional.of(file);
    }

    @Override
    public boolean requiresCleanupOfRecoverableState() {
        return true;
    }

    @Override
    public boolean cleanupRecoverableState(ResumeRecoverable resumable) throws IOException {
        final OSSRecoverable ossRecoverable = (OSSRecoverable) resumable;
        final String smallPartObjectToDelete = ossRecoverable.getLastPartObject();
        return smallPartObjectToDelete != null && ossAccessor.deleteObject(smallPartObjectToDelete);
    }

    @Override
    public RecoverableFsDataOutputStream.Committer recoverForCommit(CommitRecoverable resumable)
            throws IOException {
        final OSSRecoverableFsDataOutputStream recovered = recover((OSSRecoverable) resumable);
        return recovered.closeForCommit();
    }

    @Override
    public SimpleVersionedSerializer<CommitRecoverable> getCommitRecoverableSerializer() {
        return (SimpleVersionedSerializer) OSSRecoverableSerializer.INSTANCE;
    }

    @Override
    public SimpleVersionedSerializer<ResumeRecoverable> getResumeRecoverableSerializer() {
        return (SimpleVersionedSerializer) OSSRecoverableSerializer.INSTANCE;
    }

    @Override
    public boolean supportsResume() {
        return true;
    }

    private Executor getExecutor() {
        if (streamConcurrentUploads <= 0) {
            return executor;
        }
        return new BackPressuringExecutor(executor, streamConcurrentUploads);
    }
}
