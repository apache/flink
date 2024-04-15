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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.core.fs.RefCountedBufferingFileStream;
import org.apache.flink.core.fs.RefCountedFSOutputStream;
import org.apache.flink.core.fs.RefCountedFileWithStream;
import org.apache.flink.util.function.FunctionWithException;

import org.apache.commons.io.IOUtils;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.File;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.flink.fs.s3.common.FlinkS3FileSystem.S3_MULTIPART_MIN_PART_SIZE;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A RecoverableFsDataOutputStream to S3 that is based on a recoverable multipart upload.
 *
 * <p>This class is NOT thread-safe. Concurrent writes tho this stream result in corrupt or lost
 * data.
 *
 * <p>The {@link #close()} method may be called concurrently when cancelling / shutting down. It
 * will still ensure that local transient resources (like streams and temp files) are cleaned up,
 * but will not touch data previously persisted in S3.
 */
@PublicEvolving
@NotThreadSafe
public final class S3RecoverableFsDataOutputStream extends RecoverableFsDataOutputStream {

    /**
     * Lock that guards the critical sections when new parts are rolled over. Despite the class
     * being declared not thread safe, we protect certain regions to at least enable concurrent
     * close() calls during cancellation or abort/cleanup.
     */
    private final ReentrantLock lock = new ReentrantLock();

    private final RecoverableMultiPartUpload upload;

    private final FunctionWithException<File, RefCountedFileWithStream, IOException>
            tmpFileProvider;

    /**
     * The number of bytes at which we start a new part of the multipart upload. This has to be
     * greater than the non-configurable minimum. That is equal to {@link
     * org.apache.flink.fs.s3.common.FlinkS3FileSystem#S3_MULTIPART_MIN_PART_SIZE
     * S3_MULTIPART_MIN_PART_SIZE} and is set by Amazon.
     */
    private final long userDefinedMinPartSize;

    private RefCountedFSOutputStream fileStream;

    private long bytesBeforeCurrentPart;

    /**
     * Single constructor to initialize all. Actual setup of the parts happens in the factory
     * methods.
     */
    S3RecoverableFsDataOutputStream(
            RecoverableMultiPartUpload upload,
            FunctionWithException<File, RefCountedFileWithStream, IOException> tempFileCreator,
            RefCountedFSOutputStream initialTmpFile,
            long userDefinedMinPartSize,
            long bytesBeforeCurrentPart) {

        checkArgument(bytesBeforeCurrentPart >= 0L);

        this.upload = checkNotNull(upload);
        this.tmpFileProvider = checkNotNull(tempFileCreator);
        this.userDefinedMinPartSize = userDefinedMinPartSize;

        this.fileStream = initialTmpFile;
        this.bytesBeforeCurrentPart = bytesBeforeCurrentPart;
    }

    // ------------------------------------------------------------------------
    //  stream methods
    // ------------------------------------------------------------------------

    @Override
    public void write(int b) throws IOException {
        fileStream.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        fileStream.write(b, off, len);
        openNewPartIfNecessary(userDefinedMinPartSize);
    }

    @Override
    public void flush() throws IOException {
        fileStream.flush();
        openNewPartIfNecessary(userDefinedMinPartSize);
    }

    @Override
    public long getPos() throws IOException {
        return bytesBeforeCurrentPart + fileStream.getPos();
    }

    @Override
    public void sync() throws IOException {
        lock();
        try {
            fileStream.flush();
            openNewPartIfNecessary(userDefinedMinPartSize);
            Committer committer = upload.snapshotAndGetCommitter();
            committer.commitAfterRecovery();
            closeForCommit();
        } finally {
            unlock();
        }
    }

    @Override
    public void close() throws IOException {
        lock();
        try {
            fileStream.flush();
        } finally {
            IOUtils.closeQuietly(fileStream);
            fileStream.release();
            unlock();
        }
    }

    // ------------------------------------------------------------------------
    //  recoverable stream methods
    // ------------------------------------------------------------------------

    @Override
    public RecoverableWriter.ResumeRecoverable persist() throws IOException {
        lock();
        try {
            fileStream.flush();
            openNewPartIfNecessary(userDefinedMinPartSize);

            // We do not stop writing to the current file, we merely limit the upload to the
            // first n bytes of the current file

            return upload.snapshotAndGetRecoverable(fileStream);
        } finally {
            unlock();
        }
    }

    @Override
    public Committer closeForCommit() throws IOException {
        lock();
        try {
            closeAndUploadPart();
            return upload.snapshotAndGetCommitter();
        } finally {
            unlock();
        }
    }

    // ------------------------------------------------------------------------
    //  S3
    // ------------------------------------------------------------------------

    private void openNewPartIfNecessary(long sizeThreshold) throws IOException {
        final long fileLength = fileStream.getPos();
        if (fileLength >= sizeThreshold) {
            lock();
            try {
                uploadCurrentAndOpenNewPart(fileLength);
            } finally {
                unlock();
            }
        }
    }

    private void uploadCurrentAndOpenNewPart(long fileLength) throws IOException {
        bytesBeforeCurrentPart += fileLength;
        closeAndUploadPart();

        // initialize a new temp file
        fileStream = RefCountedBufferingFileStream.openNew(tmpFileProvider);
    }

    private void closeAndUploadPart() throws IOException {
        fileStream.flush();
        fileStream.close();
        if (fileStream.getPos() > 0L) {
            upload.uploadPart(fileStream);
        }
        fileStream.release();
    }

    // ------------------------------------------------------------------------
    //  locking
    // ------------------------------------------------------------------------

    private void lock() throws IOException {
        try {
            lock.lockInterruptibly();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted");
        }
    }

    private void unlock() {
        lock.unlock();
    }

    // ------------------------------------------------------------------------
    //  factory methods
    // ------------------------------------------------------------------------

    public static S3RecoverableFsDataOutputStream newStream(
            final RecoverableMultiPartUpload upload,
            final FunctionWithException<File, RefCountedFileWithStream, IOException> tmpFileCreator,
            final long userDefinedMinPartSize)
            throws IOException {

        checkArgument(userDefinedMinPartSize >= S3_MULTIPART_MIN_PART_SIZE);

        final RefCountedBufferingFileStream fileStream =
                boundedBufferingFileStream(tmpFileCreator, Optional.empty());

        return new S3RecoverableFsDataOutputStream(
                upload, tmpFileCreator, fileStream, userDefinedMinPartSize, 0L);
    }

    public static S3RecoverableFsDataOutputStream recoverStream(
            final RecoverableMultiPartUpload upload,
            final FunctionWithException<File, RefCountedFileWithStream, IOException> tmpFileCreator,
            final long userDefinedMinPartSize,
            final long bytesBeforeCurrentPart)
            throws IOException {

        checkArgument(userDefinedMinPartSize >= S3_MULTIPART_MIN_PART_SIZE);

        final RefCountedBufferingFileStream fileStream =
                boundedBufferingFileStream(tmpFileCreator, upload.getIncompletePart());

        return new S3RecoverableFsDataOutputStream(
                upload, tmpFileCreator, fileStream, userDefinedMinPartSize, bytesBeforeCurrentPart);
    }

    private static RefCountedBufferingFileStream boundedBufferingFileStream(
            final FunctionWithException<File, RefCountedFileWithStream, IOException> tmpFileCreator,
            final Optional<File> incompletePart)
            throws IOException {

        if (!incompletePart.isPresent()) {
            return RefCountedBufferingFileStream.openNew(tmpFileCreator);
        }

        final File file = incompletePart.get();
        return RefCountedBufferingFileStream.restore(tmpFileCreator, file);
    }
}
