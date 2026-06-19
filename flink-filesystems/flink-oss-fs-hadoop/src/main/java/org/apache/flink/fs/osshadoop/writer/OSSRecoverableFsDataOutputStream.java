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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.core.fs.RefCountedBufferingFileStream;
import org.apache.flink.core.fs.RefCountedFileWithStream;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.function.FunctionWithException;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A RecoverableFsDataOutputStream to OSS that is based on a recoverable multipart upload.
 *
 * <p>This class is NOT thread-safe. Concurrent writes tho this stream result in corrupt or lost
 * data.
 *
 * <p>The {@link #close()} method may be called concurrently when cancelling / shutting down. It
 * will still ensure that local transient resources (like streams and temp files) are cleaned up,
 * but will not touch data previously persisted in OSS.
 */
@PublicEvolving
@NotThreadSafe
public class OSSRecoverableFsDataOutputStream extends RecoverableFsDataOutputStream {

    /**
     * Lock that guards the critical sections when new parts are rolled over. Despite the class
     * being declared not thread safe, we protect certain regions to at least enable concurrent
     * close() calls during cancellation or abort/cleanup.
     */
    private final ReentrantLock lock = new ReentrantLock();

    private long ossUploadPartSize;
    private FunctionWithException<File, RefCountedFileWithStream, IOException> cachedFileCreator;

    private RefCountedBufferingFileStream fileStream;

    private OSSRecoverableMultipartUpload upload;
    private long sizeBeforeCurrentPart;

    public OSSRecoverableFsDataOutputStream(
            long ossUploadPartSize,
            FunctionWithException<File, RefCountedFileWithStream, IOException> cachedFileCreator,
            OSSRecoverableMultipartUpload upload,
            long sizeBeforeCurrentPart)
            throws IOException {
        this.ossUploadPartSize = ossUploadPartSize;
        this.cachedFileCreator = cachedFileCreator;
        this.upload = upload;

        if (upload.getIncompletePart().isPresent()) {
            this.fileStream =
                    RefCountedBufferingFileStream.restore(
                            this.cachedFileCreator, upload.getIncompletePart().get());
        } else {
            this.fileStream = RefCountedBufferingFileStream.openNew(this.cachedFileCreator);
        }
        this.sizeBeforeCurrentPart = sizeBeforeCurrentPart;
    }

    @Override
    public RecoverableWriter.ResumeRecoverable persist() throws IOException {
        lock();
        try {
            fileStream.flush();

            switchNewPartFileIfNecessary(ossUploadPartSize);

            return upload.getRecoverable(fileStream);
        } finally {
            unlock();
        }
    }

    @Override
    public Committer closeForCommit() throws IOException {
        lock();
        try {
            uploadCurrentPart();
            return upload.getCommitter();
        } finally {
            unlock();
        }
    }

    private void uploadCurrentPart() throws IOException {
        fileStream.flush();
        fileStream.close();
        if (fileStream.getPos() > 0L) {
            upload.uploadPart(fileStream);
        }
        fileStream.release();
    }

    @Override
    public long getPos() throws IOException {
        return sizeBeforeCurrentPart + fileStream.getPos();
    }

    @Override
    public void write(int b) throws IOException {
        fileStream.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        fileStream.write(b, off, len);
        switchNewPartFileIfNecessary(ossUploadPartSize);
    }

    @Override
    public void flush() throws IOException {
        fileStream.flush();
        switchNewPartFileIfNecessary(ossUploadPartSize);
    }

    @Override
    public void sync() throws IOException {
        fileStream.sync();
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

    private void lock() throws IOException {
        try {
            lock.lockInterruptibly();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted exception: " + e);
        }
    }

    private void unlock() {
        lock.unlock();
    }

    private void switchNewPartFileIfNecessary(long partSizeThreshold) throws IOException {
        final long length = fileStream.getPos();
        if (length >= partSizeThreshold) {
            lock();
            try {
                sizeBeforeCurrentPart += fileStream.getPos();

                uploadCurrentPart();

                fileStream = RefCountedBufferingFileStream.openNew(cachedFileCreator);
            } finally {
                unlock();
            }
        }
    }
}
