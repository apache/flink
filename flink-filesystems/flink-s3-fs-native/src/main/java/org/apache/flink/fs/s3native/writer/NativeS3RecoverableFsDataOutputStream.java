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

import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.fs.s3native.writer.NativeS3Recoverable.PartETag;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class NativeS3RecoverableFsDataOutputStream extends RecoverableFsDataOutputStream {

    private final NativeS3AccessHelper s3AccessHelper;
    private final String key;
    private final String uploadId;
    private final String localTmpDir;
    private final long minPartSize;

    private final List<PartETag> completedParts;
    private long numBytesInParts;

    private File currentTempFile;
    private FileOutputStream currentOutputStream;
    private long currentPartSize;
    private final AtomicInteger nextPartNumber;

    private volatile boolean closed;

    public NativeS3RecoverableFsDataOutputStream(
            NativeS3AccessHelper s3AccessHelper,
            String key,
            String uploadId,
            String localTmpDir,
            long minPartSize)
            throws IOException {
        this(s3AccessHelper, key, uploadId, localTmpDir, minPartSize, new ArrayList<>(), 0L);
    }

    public NativeS3RecoverableFsDataOutputStream(
            NativeS3AccessHelper s3AccessHelper,
            String key,
            String uploadId,
            String localTmpDir,
            long minPartSize,
            List<PartETag> existingParts,
            long numBytesInParts)
            throws IOException {
        this.s3AccessHelper = s3AccessHelper;
        this.key = key;
        this.uploadId = uploadId;
        this.localTmpDir = localTmpDir;
        this.minPartSize = minPartSize;
        this.completedParts = Collections.synchronizedList(new ArrayList<>(existingParts));
        this.numBytesInParts = numBytesInParts;
        this.nextPartNumber = new AtomicInteger(existingParts.size() + 1);
        this.currentPartSize = 0;
        this.closed = false;

        createNewTempFile();
    }

    private void createNewTempFile() throws IOException {
        File tmpDir = new File(localTmpDir);
        if (!tmpDir.exists()) {
            tmpDir.mkdirs();
        }

        currentTempFile = new File(tmpDir, "s3-part-" + UUID.randomUUID());
        currentOutputStream = new FileOutputStream(currentTempFile);
        currentPartSize = 0;
    }

    @Override
    public long getPos() throws IOException {
        return numBytesInParts + currentPartSize;
    }

    @Override
    public void write(int b) throws IOException {
        if (closed) {
            throw new IOException("Stream is closed");
        }

        currentOutputStream.write(b);
        currentPartSize++;

        if (currentPartSize >= minPartSize) {
            uploadCurrentPart();
        }
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        if (closed) {
            throw new IOException("Stream is closed");
        }
        if (b == null) {
            throw new NullPointerException();
        }
        if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        }

        currentOutputStream.write(b, off, len);
        currentPartSize += len;

        if (currentPartSize >= minPartSize) {
            uploadCurrentPart();
        }
    }

    @Override
    public void flush() throws IOException {
        if (!closed) {
            currentOutputStream.flush();
        }
    }

    @Override
    public void sync() throws IOException {
        flush();
    }

    private void uploadCurrentPart() throws IOException {
        currentOutputStream.close();

        int partNumber = nextPartNumber.getAndIncrement();
        NativeS3AccessHelper.UploadPartResult result =
                s3AccessHelper.uploadPart(
                        key, uploadId, partNumber, currentTempFile, currentPartSize);

        completedParts.add(new PartETag(result.getPartNumber(), result.getETag()));
        numBytesInParts += currentPartSize;

        Files.delete(currentTempFile.toPath());

        createNewTempFile();
    }

    @Override
    public Committer closeForCommit() throws IOException {
        if (closed) {
            throw new IOException("Stream is already closed");
        }

        closed = true;
        currentOutputStream.close();

        if (currentPartSize > 0) {
            uploadCurrentPart();
        } else {
            Files.delete(currentTempFile.toPath());
        }

        NativeS3Recoverable recoverable =
                new NativeS3Recoverable(
                        key, uploadId, new ArrayList<>(completedParts), numBytesInParts);

        return new NativeS3Committer(s3AccessHelper, recoverable);
    }

    @Override
    public RecoverableWriter.ResumeRecoverable persist() throws IOException {
        flush();

        String incompletePartKey = null;
        long incompletePartLength = 0;

        if (currentPartSize > 0) {
            currentOutputStream.flush();
            incompletePartKey = key + "/.incomplete/" + uploadId + "/" + UUID.randomUUID();
            s3AccessHelper.putObject(incompletePartKey, currentTempFile);
            incompletePartLength = currentPartSize;
        }

        return new NativeS3Recoverable(
                key,
                uploadId,
                new ArrayList<>(completedParts),
                numBytesInParts,
                incompletePartKey,
                incompletePartLength);
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;
            if (currentOutputStream != null) {
                currentOutputStream.close();
            }
            if (currentTempFile != null && currentTempFile.exists()) {
                Files.delete(currentTempFile.toPath());
            }

            try {
                s3AccessHelper.abortMultiPartUpload(key, uploadId);
            } catch (IOException e) {
            }
        }
    }
}
