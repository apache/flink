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

package org.apache.flink.fs.s3native;

import org.apache.flink.core.fs.FSDataOutputStream;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import javax.annotation.Nullable;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

/**
 * S3 output stream for non-recoverable S3 writes.
 *
 * <p><b>Thread Safety:</b> The lock guards write, flush, sync, and close so that {@link #close()}
 * can be safely invoked from another thread (e.g. during task cancellation) per {@link
 * org.apache.flink.core.fs.FSDataOutputStream} contract.
 */
public class NativeS3OutputStream extends FSDataOutputStream {

    private static final int BUFFER_SIZE = 64 * 1024;

    private final S3Client s3Client;
    private final String bucketName;
    private final String key;
    private final File tmpFile;
    private final OutputStream bufferedStream;
    private final S3EncryptionConfig encryptionConfig;

    private final ReentrantLock lock = new ReentrantLock();

    private long position;

    /** Flag to ensure upload happens exactly once. */
    private boolean fileUploaded = false;

    public NativeS3OutputStream(
            S3Client s3Client, String bucketName, String key, String localTmpDir)
            throws IOException {
        this(s3Client, bucketName, key, localTmpDir, null);
    }

    public NativeS3OutputStream(
            S3Client s3Client,
            String bucketName,
            String key,
            String localTmpDir,
            @Nullable S3EncryptionConfig encryptionConfig)
            throws IOException {
        this.s3Client = s3Client;
        this.bucketName = bucketName;
        this.key = key;
        this.encryptionConfig =
                encryptionConfig != null ? encryptionConfig : S3EncryptionConfig.none();

        File tmpDir = new File(localTmpDir);
        if (!tmpDir.exists()) {
            tmpDir.mkdirs();
        }

        this.tmpFile = new File(tmpDir, "s3-upload-" + UUID.randomUUID());
        this.bufferedStream = new BufferedOutputStream(new FileOutputStream(tmpFile), BUFFER_SIZE);
        this.position = 0;
    }

    @Override
    public long getPos() throws IOException {
        lock.lock();
        try {
            return position;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void write(int b) throws IOException {
        lock.lock();
        try {
            if (fileUploaded) {
                throw new IOException("Stream is closed");
            }
            bufferedStream.write(b);
            position++;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        if (b == null) {
            throw new NullPointerException();
        }
        if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        }
        lock.lock();
        try {
            if (fileUploaded) {
                throw new IOException("Stream is closed");
            }
            bufferedStream.write(b, off, len);
            position += len;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void flush() throws IOException {
        lock.lock();
        try {
            if (fileUploaded) {
                throw new IOException("Stream is closed");
            }
            bufferedStream.flush();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Flushes all buffered data and uploads the file to S3.
     *
     * @throws IOException if upload fails
     */
    @Override
    public void sync() throws IOException {
        lock.lock();
        try {
            if (!fileUploaded) {
                fileUploaded = true;
                uploadToS3();
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Closes this output stream and uploads the file to S3.
     *
     * @throws IOException if upload fails
     */
    @Override
    public void close() throws IOException {
        lock.lock();
        try {
            if (!fileUploaded) {
                fileUploaded = true;
                uploadToS3();
            }
        } finally {
            lock.unlock();
        }
    }

    /** Uploads the temp file to S3 and cleans up local resources. */
    private void uploadToS3() throws IOException {
        try {
            bufferedStream.flush();
            bufferedStream.close();

            PutObjectRequest.Builder putRequestBuilder =
                    PutObjectRequest.builder().bucket(bucketName).key(key);

            // Apply encryption settings
            if (encryptionConfig.isEnabled()) {
                putRequestBuilder.serverSideEncryption(encryptionConfig.getServerSideEncryption());
                if (encryptionConfig.getEncryptionType()
                                == S3EncryptionConfig.EncryptionType.SSE_KMS
                        && encryptionConfig.getKmsKeyId() != null) {
                    putRequestBuilder.ssekmsKeyId(encryptionConfig.getKmsKeyId());
                }
            }

            s3Client.putObject(putRequestBuilder.build(), RequestBody.fromFile(tmpFile));
        } finally {
            if (tmpFile.exists()) {
                Files.delete(tmpFile.toPath());
            }
        }
    }
}
