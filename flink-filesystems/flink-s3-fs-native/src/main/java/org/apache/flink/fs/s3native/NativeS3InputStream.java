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

import org.apache.flink.core.fs.FSDataInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.BufferedInputStream;
import java.io.IOException;

/**
 * S3 input stream with configurable read-ahead buffer, range-based requests for seek operations,
 * automatic stream reopening on errors, and lazy initialization to minimize memory footprint.
 */
public class NativeS3InputStream extends FSDataInputStream {

    private static final Logger LOG = LoggerFactory.getLogger(NativeS3InputStream.class);

    /** Default read-ahead buffer size: 256KB. */
    private static final int DEFAULT_READ_BUFFER_SIZE = 256 * 1024;

    /** Maximum buffer size for very large sequential reads. */
    private static final int MAX_READ_BUFFER_SIZE = 4 * 1024 * 1024; // 4MB

    private final S3Client s3Client;
    private final String bucketName;
    private final String key;
    private final long contentLength;
    private final int readBufferSize;

    private ResponseInputStream<GetObjectResponse> currentStream;
    private BufferedInputStream bufferedStream;
    private long position;
    private boolean closed;

    public NativeS3InputStream(
            S3Client s3Client, String bucketName, String key, long contentLength) {
        this(s3Client, bucketName, key, contentLength, DEFAULT_READ_BUFFER_SIZE);
    }

    public NativeS3InputStream(
            S3Client s3Client,
            String bucketName,
            String key,
            long contentLength,
            int readBufferSize) {
        this.s3Client = s3Client;
        this.bucketName = bucketName;
        this.key = key;
        this.contentLength = contentLength;
        this.readBufferSize = Math.min(readBufferSize, MAX_READ_BUFFER_SIZE);
        this.position = 0;
        this.closed = false;

        LOG.debug(
                "Created S3 input stream - bucket: {}, key: {}, size: {} bytes, buffer: {} KB",
                bucketName,
                key,
                contentLength,
                this.readBufferSize / 1024);
    }

    private void lazyInitialize() throws IOException {
        if (currentStream == null && !closed) {
            openStreamAtCurrentPosition();
        }
    }

    /**
     * Opens (or reopens) the S3 stream at the current position.
     *
     * <p>This method:
     *
     * <ul>
     *   <li>Closes any existing stream
     *   <li>Opens a new stream starting at {@link #position}
     *   <li>Uses HTTP range requests for non-zero positions
     * </ul>
     */
    private void openStreamAtCurrentPosition() throws IOException {

        if (bufferedStream != null) {
            try {
                bufferedStream.close();
            } catch (IOException e) {
                LOG.warn("Error closing buffered stream for {}/{}", bucketName, key, e);
            } finally {
                bufferedStream = null;
            }
        }
        if (currentStream != null) {
            try {
                currentStream.close();
            } catch (IOException e) {
                LOG.warn("Error closing S3 response stream for {}/{}", bucketName, key, e);
            } finally {
                currentStream = null;
            }
        }

        try {
            GetObjectRequest.Builder requestBuilder =
                    GetObjectRequest.builder().bucket(bucketName).key(key);

            if (position > 0) {
                requestBuilder.range(String.format("bytes=%d-", position));
                LOG.debug("Opening S3 stream with range: bytes={}-{}", position, contentLength - 1);
            } else {
                LOG.debug("Opening S3 stream for full object: {} bytes", contentLength);
            }
            currentStream = s3Client.getObject(requestBuilder.build());
            bufferedStream = new BufferedInputStream(currentStream, readBufferSize);
        } catch (Exception e) {
            if (bufferedStream != null) {
                try {
                    bufferedStream.close();
                } catch (IOException ignored) {
                }
                bufferedStream = null;
            }
            if (currentStream != null) {
                try {
                    currentStream.close();
                } catch (IOException ignored) {
                }
                currentStream = null;
            }
            throw new IOException("Failed to open S3 stream for " + bucketName + "/" + key, e);
        }
    }

    @Override
    public void seek(long desired) throws IOException {
        if (closed) {
            throw new IOException("Stream is closed");
        }
        if (desired < 0) {
            throw new IOException("Cannot seek to negative position: " + desired);
        }

        if (desired != position) {
            position = desired;
            if (currentStream != null) {
                openStreamAtCurrentPosition();
            }
        }
    }

    @Override
    public long getPos() throws IOException {
        return position;
    }

    @Override
    public int read() throws IOException {
        if (closed) {
            throw new IOException("Stream is closed");
        }
        lazyInitialize();
        if (position >= contentLength) {
            return -1;
        }
        int data = bufferedStream.read();
        if (data != -1) {
            position++;
        }
        return data;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (closed) {
            throw new IOException("Stream is closed");
        }
        if (b == null) {
            throw new NullPointerException();
        }
        if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        }
        if (len == 0) {
            return 0;
        }
        lazyInitialize();
        if (position >= contentLength) {
            return -1;
        }
        long remaining = contentLength - position;
        int toRead = (int) Math.min(len, remaining);
        int bytesRead = bufferedStream.read(b, off, toRead);
        if (bytesRead > 0) {
            position += bytesRead;
        }
        return bytesRead;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }

        closed = true;
        IOException exception = null;

        if (bufferedStream != null) {
            try {
                bufferedStream.close();
            } catch (IOException e) {
                exception = e;
                LOG.warn("Error closing buffered stream for {}/{}", bucketName, key, e);
            } finally {
                bufferedStream = null;
            }
        }

        if (currentStream != null) {
            try {
                currentStream.close();
            } catch (IOException e) {
                if (exception == null) {
                    exception = e;
                } else {
                    exception.addSuppressed(e);
                }
                LOG.warn("Error closing S3 response stream for {}/{}", bucketName, key, e);
            } finally {
                currentStream = null;
            }
        }

        LOG.debug(
                "Closed S3 input stream - bucket: {}, key: {}, final position: {}/{}",
                bucketName,
                key,
                position,
                contentLength);
        if (exception != null) {
            throw exception;
        }
    }

    /**
     * Returns an estimate of the number of bytes that can be read without blocking.
     *
     * <p>This implementation returns the remaining bytes in the object based on content length and
     * current position. Note that actual reads may still block due to network I/O, but this
     * indicates how much data is logically available.
     *
     * @return the number of remaining bytes (capped at Integer.MAX_VALUE)
     * @throws IOException if the stream has been closed
     */
    @Override
    public int available() throws IOException {
        if (closed) {
            throw new IOException("Stream is closed");
        }

        long remaining = contentLength - position;
        return (int) Math.min(remaining, Integer.MAX_VALUE);
    }

    @Override
    public long skip(long n) throws IOException {
        if (closed) {
            throw new IOException("Stream is closed");
        }
        if (n <= 0) {
            return 0;
        }

        long newPos = Math.min(position + n, contentLength);
        long skipped = newPos - position;
        seek(newPos);
        return skipped;
    }
}
