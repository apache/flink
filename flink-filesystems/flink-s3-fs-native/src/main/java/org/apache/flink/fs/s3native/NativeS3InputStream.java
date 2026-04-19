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
import java.util.concurrent.locks.ReentrantLock;

/**
 * S3 input stream with configurable read-ahead buffer, range-based requests for seek operations,
 * automatic stream reopening on errors, and lazy initialization to minimize memory footprint.
 *
 * <p><b>Thread Safety:</b> Internal state is guarded by a lock to ensure safe concurrent access and
 * resource cleanup.
 *
 * <p><b>Partial reads and {@code seek}:</b> Apache HttpClient tries to drain the remainder of the
 * response body on {@link java.io.InputStream#close()} to reuse connections. After a {@link #seek}
 * or early close, draining can fail against S3-compatible endpoints (for example MinIO) with {@code
 * ConnectionClosedException: Premature end of Content-Length delimited message body}.
 *
 * <p>The {@code close()} path handles this gracefully: it attempts normal connection cleanup first
 * (which preserves HTTP connection reuse for well-behaved servers), and only falls back to {@link
 * ResponseInputStream#abort()} when the connection was already closed early by the server — in
 * which case the connection is not reusable anyway, so aborting carries no performance penalty.
 * Abandoned streams after {@link #seek} always use {@code abort()} immediately since the connection
 * is being discarded regardless.
 */
class NativeS3InputStream extends FSDataInputStream {

    private static final Logger LOG = LoggerFactory.getLogger(NativeS3InputStream.class);

    /** Default read-ahead buffer size: 256KB. */
    private static final int DEFAULT_READ_BUFFER_SIZE = 256 * 1024;

    /** Maximum buffer size for very large sequential reads. */
    private static final int MAX_READ_BUFFER_SIZE = 4 * 1024 * 1024; // 4MB

    private final ReentrantLock lock = new ReentrantLock();

    private final S3Client s3Client;
    private final String bucketName;
    private final String key;
    private final long contentLength;
    private final int readBufferSize;

    private ResponseInputStream<GetObjectResponse> currentStream;
    private BufferedInputStream bufferedStream;
    private long position;
    private volatile boolean closed;

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
     *   <li>Closes any existing stream (using {@code abort()} since it is being discarded)
     *   <li>Opens a new stream starting at {@link #position}
     *   <li>Uses HTTP range requests for non-zero positions
     * </ul>
     */
    private void openStreamAtCurrentPosition() throws IOException {
        lock.lock();
        try {
            releaseCurrentObjectStream(true);

            try {
                GetObjectRequest.Builder requestBuilder =
                        GetObjectRequest.builder().bucket(bucketName).key(key);

                if (position > 0) {
                    requestBuilder.range(String.format("bytes=%d-", position));
                    LOG.debug(
                            "Opening S3 stream with range: bytes={}-{}",
                            position,
                            contentLength - 1);
                } else {
                    LOG.debug("Opening S3 stream for full object: {} bytes", contentLength);
                }
                currentStream = s3Client.getObject(requestBuilder.build());
                bufferedStream = new BufferedInputStream(currentStream, readBufferSize);
            } catch (Exception e) {
                releaseCurrentObjectStream(true);
                throw new IOException("Failed to open S3 stream for " + bucketName + "/" + key, e);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Releases the in-flight {@code GetObject} HTTP response.
     *
     * <p>When {@code abandon} is {@code true}, the stream is being discarded (e.g. after a {@link
     * #seek}) and {@link ResponseInputStream#abort()} is called immediately, bypassing any attempt
     * to drain the response body. This is necessary for correctness because the caller has already
     * moved on and will not consume the rest of the body.
     *
     * <p>When {@code abandon} is {@code false}, normal {@code close()} is attempted first. This
     * preserves HTTP connection reuse for well-behaved S3 servers. If the server closes the
     * connection early (a pattern seen on MinIO and other S3-compatible storage), the resulting
     * {@code ConnectionClosedException} is caught, treated as non-fatal, and escalated to a WARN
     * log. The connection is then aborted since it is no longer usable anyway.
     *
     * @param abandon {@code true} to use {@code abort()} immediately (stream is being abandoned
     *     after seek); {@code false} to try {@code close()} first, falling back to {@code abort()}
     *     on early-connection-close from MinIO
     */
    private void releaseCurrentObjectStream(boolean abandon) {
        if (currentStream == null && bufferedStream == null) {
            return;
        }
        if (abandon && currentStream != null) {
            try {
                currentStream.abort();
            } catch (RuntimeException e) {
                LOG.warn("Error aborting S3 response stream for {}/{}", bucketName, key, e);
            } finally {
                bufferedStream = null;
                currentStream = null;
            }
            return;
        }

        // Normal close path: attempt graceful cleanup to preserve HTTP connection reuse.
        // S3-compatible storage (MinIO) may close the connection before all bytes are drained.
        // In that case the ConnectionClosedException is non-fatal — abort the stream and
        // log a WARN so the task does not fail.
        if (bufferedStream != null) {
            try {
                bufferedStream.close();
            } catch (IOException e) {
                if (isPrematureEndOfMessage(e)) {
                    LOG.warn(
                            "S3 server closed connection prematurely for {}/{} (expected {} bytes) "
                                    + "-- aborting and treating as non-fatal",
                            bucketName,
                            key,
                            contentLength,
                            e);
                    abortSafely();
                } else {
                    LOG.warn("Error closing buffered stream for {}/{}", bucketName, key, e);
                }
            } finally {
                bufferedStream = null;
                currentStream = null;
            }
        } else if (currentStream != null) {
            try {
                currentStream.close();
            } catch (IOException e) {
                if (isPrematureEndOfMessage(e)) {
                    LOG.warn(
                            "S3 server closed connection prematurely for {}/{} (expected {} bytes) "
                                    + "-- aborting and treating as non-fatal",
                            bucketName,
                            key,
                            contentLength,
                            e);
                    abortSafely();
                } else {
                    LOG.warn("Error closing S3 response stream for {}/{}", bucketName, key, e);
                }
            } finally {
                currentStream = null;
            }
        }
    }

    /** Abort the underlying {@code ResponseInputStream} safely, logging any exception as a WARN. */
    private void abortSafely() {
        if (currentStream == null) {
            return;
        }
        try {
            currentStream.abort();
        } catch (RuntimeException e) {
            LOG.warn("Error aborting S3 response stream for {}/{}", bucketName, key, e);
        }
    }

    /**
     * Returns true if the given I/O exception represents a connection closed before all bytes were
     * received — a pattern seen on S3-compatible storage (MinIO) when it closes a connection early
     * and Apache HttpClient tries to drain the remainder.
     */
    private static boolean isPrematureEndOfMessage(IOException e) {
        String msg = e.getMessage() != null ? e.getMessage() : "";
        return msg.contains("Premature end of Content-Length")
                || msg.contains("Connection closed")
                || msg.contains("ConnectionClosed");
    }

    @Override
    public void seek(long desired) throws IOException {
        lock();
        try {
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
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long getPos() throws IOException {
        lock();
        try {
            return position;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int read() throws IOException {
        lock();
        try {
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
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (b == null) {
            throw new NullPointerException("Read buffer must not be null");
        }
        if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException(
                    String.format(
                            "Range [off=%d, len=%d] out of bounds for buffer of length %d",
                            off, len, b.length));
        }
        if (len == 0) {
            return 0;
        }
        lock();
        try {
            if (closed) {
                throw new IOException("Stream is closed");
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
        } finally {
            lock.unlock();
        }
    }

    private void lock() throws IOException {
        try {
            lock.lockInterruptibly();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while acquiring lock", e);
        }
    }

    @Override
    public void close() throws IOException {
        lock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;

            // Only skip normal close and go straight to abort if the stream was fully read.
            // Otherwise we try close() first (to preserve connection reuse), falling back to
            // abort() when MinIO closed the connection early (which makes reuse impossible anyway).
            boolean discardRemaining = position >= contentLength;
            releaseCurrentObjectStream(discardRemaining);

            LOG.debug(
                    "Closed S3 input stream - bucket: {}, key: {}, final position: {}/{}",
                    bucketName,
                    key,
                    position,
                    contentLength);
        } finally {
            lock.unlock();
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
        lock();
        try {
            if (closed) {
                throw new IOException("Stream is closed");
            }
            long remaining = contentLength - position;
            return (int) Math.min(remaining, Integer.MAX_VALUE);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long skip(long n) throws IOException {
        lock();
        try {
            if (closed) {
                throw new IOException("Stream is closed");
            }
            if (n <= 0) {
                return 0;
            }
            long newPos = Math.min(position + n, contentLength);
            long skipped = newPos - position;
            if (newPos != position) {
                position = newPos;
                if (currentStream != null) {
                    openStreamAtCurrentPosition();
                }
            }
            return skipped;
        } finally {
            lock.unlock();
        }
    }
}
