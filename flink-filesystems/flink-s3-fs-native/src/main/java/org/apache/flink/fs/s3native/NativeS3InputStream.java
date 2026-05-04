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

import javax.annotation.concurrent.GuardedBy;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.concurrent.locks.ReentrantLock;

/**
 * S3 input stream with configurable read-ahead buffer, lazy seek, and automatic stream reopening.
 *
 * <p>{@link #seek(long)} only records the desired position without performing any I/O. All HTTP
 * work is deferred to the next {@link #read()} call via {@link #lazySeek()}, so multiple seeks
 * between reads coalesce. When the seek is forward and within {@code readBufferSize}, bytes are
 * skipped in-buffer instead of reopening the HTTP connection.
 */
class NativeS3InputStream extends FSDataInputStream {

    private static final Logger LOG = LoggerFactory.getLogger(NativeS3InputStream.class);

    /** Default read-ahead buffer size. */
    private static final int DEFAULT_READ_BUFFER_SIZE = 256 * 1024;

    private final ReentrantLock lock = new ReentrantLock();

    private final S3Client s3Client;
    private final String bucketName;
    private final String key;
    private final long contentLength;
    private final int readBufferSize;

    @GuardedBy("lock")
    private ResponseInputStream<GetObjectResponse> currentStream;

    @GuardedBy("lock")
    private BufferedInputStream bufferedStream;

    /**
     * The position the caller expects to read from next. Updated by {@link #seek(long)}, {@link
     * #skip(long)}, and after every successful {@link #read()}.
     */
    @GuardedBy("lock")
    private long nextReadPos;

    /**
     * The actual byte offset of the underlying stream cursor, reconciled lazily via {@link
     * #lazySeek()}.
     */
    @GuardedBy("lock")
    private long streamPos;

    @GuardedBy("lock")
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
        this.readBufferSize = readBufferSize;
        this.nextReadPos = 0;
        this.streamPos = 0;
        this.closed = false;

        LOG.debug(
                "Created S3 input stream - bucket: {}, key: {}, size: {} bytes, buffer: {} KB",
                bucketName,
                key,
                contentLength,
                this.readBufferSize / 1024);
    }

    /** Reconciles {@link #nextReadPos} and {@link #streamPos} before reading bytes. */
    @GuardedBy("lock")
    private void lazySeek() throws IOException {
        assert lock.isHeldByCurrentThread() : "lazySeek() requires lock to be held";
        long targetPos = nextReadPos;

        if (currentStream == null) {
            streamPos = targetPos;
            return;
        }

        if (targetPos == streamPos) {
            return;
        }

        long diff = targetPos - streamPos;
        streamPos = targetPos;

        if (targetPos >= contentLength) {
            releaseStreams();
            return;
        }

        // BufferedInputStream does not expose how many bytes are in its local array, so
        // readBufferSize is used as the skip threshold: at most readBufferSize bytes may be
        // consumed from the live HTTP connection before a range request is preferred instead.
        if (diff > 0 && diff <= (long) readBufferSize) {
            skipBytesInBuffer(diff);
            return;
        }

        openStreamAtCurrentPosition();
    }

    @GuardedBy("lock")
    private void ensureStreamOpen() throws IOException {
        assert lock.isHeldByCurrentThread() : "ensureStreamOpen() requires lock to be held";
        if (currentStream == null && !closed) {
            openStreamAtCurrentPosition();
        }
    }

    @GuardedBy("lock")
    private void skipBytesInBuffer(long n) throws IOException {
        assert lock.isHeldByCurrentThread() : "skipBytesInBuffer() requires lock to be held";
        long remaining = n;
        while (remaining > 0) {
            long skipped = bufferedStream.skip(remaining);
            if (skipped <= 0) {
                openStreamAtCurrentPosition();
                return;
            }
            remaining -= skipped;
        }
    }

    /** Opens (or reopens) the S3 stream at {@link #streamPos}. */
    private void openStreamAtCurrentPosition() throws IOException {
        lock.lock();
        try {
            releaseStreams();

            try {
                GetObjectRequest.Builder requestBuilder =
                        GetObjectRequest.builder().bucket(bucketName).key(key);

                if (streamPos > 0) {
                    requestBuilder.range(String.format("bytes=%d-", streamPos));
                    LOG.debug(
                            "Opening S3 stream with range: bytes={}-{}",
                            streamPos,
                            contentLength - 1);
                } else {
                    LOG.debug("Opening S3 stream for full object: {} bytes", contentLength);
                }
                currentStream = s3Client.getObject(requestBuilder.build());
                bufferedStream = new BufferedInputStream(currentStream, readBufferSize);
            } catch (Exception e) {
                releaseStreams();
                throw new IOException("Failed to open S3 stream for " + bucketName + "/" + key, e);
            }
        } finally {
            lock.unlock();
        }
    }

    /** Aborts the in-flight HTTP connection to avoid draining remaining bytes on close. */
    @GuardedBy("lock")
    private void abortCurrentStream() {
        assert lock.isHeldByCurrentThread() : "abortCurrentStream() requires lock to be held";
        if (currentStream != null) {
            try {
                currentStream.abort();
            } catch (RuntimeException e) {
                LOG.warn("Error aborting S3 response stream for {}/{}", bucketName, key, e);
            }
        }
    }

    /**
     * Aborts and closes both streams, nulling the references.
     *
     * @return the first {@link IOException} encountered (with subsequent ones added as suppressed),
     *     or {@code null} if cleanup succeeded
     */
    @GuardedBy("lock")
    private IOException releaseStreams() {
        assert lock.isHeldByCurrentThread() : "releaseStreams() requires lock to be held";
        abortCurrentStream();
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
        return exception;
    }

    @Override
    public void seek(long desired) throws IOException {
        lock();
        try {
            if (closed) {
                throw new IOException("Stream is closed");
            }
            if (desired < 0) {
                throw new EOFException("Cannot seek to negative position: " + desired);
            }
            if (desired > contentLength) {
                throw new EOFException(
                        "Cannot seek past end of stream: position="
                                + desired
                                + ", length="
                                + contentLength);
            }

            nextReadPos = desired;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long getPos() throws IOException {
        lock();
        try {
            return nextReadPos;
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
            if (nextReadPos >= contentLength) {
                return -1;
            }
            lazySeek();
            ensureStreamOpen();
            int data = bufferedStream.read();
            if (data != -1) {
                nextReadPos++;
                streamPos++;
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
            if (nextReadPos >= contentLength) {
                return -1;
            }
            lazySeek();
            ensureStreamOpen();
            long remaining = contentLength - nextReadPos;
            int toRead = (int) Math.min(len, remaining);
            int bytesRead = bufferedStream.read(b, off, toRead);
            if (bytesRead > 0) {
                nextReadPos += bytesRead;
                streamPos += bytesRead;
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

            IOException exception = releaseStreams();

            LOG.debug(
                    "Closed S3 input stream - bucket: {}, key: {}, final position: {}/{}",
                    bucketName,
                    key,
                    nextReadPos,
                    contentLength);
            if (exception != null) {
                throw exception;
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int available() throws IOException {
        lock();
        try {
            if (closed) {
                throw new IOException("Stream is closed");
            }
            long remaining = contentLength - nextReadPos;
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
            long newPos = Math.min(nextReadPos + n, contentLength);
            long skipped = newPos - nextReadPos;
            nextReadPos = newPos;
            return skipped;
        } finally {
            lock.unlock();
        }
    }
}
