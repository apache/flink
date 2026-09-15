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

package org.apache.flink.core.fs;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Object storage input stream with configurable read-ahead buffer and range-based requests for seek
 * operations.
 *
 * <p>Thread-safety: all public methods are guarded by a {@link ReentrantLock}. {@link #close()} is
 * non-interruptible and safe to call from any thread (e.g., task cancellation).
 *
 * <p>Extension: use {@link InputStreamExtension#buffering(InputStreamOpener, int)} for the default
 * behavior. Pass a custom {@link InputStreamExtension} to customize stream opening.
 *
 * @see InputStreamExtension
 */
@Internal
@Experimental
@ThreadSafe
public final class ObjectStorageInputStream extends FSDataInputStream {

    private final long contentLength;
    private final long skipOnSeekThreshold;

    private final InputStreamExtension inputStreamExtension;
    private final StreamContextImpl streamContext = new StreamContextImpl();

    /** Lock guarding all mutable state. */
    private final ReentrantLock lock = new ReentrantLock();

    @Nullable
    @GuardedBy("lock")
    private InputStream sdkStream;

    @Nullable
    @GuardedBy("lock")
    private InputStream wrappedStream;

    /** Current byte position. Written and read under {@code lock}. */
    @GuardedBy("lock")
    private long position;

    @GuardedBy("lock")
    private boolean closed;

    /**
     * Creates a new object storage input stream.
     *
     * <p>Use {@link InputStreamExtension#buffering(InputStreamOpener, int)} for the default
     * behavior (opens via the opener and wraps with {@link java.io.BufferedInputStream}).
     *
     * @param contentLength content length in bytes; must be &ge; 0
     * @param skipOnSeekThreshold maximum forward seek distance (bytes) handled by read-and-discard
     *     instead of close-and-reopen; must be non-negative (0 disables read-and-discard)
     * @param inputStreamExtension extension for stream opening
     */
    public ObjectStorageInputStream(
            final long contentLength,
            final long skipOnSeekThreshold,
            final InputStreamExtension inputStreamExtension) {
        Preconditions.checkArgument(contentLength >= 0, "contentLength must be non-negative");
        Preconditions.checkArgument(
                skipOnSeekThreshold >= 0, "skipOnSeekThreshold must be non-negative");
        this.contentLength = contentLength;
        this.skipOnSeekThreshold = skipOnSeekThreshold;
        this.inputStreamExtension = Preconditions.checkNotNull(inputStreamExtension, "extension");
        this.position = 0;
    }

    @GuardedBy("lock")
    private void lazyInitialize() throws IOException {
        checkLocked();
        if (sdkStream == null && !closed) {
            openStream();
        }
    }

    /**
     * Closes the current streams and opens new ones at the current {@linkplain #getPos() position}.
     *
     * <p>If {@code openStream()} throws, the previous streams are already closed but no new streams
     * are set — the next read will retry.
     */
    @GuardedBy("lock")
    private void openStream() throws IOException {
        checkLocked();
        closeCurrentStream();
        final RawAndWrappedInputStreams pair = inputStreamExtension.openStream(streamContext);
        this.sdkStream = pair.sdk();
        this.wrappedStream = pair.wrapped();
    }

    /**
     * Closes the current underlying streams. If {@code wrappedStream} is present it is closed first
     * (which also closes the wrapped {@code sdkStream}). If that close fails, a direct close of
     * {@code sdkStream} is attempted so the remote connection is not leaked.
     *
     * @throws IOException if closing the stream(s) fails
     */
    @GuardedBy("lock")
    private void closeCurrentStream() throws IOException {
        checkLocked();
        if (wrappedStream != null) {
            try {
                // Closing the wrapper typically closes the underlying stream as well.
                wrappedStream.close();
            } catch (final IOException e) {
                // wrappedStream.close() may have failed before closing the underlying stream.
                // Try closing it directly so we don't leak the connection.
                if (sdkStream != null) {
                    try {
                        sdkStream.close();
                    } catch (final IOException suppressed) {
                        // Avoid self-suppression: the wrapper's close() delegates to
                        // sdkStream.close(), so both may throw the same exception instance.
                        if (suppressed != e) {
                            e.addSuppressed(suppressed);
                        }
                    }
                }
                throw e;
            } finally {
                wrappedStream = null;
                sdkStream = null;
            }
        } else if (sdkStream != null) {
            try {
                sdkStream.close();
            } finally {
                sdkStream = null;
            }
        }
    }

    /**
     * Size of the throwaway buffer used in read-and-discard seek. The exact size does not matter
     * much: actual I/O granularity is governed by the SDK's internal chunk buffer, not this array.
     */
    private static final int SKIP_BUFFER_SIZE = 8192;

    /**
     * Seeks to the specified position in the stream.
     *
     * <p>For forward seeks within {@code skipThreshold}, bytes are read-and-discarded from the
     * current stream. This uses {@code read()} rather than {@code skip()} because {@link
     * java.io.BufferedInputStream#skip} can delegate to the underlying SDK stream's {@code skip()},
     * which discards its internal buffer even when the target falls within already-fetched data.
     * Reading-and-discarding flows through the normal read path, consuming bytes from the SDK's
     * existing buffer without additional HTTP requests.
     *
     * <p>For larger forward seeks or any backward seek, the stream is closed and reopened lazily
     * with a range request at the new position — this costs one HTTP request regardless of
     * distance.
     *
     * @param desired the position to seek to (byte offset from beginning)
     * @throws IOException if the position is negative, beyond the file size, or the stream is
     *     closed
     */
    @Override
    public void seek(final long desired) throws IOException {
        acquireLockInterruptibly();
        try {
            checkNotClosed();
            if (desired < 0 || desired > contentLength) {
                throw new IOException(
                        "Cannot seek to position "
                                + desired
                                + " (file size: "
                                + contentLength
                                + ")");
            }
            seekInternal(desired);
        } finally {
            lock.unlock();
        }
    }

    @GuardedBy("lock")
    private void seekInternal(final long desired) throws IOException {
        checkLocked();
        if (desired == position) {
            return;
        }
        final long delta = desired - position;
        if (delta > 0 && delta <= skipOnSeekThreshold && wrappedStream != null) {
            readAndDiscard(delta);
        } else {
            position = desired;
            closeCurrentStream();
        }
    }

    /** Advances the stream by reading-and-discarding {@code delta} bytes. */
    @GuardedBy("lock")
    private void readAndDiscard(final long delta) throws IOException {
        if (delta == 0) {
            return;
        }
        checkLocked();
        Preconditions.checkState(
                wrappedStream != null, "readAndDiscard requires an open wrapped stream");
        final byte[] skipBuf = new byte[(int) Math.min(delta, SKIP_BUFFER_SIZE)];
        long remaining = delta;
        while (remaining > 0L) {
            final int toRead = (int) Math.min(remaining, skipBuf.length);
            final int bytesRead = wrappedStream.read(skipBuf, 0, toRead);
            if (bytesRead <= 0) {
                break;
            }
            remaining -= bytesRead;
            position += bytesRead;
        }
        if (remaining > 0L) {
            throw new IOException(
                    "Unexpected end of stream during read-and-discard seek: "
                            + remaining
                            + " bytes remaining");
        }
    }

    /**
     * Returns the current position in the stream.
     *
     * @return the byte offset from the beginning of the stream
     */
    @Override
    public long getPos() {
        lock.lock();
        try {
            return position;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Reads a single byte from the stream.
     *
     * @return the byte read as an integer (0-255), or -1 if end of stream is reached
     * @throws IOException if the stream is closed or an I/O error occurs
     */
    @Override
    public int read() throws IOException {
        acquireLockInterruptibly();
        try {
            checkNotClosed();
            if (position >= contentLength) {
                return -1;
            }
            lazyInitialize();
            final int data = wrappedStream.read();
            if (data != -1) {
                position++;
            }
            return data;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Reads bytes into a portion of a byte array.
     *
     * @param b the buffer into which the data is read
     * @param off the start offset in array {@code b} at which the data is written
     * @param len the maximum number of bytes to read
     * @return the number of bytes read, or -1 if end of stream is reached
     * @throws IOException if the stream is closed or an I/O error occurs
     * @throws NullPointerException if {@code b} is null
     * @throws IndexOutOfBoundsException if {@code off} or {@code len} is negative, or {@code len}
     *     is greater than {@code b.length - off}
     */
    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
        acquireLockInterruptibly();
        try {
            checkNotClosed();
            Preconditions.checkNotNull(b, "buffer");
            Objects.checkFromIndexSize(off, len, b.length);
            if (len == 0) {
                return 0;
            }
            if (position >= contentLength) {
                return -1;
            }
            lazyInitialize();
            final long remaining = contentLength - position;
            final int toRead = (int) Math.min(len, remaining);
            final int bytesRead = wrappedStream.read(b, off, toRead);
            if (bytesRead > 0) {
                position += bytesRead;
            }
            return bytesRead;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Idempotently closes this stream and releases the underlying connection.
     *
     * <p>Uses non-interruptible {@code lock()} instead of {@code lockInterruptibly()} so that close
     * always completes — stream cleanup must run to avoid resource leaks.
     *
     * @throws IOException if closing the underlying stream fails
     */
    @Override
    public void close() throws IOException {
        lock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            closeCurrentStream();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns an estimate of the number of bytes that can be read without blocking.
     *
     * @return the number of remaining bytes in the blob, capped at {@link Integer#MAX_VALUE}
     * @throws IOException if the stream is closed
     */
    @Override
    public int available() throws IOException {
        acquireLockInterruptibly();
        try {
            checkNotClosed();
            final long remaining = Math.max(0L, contentLength - position);
            return (int) Math.min(remaining, Integer.MAX_VALUE);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Skips over and discards {@code n} bytes of data from this input stream.
     *
     * @param n the number of bytes to skip
     * @return the actual number of bytes skipped (may be less if end of stream is reached)
     * @throws IOException if the stream is closed or an I/O error occurs
     */
    @Override
    public long skip(final long n) throws IOException {
        acquireLockInterruptibly();
        try {
            checkNotClosed();
            if (n <= 0) {
                return 0;
            }
            final long newPos = Math.min(position + n, contentLength);
            final long skipped = newPos - position;
            seekInternal(newPos);
            return skipped;
        } finally {
            lock.unlock();
        }
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    private final class StreamContextImpl implements InputStreamExtension.StreamContext {

        private StreamContextImpl() {}

        @Override
        public long getPos() {
            checkLocked();
            return position;
        }

        @Override
        public long getContentLength() {
            return contentLength;
        }
    }

    /** Asserts that the current thread holds the lock. */
    private void checkLocked() {
        Preconditions.checkState(lock.isHeldByCurrentThread());
    }

    private void checkNotClosed() throws IOException {
        checkLocked();
        if (closed) {
            throw new IOException("Stream is closed");
        }
    }

    /**
     * Acquires the lock, wrapping {@link InterruptedException} as {@link IOException} while
     * restoring the interrupt flag.
     */
    private void acquireLockInterruptibly() throws IOException {
        try {
            lock.lockInterruptibly();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting to acquire stream lock", e);
        }
    }
}
