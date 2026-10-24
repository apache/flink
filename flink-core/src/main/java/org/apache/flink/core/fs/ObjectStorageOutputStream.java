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
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Object storage output stream that delegates writes to an underlying {@link OutputStream}.
 *
 * <p>The underlying stream may be a raw cloud stream or a wrapped stream (e.g., a compressing or
 * encrypting stream). The delegate may buffer writes internally and upload blocks asynchronously.
 * On {@link #close()} or {@link #sync()}, all buffered data is flushed and the stream is committed,
 * making the file visible.
 *
 * <p>Thread-safety: all public methods are guarded by a {@link ReentrantLock}. {@link #close()} and
 * {@link #sync()} use non-interruptible {@code lock()} so they always complete — stream cleanup
 * must run to avoid resource leaks or silent data corruption. Other methods use {@code
 * lockInterruptibly()} and throw {@link IOException} wrapping {@link InterruptedException} when
 * interrupted.
 *
 * @see FSDataOutputStream
 */
@Internal
@Experimental
@ThreadSafe
public final class ObjectStorageOutputStream extends FSDataOutputStream {

    private final OutputStream delegate;

    /** Path to the file within the storage system. Used in diagnostic messages. */
    private final String filePath;

    /** Lock guarding all mutable state. */
    private final ReentrantLock lock = new ReentrantLock();

    @GuardedBy("lock")
    private long position;

    @GuardedBy("lock")
    private boolean closed;

    /**
     * Creates a new object storage output stream wrapping an already-opened delegate stream.
     *
     * @param delegate the underlying output stream (e.g., an SDK stream or a cipher-wrapped stream)
     * @param filePath the path to the file within the storage system (used in diagnostic messages)
     */
    public ObjectStorageOutputStream(final OutputStream delegate, final String filePath) {
        this.delegate = Preconditions.checkNotNull(delegate, "delegate");
        this.filePath = Preconditions.checkNotNull(filePath, "filePath");
        this.position = 0;
    }

    /**
     * Returns the current position in the stream.
     *
     * @return the number of bytes written so far
     * @throws IOException if interrupted while acquiring the lock, or if the stream is closed
     */
    @Override
    public long getPos() throws IOException {
        acquireLockInterruptibly();
        try {
            checkNotClosed();
            return position;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Writes a single byte to the stream.
     *
     * @param b the byte to write (only the low 8 bits are used)
     * @throws IOException if interrupted while acquiring the lock, if the stream is closed, or if
     *     an I/O error occurs
     */
    @Override
    public void write(final int b) throws IOException {
        acquireLockInterruptibly();
        try {
            checkNotClosed();
            delegate.write(b);
            position++;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Writes bytes from the specified byte array to this output stream.
     *
     * @param b the data to write
     * @param off the start offset in the data
     * @param len the number of bytes to write
     * @throws IOException if interrupted while acquiring the lock, if the stream is closed, or if
     *     an I/O error occurs
     * @throws NullPointerException if {@code b} is null
     * @throws IndexOutOfBoundsException if {@code off} or {@code len} is out of bounds
     */
    @Override
    public void write(final byte[] b, final int off, final int len) throws IOException {
        acquireLockInterruptibly();
        try {
            checkNotClosed();
            Preconditions.checkNotNull(b, "buffer");
            Objects.checkFromIndexSize(off, len, b.length);
            delegate.write(b, off, len);
            position += len;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Flushes this output stream and forces any buffered data to be uploaded.
     *
     * @throws IOException if interrupted while acquiring the lock, if the stream is closed, or if
     *     an I/O error occurs
     */
    @Override
    public void flush() throws IOException {
        acquireLockInterruptibly();
        try {
            checkNotClosed();
            delegate.flush();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Flushes all buffered data and commits the file, making it visible to readers.
     *
     * <p>This closes the underlying stream, triggering the commit. After this method returns
     * successfully, the file is durable and visible. No further writes are possible — subsequent
     * write or flush calls will throw {@link IOException}.
     *
     * <p>Calling {@code sync()} on an already-synced or closed stream is a no-op.
     *
     * <p>Uses non-interruptible {@code lock()} so that the commit always completes even if the
     * calling thread is interrupted.
     *
     * @throws IOException if the commit fails
     */
    @Override
    public void sync() throws IOException {
        commitAndClose();
    }

    /**
     * Closes this output stream and attempts to commit the file.
     *
     * <p>On the happy path, callers should use {@link #sync()} before close to guarantee data
     * visibility. If {@code sync()} was already called, this method is a no-op.
     *
     * <p>This method is safe to call from any thread (e.g., task cancellation or safety-net
     * cleanup). Uses non-interruptible {@code lock()} so that stream cleanup always completes.
     *
     * @throws IOException if closing the stream fails
     */
    @Override
    public void close() throws IOException {
        commitAndClose();
    }

    private void commitAndClose() throws IOException {
        lock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            flushAndCloseDelegate();
        } finally {
            lock.unlock();
        }
    }

    private void flushAndCloseDelegate() throws IOException {
        Exception primaryException = null;
        try {
            delegate.flush();
        } catch (final Exception e) {
            primaryException = e;
        }
        try {
            delegate.close();
        } catch (final Exception closeEx) {
            if (primaryException != null) {
                primaryException.addSuppressed(closeEx);
            } else {
                primaryException = closeEx;
            }
        }
        if (primaryException != null) {
            ExceptionUtils.rethrowIOException(primaryException);
        }
    }

    @GuardedBy("lock")
    private void checkNotClosed() throws IOException {
        Preconditions.checkState(lock.isHeldByCurrentThread());
        if (closed) {
            throw new IOException("Stream already closed: " + filePath);
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
            throw new IOException(
                    "Interrupted while waiting to acquire stream lock: " + filePath, e);
        }
    }
}
