/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.util;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.util.IOUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This test utility class provides a {@link FSDataInputStream} that can block on a latch and takes
 * a latch that the creator can block on until the stream is closed. This is typically used to test
 * that a blocking read can be interrupted / closed.
 */
public class BlockingFSDataInputStream extends FSDataInputStream {

    /** Optional delegate stream to which all ops are forwarded. */
    private final FSDataInputStream delegate;

    /**
     * Optional latch on which the stream blocks, e.g. until the test triggers it after some call to
     * #close().
     */
    private final OneShotLatch triggerUnblock;

    /**
     * Optional latch on which the test can block until the stream is blocked at the desired
     * blocking position.
     */
    private final OneShotLatch waitUntilStreamBlocked;

    /** Closed flag. */
    private final AtomicBoolean closed;

    /** The read position at which this will block. 0 by default. */
    private final long blockAtPosition;

    /** The current read position. */
    private long position;

    public BlockingFSDataInputStream(
            @Nullable OneShotLatch waitForBlock, @Nullable OneShotLatch triggerUnblock) {
        this(null, waitForBlock, triggerUnblock, 0L);
    }

    public BlockingFSDataInputStream(
            @Nullable FSDataInputStream delegate,
            @Nullable OneShotLatch waitForBlock,
            @Nullable OneShotLatch triggerUnblock) {
        this(delegate, waitForBlock, triggerUnblock, 0L);
    }

    public BlockingFSDataInputStream(
            @Nullable FSDataInputStream delegate,
            @Nullable OneShotLatch waitForBlock,
            @Nullable OneShotLatch triggerUnblock,
            long blockAtPosition) {

        this.delegate = delegate;
        this.triggerUnblock = triggerUnblock;
        this.waitUntilStreamBlocked = waitForBlock;
        this.blockAtPosition = blockAtPosition;
        if (delegate != null) {
            try {
                this.position = delegate.getPos();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            this.position = 0;
        }
        this.closed = new AtomicBoolean(false);
    }

    @Override
    public void seek(long desired) throws IOException {
        if (delegate != null) {
            delegate.seek(desired);
        }
        this.position = desired;
    }

    @Override
    public long getPos() throws IOException {
        return position;
    }

    @Override
    public int read() throws IOException {

        if (position == blockAtPosition) {
            unblockWaiter();
            awaitBlocker();
        }

        int val = 0;
        if (delegate != null) {
            try {
                val = delegate.read();
            } catch (IOException ex) {
                unblockWaiter();
                throw ex;
            }
        }

        // We also check for close here, in case the underlying stream does not do this
        if (closed.get()) {
            throw new IOException("Stream closed.");
        } else {
            ++position;
            return val;
        }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        // We override this to ensure that we use the blocking read method internally.
        if (b == null) {
            throw new NullPointerException();
        } else if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }

        int c = read();
        if (c == -1) {
            return -1;
        }
        b[off] = (byte) c;

        int i = 1;
        try {
            for (; i < len; i++) {
                c = read();
                if (c == -1) {
                    break;
                }
                b[off + i] = (byte) c;
            }
        } catch (IOException ee) {
        }
        return i;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            if (delegate != null) {
                IOUtils.closeQuietly(delegate);
            }
            // trigger all the latches, essentially all blocking ops on the stream should resume
            // after close.
            unblockAll();
        }
    }

    private void unblockWaiter() {
        if (null != waitUntilStreamBlocked) {
            waitUntilStreamBlocked.trigger();
        }
    }

    private void awaitBlocker() {
        if (null != triggerUnblock) {
            try {
                triggerUnblock.await();
            } catch (InterruptedException ignored) {
            }
        }
    }

    private void unblockAll() {
        if (null != waitUntilStreamBlocked) {
            waitUntilStreamBlocked.trigger();
        }
        if (null != triggerUnblock) {
            triggerUnblock.trigger();
        }
    }
}
