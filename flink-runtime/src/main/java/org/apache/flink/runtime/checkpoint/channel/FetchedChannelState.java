/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Sealed container for recovered channel-state data written to spill files.
 *
 * <p>Holds a list of file paths (in write order). Segment boundaries are self-described in disk
 * segment headers ([4B gateIdx][4B channelIdx][4B bufferLength]), so no in-memory segment locator
 * table is maintained. The reader scans files sequentially, reading each 12-byte header to obtain
 * the channel info and body length.
 *
 * <p>The file list grows as the writer rotates to new files (one rotation per 64 MB soft limit),
 * and is sealed on writer close.
 *
 * <p>File lifecycle is managed by {@link #acquire()} / {@link #release()} reference counting. Files
 * are deleted only when the last lifecycle grant is released (i.e. when both the drain reader and
 * all snapshot readers have finished).
 *
 * <p>Mutations (file list appends) are single-writer and intentionally unsynchronized; callers must
 * serialize them via the channel IO executor.
 */
@Internal
public final class FetchedChannelState implements Closeable {

    /** Ordered list of spill file paths, one entry per physical file. Sealed at construction. */
    private final List<Path> files;

    // close() and release() may be called from different threads; volatile ensures visibility.
    private volatile boolean closed = false;

    private final AtomicInteger refCount = new AtomicInteger(0);

    private final AtomicBoolean cleanedUp = new AtomicBoolean(false);

    /**
     * Wraps an already-written, ordered list of spill files. The list is sealed; it never grows.
     */
    FetchedChannelState(List<Path> files) {
        this.files = new ArrayList<>(checkNotNull(files));
    }

    // -------------------------------------------------------------------------------------------
    // Read-phase API (called by the reader after the writer is sealed)
    // -------------------------------------------------------------------------------------------

    /** Returns the ordered list of spill file paths. Read-only view. */
    public List<Path> files() {
        return Collections.unmodifiableList(files);
    }

    // -------------------------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------------------------

    /** Acquires a lifecycle grant for a reader or handoff owner. */
    public void acquire() {
        refCount.incrementAndGet();
    }

    /**
     * Releases a lifecycle grant. When the last grant is released (refCount reaches zero), all
     * spill files are deleted. This preserves the invariant that files exist for the lifetime of
     * all readers (drain + snapshot) and are cleaned up exactly once when the last reader finishes.
     */
    public void release() throws IOException {
        if (refCount.decrementAndGet() == 0) {
            if (cleanedUp.compareAndSet(false, true)) {
                closed = true;
                deleteAllFiles();
            }
        }
    }

    /** Forces cleanup even when lifecycle grants are still outstanding. */
    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        if (cleanedUp.compareAndSet(false, true)) {
            deleteAllFiles();
        }
    }

    private void deleteAllFiles() throws IOException {
        IOException firstError = null;
        for (Path file : files) {
            try {
                Files.deleteIfExists(file);
            } catch (IOException e) {
                if (firstError == null) {
                    firstError = e;
                } else {
                    firstError.addSuppressed(e);
                }
            }
        }
        if (firstError != null) {
            throw firstError;
        }
    }

    @VisibleForTesting
    public boolean isClosed() {
        return closed;
    }
}
