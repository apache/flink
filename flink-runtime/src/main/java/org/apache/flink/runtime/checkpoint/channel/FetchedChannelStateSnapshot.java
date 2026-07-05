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

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * An immutable resume point for a {@link FetchedChannelState} reader: where to start reading, plus
 * the metadata of a half-delivered segment when the boundary falls inside one. It holds one
 * lifecycle grant on the underlying {@link FetchedChannelState} (acquired in the constructor).
 *
 * <p>{@code channel} decides how the resume point is read. It is {@code null} when {@code
 * readOffset} points at a segment header, and non-null when it points into a segment body, in which
 * case {@code remaining} is that segment's not-yet-delivered byte count. The three cases that occur
 * in practice:
 *
 * <pre>
 * (0, 0,   null, 0)   nothing delivered yet — read everything from the first header
 * (2, 840, null, 0)   boundary landed exactly on a header — read that header, then continue
 * (2, 851, c3,   57)  boundary landed inside c3's body — hand out its last 57 bytes, no header read
 * </pre>
 *
 * <p>A snapshot is a one-shot, single-reader handle: exactly one {@link FetchedChannelStateReader}
 * may be opened from it via {@link #reader()}, and opening one transfers the grant to that reader,
 * which returns it on close. A second {@link #reader()} call fails loud.
 *
 * <p>Owners must {@link #close()} the snapshot. Closing releases the grant when no reader was
 * opened; once one was, the reader owns it and closing here is a no-op — so closing early never
 * deletes files out from under a live reader.
 */
@Internal
public final class FetchedChannelStateSnapshot implements AutoCloseable {

    private final FetchedChannelState channelState;

    /** Spill file to resume in. */
    private final int fileIndex;

    /** Byte offset within that file to resume at. */
    private final long readOffset;

    /** Channel of the half-delivered segment, or {@code null} if {@code readOffset} is a header. */
    @Nullable private final InputChannelInfo channel;

    /** Not-yet-delivered body bytes of that segment; 0 iff {@code channel} is {@code null}. */
    private final int remaining;

    /** True once {@link #reader()} has been called; prevents opening a second reader. */
    private boolean readerOpened;

    private boolean closed;

    /**
     * Creates a snapshot resuming at {@code readOffset} in file {@code fileIndex}. Acquires one
     * lifecycle grant on {@code channelState}; the grant is released when the reader returned by
     * {@link #reader()} is closed.
     */
    FetchedChannelStateSnapshot(
            FetchedChannelState channelState,
            int fileIndex,
            long readOffset,
            @Nullable InputChannelInfo channel,
            int remaining) {
        checkArgument(
                (channel == null) == (remaining == 0),
                "channel and remaining must be set together: %s / %s",
                channel,
                remaining);
        this.channelState = channelState;
        this.fileIndex = fileIndex;
        this.readOffset = readOffset;
        this.channel = channel;
        this.remaining = remaining;
        channelState.acquire();
    }

    /**
     * Opens the reader for this snapshot. May be called at most once; a second call fails loud to
     * enforce the 1:1 snapshot-to-reader invariant.
     *
     * @return a new reader starting from this snapshot's position; caller must close it when done
     */
    public FetchedChannelStateReader reader() {
        checkState(!closed, "Snapshot is closed");
        checkState(!readerOpened, "A reader has already been opened from this snapshot");
        readerOpened = true;
        return new FetchedChannelStateReaderImpl(this);
    }

    /**
     * Releases the lifecycle grant held by this snapshot. Called by the reader on close; must not
     * be called directly by any other party.
     */
    void release() throws IOException {
        channelState.release();
    }

    /**
     * Releases the grant if no reader was opened; otherwise the reader owns it and this is a no-op.
     * Idempotent.
     */
    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        if (!readerOpened) {
            channelState.release();
        }
    }

    /** Returns the underlying channel state (package-private; used by the reader). */
    FetchedChannelState channelState() {
        return channelState;
    }

    int fileIndex() {
        return fileIndex;
    }

    long readOffset() {
        return readOffset;
    }

    @Nullable
    InputChannelInfo channel() {
        return channel;
    }

    int remaining() {
        return remaining;
    }
}
