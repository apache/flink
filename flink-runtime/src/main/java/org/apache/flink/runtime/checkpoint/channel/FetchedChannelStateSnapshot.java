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

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * An immutable resume point for a {@link FetchedChannelState} reader. It captures the {@link
 * FetchedChannelStateReaderImpl.Position} at which reading should start and holds one lifecycle
 * grant on the underlying {@link FetchedChannelState} (acquired in the constructor).
 *
 * <p>A snapshot is a one-shot, single-reader handle:
 *
 * <ul>
 *   <li>Exactly one {@link FetchedChannelStateReader} may be opened from it via {@link #reader()}.
 *   <li>When that reader is closed, it releases the lifecycle grant via {@link #release()}.
 * </ul>
 *
 * <p>The 1:1 reader constraint is enforced fail-loud: calling {@link #reader()} a second time
 * throws {@link IllegalStateException} immediately.
 */
final class FetchedChannelStateSnapshot {

    private final FetchedChannelState channelState;
    private final FetchedChannelStateReaderImpl.Position position;

    /** True once {@link #reader()} has been called; prevents opening a second reader. */
    private boolean readerOpened;

    /**
     * Creates a snapshot at {@code position} within {@code channelState}. Acquires one lifecycle
     * grant on {@code channelState}; the grant is released when the reader returned by {@link
     * #reader()} is closed.
     */
    FetchedChannelStateSnapshot(
            FetchedChannelState channelState, FetchedChannelStateReaderImpl.Position position) {
        this.channelState = channelState;
        this.position = position;
        channelState.acquire();
    }

    /**
     * Opens the reader for this snapshot. May be called at most once; a second call fails loud to
     * enforce the 1:1 snapshot-to-reader invariant.
     *
     * @return a new reader starting from this snapshot's position; caller must close it when done
     */
    FetchedChannelStateReader reader() {
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

    /** Returns the underlying channel state (package-private; used by the reader). */
    FetchedChannelState channelState() {
        return channelState;
    }

    /**
     * Returns the start position (package-private; used by the reader, which copies it
     * immediately).
     */
    FetchedChannelStateReaderImpl.Position position() {
        return position;
    }
}
