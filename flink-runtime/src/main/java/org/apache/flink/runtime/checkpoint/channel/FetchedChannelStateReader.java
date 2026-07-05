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

import java.io.Closeable;
import java.io.InputStream;
import java.util.Collections;
import java.util.Optional;

/**
 * Forward reader over a {@link FetchedChannelState}'s spill files. This is our own segment reader,
 * on purpose <em>not</em> a Java {@link java.util.Iterator}: our access pattern ("a body must be
 * fully read before the next segment", "body ownership is handed to the consumer", "consume and
 * commit are separate steps") does not fit the {@code hasNext/next} contract.
 *
 * <p>This interface is the contract callers depend on; {@link FetchedChannelStateReaderImpl} holds
 * the implementation (the live file stream, the two progress positions, the bounded body view).
 *
 * <p>Reading is strictly sequential: a reader is positioned once (offset 0 for the root reader, or
 * the committed position for a {@link #snapshot()}), then consumes forward only via {@link
 * #nextSegment()}. It never seeks backward and never re-positions mid-iteration.
 *
 * <p>The drain thread reads the root reader front to back and commits via {@link
 * SpillSegment#commit()}; each checkpoint derives a fresh {@link #snapshot()} that resumes from the
 * committed position. {@link #snapshot()} and {@link SpillSegment#commit()} must be called under
 * the drainer lock; disk reads happen outside it.
 */
@Internal
public interface FetchedChannelStateReader extends Closeable {

    /**
     * Advances to the next segment and returns it, or {@link Optional#empty()} when no segment
     * remains. Advancing and probing are one step; there is no separate {@code hasNext}.
     *
     * <p>Entry rule (the first call is exempt): the previous segment's body must be fully read,
     * otherwise this is a contract violation and fails loud (no skip-ahead).
     */
    Optional<SpillSegment> nextSegment();

    /**
     * Derives an independent resume point starting from the committed position. The snapshot holds
     * its own {@link FetchedChannelState} lifecycle grant; the caller must open a reader from it
     * via {@link FetchedChannelStateSnapshot#reader()} and close that reader when done.
     *
     * <p>Must be called under the drainer lock so that the copied position reflects the latest
     * committed state.
     *
     * @return a snapshot capturing the current committed position; caller must open and close a
     *     reader from it
     */
    FetchedChannelStateSnapshot snapshot();

    /**
     * Returns a reader with no segments — its first {@link #nextSegment()} is empty. Each call
     * hands out a fresh instance: readers have independent lifecycle and {@link #close()} is
     * single-use, so a shared instance would let one consumer's close break later consumers. Used
     * wherever there is nothing to snapshot (e.g. after drain finished, or the no-op recovery
     * trigger).
     */
    static FetchedChannelStateReader emptyReader() {
        return new FetchedChannelState(Collections.emptyList()).reader();
    }

    /**
     * One per-channel segment produced by {@link #nextSegment()}.
     *
     * <p>The segment body bytes are opaque to the reader; record framing is handled by the
     * consumer's deserializer. A consumer reads {@link #bodyStream()} to EOF (after {@link
     * #length()} bytes), and the drain consumer additionally calls {@link #commit()} under the
     * drainer lock after each delivery so that a later {@link FetchedChannelStateReader#snapshot()}
     * resumes from the delivered boundary.
     *
     * <p>Ownership of {@link #bodyStream()} passes to the consumer: the reader no longer tracks how
     * far it has been read. The "previous body must be fully read" rule (no skip-ahead) is enforced
     * at the next {@link FetchedChannelStateReader#nextSegment()} call, not here.
     *
     * <p>A segment is valid only until the next {@code nextSegment()} call on the parent reader.
     */
    interface SpillSegment {

        /** The channel whose data this segment contains. */
        InputChannelInfo channelInfo();

        /**
         * Returns an {@link InputStream} bounded to this segment's body. Reading returns {@code -1}
         * (EOF) after {@link #length()} bytes; it never reads into the next segment or the next
         * file.
         *
         * <p>The stream is single-use, not thread-safe, and must be fully consumed before the next
         * {@link FetchedChannelStateReader#nextSegment()}.
         */
        InputStream bodyStream();

        /**
         * Number of body bytes this segment hands out before EOF. For the snapshot path this is the
         * not-yet-delivered remainder used as the length prefix when writing to the checkpoint
         * stream. Bounded by the spill file size limit, so it always fits in an {@code int}.
         */
        int length();

        /**
         * Advances the reader's committed position to match how many body bytes have been read from
         * {@link #bodyStream()} so far. Must be called under the drainer lock after each buffer
         * delivery so that a subsequent {@link FetchedChannelStateReader#snapshot()} sees the
         * correct delivered boundary. Only the drain (root) reader commits; the snapshot reader
         * never does.
         */
        void commit();
    }
}
