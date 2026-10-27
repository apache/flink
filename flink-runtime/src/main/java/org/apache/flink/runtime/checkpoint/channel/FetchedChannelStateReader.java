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
import java.util.Optional;

/**
 * Forward-only, strictly sequential reader over a {@link FetchedChannelState}'s spill files.
 * Deliberately not a Java {@link java.util.Iterator}: a body must be fully read before advancing,
 * body ownership is handed to the consumer, and consume/commit are separate steps.
 *
 * <p>The main reader (opened via {@link FetchedChannelState#reader()}, starting at offset 0)
 * records the delivered boundary — the "committed position" — via {@link SpillSegment#commit()};
 * before anything is committed it equals the reader's start position. Each checkpoint derives a
 * {@link #snapshot()} that resumes from that boundary. {@link #snapshot()} and {@link
 * SpillSegment#commit()} must be called under the drainer lock; disk reads happen outside it.
 */
@Internal
public interface FetchedChannelStateReader extends Closeable {

    /**
     * Advances to the next segment and returns it, or {@link Optional#empty()} when no segment
     * remains.
     *
     * <p>Entry rule (the first call is exempt): the previous segment's body must be fully read,
     * otherwise this is a contract violation and fails loud (no skip-ahead).
     */
    Optional<SpillSegment> advanceAndGetNextSegment();

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
     * One per-channel segment produced by {@link #advanceAndGetNextSegment()}.
     *
     * <p>The segment body bytes are opaque to the reader; record framing is handled by the
     * consumer's deserializer. A consumer reads {@link #bodyStream()} to EOF (after {@link
     * #length()} bytes), and the drain consumer additionally calls {@link #commit()}.
     *
     * <p>Ownership of {@link #bodyStream()} passes to the consumer: the reader no longer tracks how
     * far it has been read. The "previous body must be fully read" rule (no skip-ahead) is enforced
     * at the next {@link FetchedChannelStateReader#advanceAndGetNextSegment()} call, not here.
     *
     * <p>A segment is valid only until the next {@code advanceAndGetNextSegment()} call on the
     * parent reader.
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
         * {@link FetchedChannelStateReader#advanceAndGetNextSegment()}.
         */
        InputStream bodyStream();

        /**
         * Number of body bytes this segment hands out before EOF. For the snapshot path this is the
         * not-yet-delivered remainder used as the length prefix when writing to the checkpoint
         * stream. Bounded by the spill file size limit, so it always fits in an {@code int}.
         */
        int length();

        /**
         * Records the body bytes read from {@link #bodyStream()} so far as delivered.
         *
         * <p>Called once per delivered buffer by the drainer's drain loop, under the same lock as
         * {@link FetchedChannelStateReader#snapshot()}, so a snapshot always resumes on a buffer
         * boundary. Only the main reader commits.
         */
        void commit();
    }
}
