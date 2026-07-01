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
import org.apache.flink.runtime.checkpoint.channel.FetchedChannelStateReader.SpillSegment;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.runtime.checkpoint.channel.AbstractSpillingHandler.SEGMENT_HEADER_BYTES;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The single {@link FetchedChannelStateReader} implementation over a {@link FetchedChannelState}'s
 * spill files.
 *
 * <p>Reading is strictly sequential and never seeks mid-iteration. There is exactly one place that
 * skips bytes: the very first {@link #nextSegment()} call, where a snapshot reader started mid-body
 * discards the already-delivered prefix to land on the not-yet-delivered remainder. Every later
 * call does no skipping at all — the previous body was read to its end, so the stream already sits
 * on the next segment's header. This "skip only on first positioning" rule is what keeps the
 * steady-state path free of any seek/skip.
 *
 * <p>The reader holds <b>two</b> {@link Position}s and nothing else duplicates them:
 *
 * <ul>
 *   <li>{@code current} — the live read position; its {@code readOffset} is exactly where the open
 *       file stream sits, advancing as the header and the consumer's body reads consume bytes (the
 *       latter outside the drainer lock).
 *   <li>{@code committed} — the delivered boundary; {@link SpillSegment#commit()} advances it from
 *       {@code current} (under the drainer lock). {@link #snapshot()} derives a new reader from it.
 * </ul>
 *
 * <p>The "previous body fully read before advancing" rule is checked at the {@link #nextSegment()}
 * entry (the first call is exempt — there is no previous segment). Body ownership is handed to the
 * consumer, so the reader does not track body progress except through {@code current}.
 */
@Internal
final class FetchedChannelStateReaderImpl implements FetchedChannelStateReader {

    private final FetchedChannelStateSnapshot snapshot;
    private final FetchedChannelState channelState;
    private final List<Path> files;

    /** Live read position; {@code readOffset} is where the open stream physically sits. */
    private final Position current;

    /** Delivered boundary; {@link SpillSegment#commit()} advances it from {@link #current}. */
    private final Position committed;

    /** Open stream over {@code current.fileIndex}, or {@code null} before the first read. */
    @Nullable private InputStream fileStream;

    /** Size of the file currently open. */
    private long currentFileSize;

    /** Body view of the segment returned by the last {@link #nextSegment()}, or {@code null}. */
    @Nullable private BoundedSegmentStream currentBody;

    private boolean positioned;
    private boolean closed;

    FetchedChannelStateReaderImpl(FetchedChannelStateSnapshot snapshot) {
        this.snapshot = snapshot;
        this.channelState = snapshot.channelState();
        this.files = channelState.files();
        // Must copy the position so that this reader's commits do not mutate the snapshot's state.
        this.committed = snapshot.position().copy();
        this.current = committed.copy();
    }

    @Override
    public Optional<SpillSegment> nextSegment() {
        checkState(!closed, "FetchedChannelStateReader is closed");
        checkState(
                currentBody == null || currentBody.remaining() == 0,
                "Previous segment body not fully consumed before advancing: %s bytes left",
                currentBody == null ? 0 : currentBody.remaining());
        try {
            if (!positioned) {
                positioned = true;
                return firstSegment();
            }
            return followingSegment();
        } catch (IOException e) {
            throw new RuntimeException("Failed to read segment", e);
        }
    }

    /**
     * First positioning — the only path that may skip bytes. Opens the file at the committed header
     * offset and reads the header. A snapshot may resume in the middle of a segment: the committed
     * {@code readOffset} says how many body bytes were already delivered, and that prefix is
     * skipped so the returned body starts at the not-yet-delivered remainder. If the segment was
     * already fully delivered (prefix == whole body), it is exhausted here and we move on to the
     * next one.
     */
    private Optional<SpillSegment> firstSegment() throws IOException {
        // The committed read offset may sit mid-body (after a partial commit), but the header lives
        // at segmentStartOffset. Capture how much was already delivered, then rewind the live read
        // offset to the header so we open the file there and read the header, not mid-body.
        int deliveredPrefix = (int) current.deliveredBodyBytes();
        current.rewindToSegmentStart();

        if (!openCurrentFile()) {
            return Optional.empty();
        }
        SegmentHeader header = readHeaderAtCurrent();
        checkState(
                deliveredPrefix <= header.bufferLength,
                "Delivered offset %s exceeds segment length %s",
                deliveredPrefix,
                header.bufferLength);

        if (deliveredPrefix == header.bufferLength) {
            // This segment was already fully delivered before the snapshot; nothing remains in it.
            // Skip its whole body to reach the next segment's header, then take the steady path.
            skipBody(header.bufferLength);
            return followingSegment();
        }

        // Discard the already-delivered prefix (the one and only skip in this class), then hand out
        // the remainder. alreadyDelivered is carried so commit() records the boundary from the
        // head.
        skipBody(deliveredPrefix);
        currentBody =
                new BoundedSegmentStream(header.bufferLength - deliveredPrefix, deliveredPrefix);
        return Optional.of(new Segment(header.channelInfo, currentBody));
    }

    /**
     * Steady-state path — no skipping. The previous body was read to its end, so the stream sits
     * exactly on this segment's header (or at the current file's end, in which case we roll to the
     * next file). Reads the header and returns the whole-body view.
     */
    private Optional<SpillSegment> followingSegment() throws IOException {
        if (!openCurrentFile()) {
            return Optional.empty();
        }
        SegmentHeader header = readHeaderAtCurrent();
        currentBody = new BoundedSegmentStream(header.bufferLength);
        return Optional.of(new Segment(header.channelInfo, currentBody));
    }

    @Override
    public FetchedChannelStateSnapshot snapshot() {
        checkState(!closed, "FetchedChannelStateReader is closed");
        return new FetchedChannelStateSnapshot(channelState, committed.copy());
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        try {
            closeFileStream();
        } finally {
            snapshot.release();
        }
    }

    // -------------------------------------------------------------------------------------------
    // Sequential IO over the spill files; all of it advances current.readOffset / current.fileIndex
    // -------------------------------------------------------------------------------------------

    /**
     * Ensures a file is open with the stream positioned at {@code current}'s read offset, ready to
     * read this segment's header. Rolls to the next file when the current one is exhausted. Returns
     * false when no segment remains.
     *
     * <p>{@code current.segmentStartOffset} is set to where the header begins, so a later {@link
     * SpillSegment#commit()} records the right segment for the snapshot to resume from.
     */
    private boolean openCurrentFile() throws IOException {
        if (current.fileIndex >= files.size()) {
            return false;
        }
        openFileAndSeek();
        if (current.readOffset < currentFileSize) {
            current.startSegmentHere();
            return true;
        }
        // Current file fully read: move to the next file's first segment.
        closeFileStream();
        current.rollToNextFile();
        if (current.fileIndex >= files.size()) {
            return false;
        }
        openFileAndSeek();
        if (current.readOffset < currentFileSize) {
            current.startSegmentHere();
            return true;
        }
        return false;
    }

    /** Reads the 12-byte header at the current read offset; advances past it. */
    private SegmentHeader readHeaderAtCurrent() throws IOException {
        byte[] headerBytes = new byte[SEGMENT_HEADER_BYTES];
        readFully(headerBytes);
        DataInputStream h = new DataInputStream(new ByteArrayInputStream(headerBytes));
        int gateIdx = h.readInt();
        int channelIdx = h.readInt();
        int bufferLength = h.readInt();
        checkState(bufferLength >= 0, "negative segment length: %s", bufferLength);
        return new SegmentHeader(new InputChannelInfo(gateIdx, channelIdx), bufferLength);
    }

    /**
     * Ensures the file at {@code current.fileIndex} is open with the stream positioned at {@code
     * current.readOffset}. If a stream is already open it is left as-is: sequential reading
     * guarantees it is already there.
     */
    private void openFileAndSeek() throws IOException {
        if (fileStream != null) {
            return;
        }
        Path path = files.get(current.fileIndex);
        currentFileSize = Files.size(path);
        InputStream in = Files.newInputStream(path);
        try {
            skipOnStream(in, current.readOffset, path);
        } catch (IOException e) {
            in.close();
            throw e;
        }
        fileStream = in;
    }

    /** Skips {@code count} body bytes on the open stream, advancing the read offset. */
    private void skipBody(long count) throws IOException {
        if (count > 0) {
            skipOnStream(fileStream, count, files.get(current.fileIndex));
            current.advanceReadOffset(count);
        }
    }

    /** Skips exactly {@code count} bytes on {@code in}, failing loud if the file ends early. */
    private void skipOnStream(InputStream in, long count, Path path) throws IOException {
        long skipped = 0;
        while (skipped < count) {
            long s = in.skip(count - skipped);
            if (s <= 0) {
                // skip can return 0 near EOF; read-and-discard as a fallback.
                if (in.read() < 0) {
                    throw new EOFException(
                            "Cannot position to offset " + count + " in spill file " + path);
                }
                skipped++;
            } else {
                skipped += s;
            }
        }
    }

    private void readFully(byte[] buf) throws IOException {
        int read = 0;
        while (read < buf.length) {
            int n = fileStream.read(buf, read, buf.length - read);
            if (n < 0) {
                throw new EOFException(
                        "Truncated segment header in file "
                                + files.get(current.fileIndex)
                                + " at offset "
                                + current.readOffset
                                + ": expected "
                                + buf.length
                                + " bytes, got "
                                + read);
            }
            read += n;
            current.advanceReadOffset(n);
        }
    }

    /** Reads up to {@code len} body bytes from the current file; called only by the body view. */
    private int readBody(byte[] buf, int off, int len) throws IOException {
        int n = fileStream.read(buf, off, len);
        if (n > 0) {
            current.advanceReadOffset(n);
        }
        return n;
    }

    private void closeFileStream() throws IOException {
        if (fileStream != null) {
            fileStream.close();
            fileStream = null;
        }
    }

    // -------------------------------------------------------------------------------------------
    // Position: the three progress values locating where a snapshot resumes
    // -------------------------------------------------------------------------------------------

    /**
     * One progress point, expressed with the three values that fully locate where a snapshot
     * resumes. The reader holds two of these: {@code current} (live read position) and {@code
     * committed} (delivered boundary). They are the same shape but different in meaning; neither
     * keeps a shadow copy of the other.
     *
     * <ul>
     *   <li>{@code fileIndex} — file holding the current segment.
     *   <li>{@code segmentStartOffset} — byte offset of the current segment's header. A snapshot
     *       reads the segment metadata (channel, full body length) from here, because the segment
     *       may already be partially delivered yet the snapshot still needs the whole-segment
     *       header.
     *   <li>{@code readOffset} — for {@code current}, exactly where the open stream sits (the live
     *       read offset); for {@code committed}, the delivered boundary. For a brand-new segment
     *       {@code readOffset == segmentStartOffset} (header not yet passed); after the header and
     *       n body bytes it is {@code segmentStartOffset + SEGMENT_HEADER_BYTES + n}.
     * </ul>
     */
    static final class Position {
        private int fileIndex;
        private long segmentStartOffset;
        private long readOffset;

        Position(int fileIndex, long segmentStartOffset, long readOffset) {
            this.fileIndex = fileIndex;
            this.segmentStartOffset = segmentStartOffset;
            this.readOffset = readOffset;
        }

        static Position atStart() {
            return new Position(0, 0L, 0L);
        }

        Position copy() {
            return new Position(fileIndex, segmentStartOffset, readOffset);
        }

        /**
         * Already-delivered body bytes of the current segment, clamped to 0 before the header is
         * crossed (where {@code readOffset <= segmentStartOffset}). Only the first-positioning path
         * uses it, to size the prefix a snapshot must discard.
         */
        long deliveredBodyBytes() {
            return Math.max(0L, readOffset - segmentStartOffset - SEGMENT_HEADER_BYTES);
        }

        /** Advances the live read offset by {@code delta} bytes just read/skipped. */
        void advanceReadOffset(long delta) {
            readOffset += delta;
        }

        /**
         * Rewinds the live read offset back to the current segment's header. Used only on first
         * positioning: a snapshot's committed offset may sit mid-body, but the header must be read
         * first, so the read offset returns to {@code segmentStartOffset} before opening the file.
         */
        void rewindToSegmentStart() {
            readOffset = segmentStartOffset;
        }

        /**
         * Marks the current read offset as the start of the segment about to be read (its header
         * begins here). Called right before reading a header.
         */
        void startSegmentHere() {
            segmentStartOffset = readOffset;
        }

        /** Rolls to the start of the next file once the current one is exhausted. */
        void rollToNextFile() {
            fileIndex++;
            segmentStartOffset = 0L;
            readOffset = 0L;
        }

        /**
         * Copies this position into {@code target} but pins {@code target}'s read offset to {@code
         * deliveredBody} body bytes of the current segment — i.e. {@code commit} records "delivered
         * up to here", not "physically read up to here".
         */
        void copyAsDelivered(Position target, long deliveredBody) {
            target.fileIndex = fileIndex;
            target.segmentStartOffset = segmentStartOffset;
            target.readOffset = segmentStartOffset + SEGMENT_HEADER_BYTES + deliveredBody;
        }
    }

    /** Parsed segment header: channel and full body length. */
    private static final class SegmentHeader {
        private final InputChannelInfo channelInfo;
        private final int bufferLength;

        private SegmentHeader(InputChannelInfo channelInfo, int bufferLength) {
            this.channelInfo = channelInfo;
            this.bufferLength = bufferLength;
        }
    }

    /**
     * The single {@link SpillSegment} implementation. Exposes one segment's channel, body, and
     * length; {@link #commit()} advances the reader's {@code committed} position to however many
     * body bytes have been read. Reading the body and committing are separate steps so the consumer
     * can read outside the drainer lock and commit inside it.
     *
     * <p>Only the root (drain) reader commits.
     */
    private final class Segment implements SpillSegment {
        private final InputChannelInfo channelInfo;
        private final BoundedSegmentStream body;

        private Segment(InputChannelInfo channelInfo, BoundedSegmentStream body) {
            this.channelInfo = channelInfo;
            this.body = body;
        }

        @Override
        public InputChannelInfo channelInfo() {
            return channelInfo;
        }

        @Override
        public InputStream bodyStream() {
            return body;
        }

        @Override
        public int length() {
            return body.deliverableLength();
        }

        @Override
        public void commit() {
            current.copyAsDelivered(committed, body.deliveredFromSegmentHead());
        }
    }

    /**
     * A forward-only, bounded view over one segment's not-yet-delivered body remainder. It hands
     * out {@code remainingLength} bytes and reaches EOF after them; it never reads into the next
     * segment or file. The underlying stream is already positioned at the first byte this view
     * hands out (the prefix, if any, was skipped before construction). If the file ends before the
     * segment end, an {@link EOFException} is thrown (fail-loud). Closing this view does not close
     * the underlying file; the reader owns it.
     */
    private final class BoundedSegmentStream extends InputStream {

        /** Body bytes already delivered before this view (the skipped prefix); 0 for the root. */
        private final int alreadyDelivered;

        private final int remainingLength;
        private int read;

        private BoundedSegmentStream(int remainingLength) {
            this(remainingLength, 0);
        }

        private BoundedSegmentStream(int remainingLength, int alreadyDelivered) {
            this.remainingLength = remainingLength;
            this.alreadyDelivered = alreadyDelivered;
        }

        /** Body bytes not yet handed out (between the read position and the segment end). */
        int remaining() {
            return remainingLength - read;
        }

        /** Number of body bytes this view will hand out: the not-yet-delivered remainder. */
        int deliverableLength() {
            return remainingLength;
        }

        /**
         * Total delivered body bytes measured from the segment head: the skipped prefix plus what
         * has been read through this view. This is what {@link Segment#commit()} records.
         */
        int deliveredFromSegmentHead() {
            return alreadyDelivered + read;
        }

        @Override
        public int read() throws IOException {
            byte[] one = new byte[1];
            int n = read(one, 0, 1);
            return n < 0 ? -1 : (one[0] & 0xFF);
        }

        @Override
        public int read(byte[] buf, int off, int len) throws IOException {
            if (read >= remainingLength) {
                return -1;
            }
            int toRead = Math.min(len, remainingLength - read);
            int n = readBody(buf, off, toRead);
            if (n < 0) {
                throw new EOFException(
                        "Unexpected EOF in segment body after "
                                + read
                                + "/"
                                + remainingLength
                                + " bytes");
            }
            read += n;
            return n;
        }

        @Override
        public void close() {
            // Do not close the underlying file; it is owned by the reader.
        }
    }
}
