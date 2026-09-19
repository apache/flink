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
import org.apache.flink.util.IOUtils;

import javax.annotation.Nullable;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.runtime.checkpoint.channel.AbstractSpillingHandler.SEGMENT_HEADER_BYTES;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The single {@link FetchedChannelStateReader} implementation over a {@link FetchedChannelState}'s
 * spill files.
 *
 * <p>Reading is strictly sequential: the stream is positioned once when a file is opened and then
 * only moves forward. A snapshot that resumes inside a segment body starts right at the boundary —
 * the segment's channel and remaining length come from the snapshot itself, so no header is re-read
 * and no prefix is skipped.
 *
 * <p>The reader tracks two {@link Position}s of the same shape — the values a {@link
 * FetchedChannelStateSnapshot} is made of: {@code current}, the live read position, and {@code
 * committed}, the delivered boundary that {@link SpillSegment#commit()} publishes from it under the
 * drainer lock.
 *
 * <p>The "previous body fully read before advancing" rule is checked at the {@link
 * #advanceAndGetNextSegment()} entry. Body ownership is handed to the consumer, so the reader does
 * not track body progress except through {@code current}.
 */
@Internal
final class FetchedChannelStateReaderImpl implements FetchedChannelStateReader {

    private final FetchedChannelStateSnapshot snapshot;

    /**
     * The live read position: the file being read and the exact byte offset within it. It runs
     * ahead of the committed boundary by what the drainer has read from disk but not yet handed to
     * a channel (at most one recovery buffer) — reads happen outside the drainer lock, delivery and
     * commit inside it. A checkpoint therefore cuts at the committed boundary: what is already in
     * the channel queue is persisted from the channel side, everything after it is re-read from the
     * spill files.
     */
    private final Position current;

    private final Position committed;

    /** Open stream over {@code current.fileIndex}, or {@code null} before the first read. */
    @Nullable private InputStream currentFileStream;

    /** Size of the file currently open. */
    private long currentFileSize;

    /**
     * The segment handed out by the last {@link #advanceAndGetNextSegment()}. A segment is only
     * valid until the next one is handed out; reading or committing an older one fails loud.
     */
    @Nullable private Segment currentSegment;

    private boolean positioned;
    private boolean closed;

    FetchedChannelStateReaderImpl(FetchedChannelStateSnapshot snapshot) {
        this.snapshot = snapshot;
        this.current =
                new Position(
                        snapshot.fileIndex(),
                        snapshot.readOffset(),
                        snapshot.channel(),
                        snapshot.remaining());
        this.committed = current.copy();
    }

    @Override
    public Optional<SpillSegment> advanceAndGetNextSegment() {
        checkState(!closed, "FetchedChannelStateReader is closed");
        checkState(
                !positioned || current.remaining == 0,
                "Previous segment body not fully consumed before advancing: %s bytes left",
                current.remaining);
        try {
            if (!positioned) {
                positioned = true;
                if (current.channel != null) {
                    return resumedSegment();
                }
            }
            return nextSegmentAtHeader();
        } catch (IOException e) {
            throw new RuntimeException("Failed to read segment", e);
        }
    }

    /**
     * Resume path, taken once by a snapshot reader whose start offset sits inside a body: the
     * channel and the remaining length come from the snapshot, so nothing is re-read or skipped.
     */
    private Optional<SpillSegment> resumedSegment() throws IOException {
        openFileAndSeek();
        currentSegment =
                new Segment(
                        this,
                        current.channel,
                        new BoundedSegmentStream(currentFileStream, current, current.remaining));
        return Optional.of(currentSegment);
    }

    /** Steady path: the stream sits on a segment header; read it and hand out the whole body. */
    private Optional<SpillSegment> nextSegmentAtHeader() throws IOException {
        if (!openCurrentFile()) {
            return Optional.empty();
        }
        SegmentHeader header = readHeaderAtCurrent();
        current.startSegment(header.channelInfo, header.bufferLength);
        if (currentSegment != null) {
            currentSegment.body.invalidate();
        }
        currentSegment =
                new Segment(
                        this,
                        header.channelInfo,
                        new BoundedSegmentStream(currentFileStream, current, header.bufferLength));
        return Optional.of(currentSegment);
    }

    @Override
    public FetchedChannelStateSnapshot snapshot() {
        checkState(!closed, "FetchedChannelStateReader is closed");
        return new FetchedChannelStateSnapshot(
                snapshot.channelState(),
                committed.fileIndex,
                committed.readOffset,
                committed.channel,
                committed.remaining);
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

    /** The spill files in write order; the reader never mutates the list. */
    private List<Path> files() {
        return snapshot.channelState().files();
    }

    /**
     * Ensures a file is open with the stream positioned at {@code current}'s read offset, ready to
     * read this segment's header. Rolls to the next file when the current one is exhausted. Returns
     * false when no segment remains.
     */
    private boolean openCurrentFile() throws IOException {
        boolean rolled = false;
        while (current.fileIndex < files().size()) {
            openFileAndSeek();
            if (current.readOffset < currentFileSize) {
                return true;
            }
            // Current file fully read: move to the next file's first segment. The writer never
            // produces an empty file, so one roll always lands on data.
            checkState(!rolled, "Rolled past more than one empty file");
            closeFileStream();
            current.rollToNextFile();
            rolled = true;
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
        checkState(
                gateIdx >= 0 && channelIdx >= 0,
                "negative channel info in segment header: %s/%s",
                gateIdx,
                channelIdx);
        return new SegmentHeader(new InputChannelInfo(gateIdx, channelIdx), bufferLength);
    }

    /**
     * Ensures the file at {@code current.fileIndex} is open with the stream positioned at {@code
     * current.readOffset}. If a stream is already open it is left as-is: sequential reading
     * guarantees it is already there.
     */
    private void openFileAndSeek() throws IOException {
        if (currentFileStream != null) {
            return;
        }
        SeekableByteChannel channel =
                Files.newByteChannel(files().get(current.fileIndex), StandardOpenOption.READ);
        try {
            currentFileSize = channel.size();
            channel.position(current.readOffset);
        } catch (IOException e) {
            channel.close();
            throw e;
        }
        currentFileStream = new BufferedInputStream(Channels.newInputStream(channel));
    }

    private void readFully(byte[] buf) throws IOException {
        IOUtils.readFully(currentFileStream, buf, 0, buf.length);
        current.advanceReadOffset(buf.length);
    }

    private void closeFileStream() throws IOException {
        if (currentFileStream != null) {
            currentFileStream.close();
            currentFileStream = null;
        }
    }

    // -------------------------------------------------------------------------------------------
    // Position: where the open stream sits
    // -------------------------------------------------------------------------------------------

    /**
     * A point in the spill files, in the four values a {@link FetchedChannelStateSnapshot} is made
     * of: {@code channel} is null exactly when {@code readOffset} sits on a segment header, and
     * non-null while a body is in flight, {@code remaining} being what is left of it.
     */
    static final class Position {
        private int fileIndex;
        private long readOffset;
        @Nullable private InputChannelInfo channel;
        private int remaining;

        Position(
                int fileIndex, long readOffset, @Nullable InputChannelInfo channel, int remaining) {
            this.fileIndex = fileIndex;
            this.readOffset = readOffset;
            this.channel = channel;
            this.remaining = remaining;
        }

        Position copy() {
            return new Position(fileIndex, readOffset, channel, remaining);
        }

        /** Publishes {@code other} into this position (used by commit). */
        void copyFrom(Position other) {
            fileIndex = other.fileIndex;
            readOffset = other.readOffset;
            channel = other.channel;
            remaining = other.remaining;
        }

        /** Advances past the {@code delta} header bytes just read; no body is in flight. */
        void advanceReadOffset(long delta) {
            readOffset += delta;
        }

        /** Enters the body of a freshly read segment header. */
        void startSegment(InputChannelInfo segmentChannel, int bodyLength) {
            channel = segmentChannel;
            remaining = bodyLength;
        }

        /** Accounts for {@code n} body bytes handed to the consumer. */
        void advanceBody(int n) {
            readOffset += n;
            remaining -= n;
            if (remaining == 0) {
                channel = null;
            }
        }

        /** Rolls to the start of the next file once the current one is exhausted. */
        void rollToNextFile() {
            fileIndex++;
            readOffset = 0L;
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
     * <p>Only the main reader commits.
     */
    private static final class Segment implements SpillSegment {
        private final FetchedChannelStateReaderImpl reader;
        private final InputChannelInfo channelInfo;
        private final BoundedSegmentStream body;

        private Segment(
                FetchedChannelStateReaderImpl reader,
                InputChannelInfo channelInfo,
                BoundedSegmentStream body) {
            this.reader = reader;
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
            checkState(
                    reader.currentSegment == this,
                    "Committing a segment that is no longer the current one");
            reader.committed.copyFrom(reader.current);
        }
    }

    /**
     * A forward-only, bounded view over the body bytes this reader still has to hand out for one
     * segment. The bound is {@code current.remaining}, so the view keeps no counter of its own; it
     * reaches EOF there and never reads into the next segment or file. If the file ends first, an
     * {@link EOFException} is thrown (fail-loud). Closing this view does not close the underlying
     * file; the reader owns it.
     */
    private static final class BoundedSegmentStream extends InputStream {
        private final InputStream fileStream;
        private final Position current;
        private final int length;

        /** Set when the reader hands out the next segment; this view must not be read after. */
        private boolean stale;

        private BoundedSegmentStream(InputStream fileStream, Position current, int length) {
            this.fileStream = fileStream;
            this.current = current;
            this.length = length;
        }

        private void invalidate() {
            stale = true;
        }

        /** Number of body bytes this view will hand out. */
        int deliverableLength() {
            return length;
        }

        @Override
        public int read() throws IOException {
            byte[] one = new byte[1];
            int n = read(one, 0, 1);
            return n < 0 ? -1 : (one[0] & 0xFF);
        }

        @Override
        public int read(byte[] buf, int off, int len) throws IOException {
            checkState(!stale, "Reading a segment that is no longer the current one");
            if (current.remaining == 0) {
                return -1;
            }
            int toRead = Math.min(len, current.remaining);
            int n = fileStream.read(buf, off, toRead);
            if (n > 0) {
                current.advanceBody(n);
            }
            if (n < 0) {
                throw new EOFException(
                        "Unexpected EOF in segment body after "
                                + (length - current.remaining)
                                + "/"
                                + length
                                + " bytes");
            }
            return n;
        }

        @Override
        public void close() {
            // Do not close the underlying file; it is owned by the reader.
        }
    }
}
