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
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.RecoverableInputChannel;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Drains a {@link FetchedChannelState} into recovered-buffer queues and snapshots remaining
 * segments when a checkpoint fires during recovery.
 *
 * <p>The drainer lock pairs channel delivery with reader-cursor advancement and also protects
 * snapshot creation plus barrier insertion. Disk reads and buffer allocation stay outside that
 * lock.
 */
@Internal
public final class FetchedChannelStateDrainer implements RecoveryCheckpointTrigger, Closeable {

    private final FetchedChannelStateReader rootReader;

    private final ResolvedChannels channels;

    private final Object lock = new Object();
    private final FetchedChannelState channelState;

    /**
     * Set under {@link #lock} once {@link #drain()} has consumed every segment. After that the
     * {@link #rootReader} is closed by {@link #close()}, so a later {@link
     * #snapshotAndInsertBarriers} must not derive from it; it returns an empty reader instead.
     * Guarded by the lock so the check is atomic with barrier insertion.
     */
    private boolean drainFinished;

    public FetchedChannelStateDrainer(
            FetchedChannelState channelState, List<RecoverableInputChannel> channels) {
        this.channelState = channelState;
        this.rootReader = checkNotNull(channelState).reader();
        this.channels = new ResolvedChannels(channels);
    }

    private static final class ResolvedChannels {
        final List<RecoverableInputChannel> allChannels;
        final Map<InputChannelInfo, RecoverableInputChannel> channelByInfo;

        ResolvedChannels(List<RecoverableInputChannel> all) {
            this.allChannels = all;
            Map<InputChannelInfo, RecoverableInputChannel> byInfo = new HashMap<>();
            for (RecoverableInputChannel ch : all) {
                byInfo.put(ch.getChannelInfo(), ch);
            }
            this.channelByInfo = byInfo;
        }
    }

    /**
     * Drains all segments from the spill file into the corresponding recovery buffer queues. Each
     * segment is split into chunks of at most {@code memorySegmentSize} bytes; a full chunk is
     * delivered under the drainer lock paired with a segment commit. After all segments are
     * drained, every channel's {@link RecoverableInputChannel#finishRecoveredBufferDelivery()} is
     * called.
     *
     * <p>Disk reads and buffer allocations happen outside the lock; only the "deliver + commit"
     * pair is locked to guarantee atomicity with snapshot.
     */
    public void drain() throws IOException, InterruptedException {
        channelState.release();
        Optional<SpillSegment> next;
        while ((next = rootReader.nextSegment()).isPresent()) {
            SpillSegment seg = next.get();
            RecoverableInputChannel ch = channels.channelByInfo.get(seg.channelInfo());
            if (ch == null) {
                throw new IllegalStateException(
                        "Drain: no physical channel found for " + seg.channelInfo());
            }
            drainSegment(seg, ch);
        }

        // Mark drain done before rootReader is closed, so a concurrent snapshot returns empty
        // rather than deriving from the soon-to-be-closed rootReader. Under the lock to stay atomic
        // with snapshotAndInsertBarriers' check.
        synchronized (lock) {
            drainFinished = true;
        }
        for (RecoverableInputChannel ch : channels.allChannels) {
            ch.finishRecoveredBufferDelivery();
        }
    }

    /**
     * Drains one segment into the given channel. Fills buffers from the segment's opaque byte
     * stream in chunks of at most {@code memorySegmentSize} bytes. A full buffer is delivered under
     * the lock and a fresh one is requested; a partial tail buffer (if non-empty) is also
     * delivered.
     */
    private void drainSegment(SpillSegment seg, RecoverableInputChannel ch)
            throws IOException, InterruptedException {
        InputStream in = seg.bodyStream();
        Buffer buf = ch.requestRecoveryBufferBlocking();
        int cap = buf.getMaxCapacity();

        while (fill(buf, in, cap - buf.getSize()) > 0) {
            if (buf.getSize() == cap) {
                // Buffer is full: deliver under lock and request a fresh one.
                synchronized (lock) {
                    ch.onRecoveredStateBuffer(buf);
                    seg.commit();
                }
                buf = ch.requestRecoveryBufferBlocking();
                cap = buf.getMaxCapacity();
            }
            // If buf is not full yet, the fill returned > 0 bytes but segment is not exhausted;
            // loop and keep filling the same buffer.
        }

        if (buf.getSize() > 0) {
            // Deliver the partial tail buffer.
            synchronized (lock) {
                ch.onRecoveredStateBuffer(buf);
                seg.commit();
            }
        } else {
            buf.recycleBuffer();
        }
    }

    /**
     * Fills up to {@code remaining} bytes from {@code in} into {@code buf}. Returns the number of
     * bytes actually written; returns 0 if the stream is at EOF. Does not close or recycle {@code
     * buf}; ownership stays with the caller.
     */
    private static int fill(Buffer buf, InputStream in, int remaining) throws IOException {
        if (remaining == 0) {
            return 0;
        }
        // Do not use try-with-resources: ChannelStateByteBuffer.close() recycles the buffer,
        // but the buffer is still owned by the caller here.
        ChannelStateByteBuffer view = ChannelStateByteBuffer.wrap(buf);
        return view.writeBytes(in, remaining);
    }

    /**
     * Atomically snapshots the undrained portion of the spill and inserts {@link
     * RecoveryCheckpointBarrier}s into all in-recovery channels. Returns an independent reader over
     * the remaining segments for replay into the checkpoint stream; the caller owns and must close
     * it.
     *
     * <p>If the drain has already finished, the root reader is closed and there is nothing left to
     * snapshot; an empty reader is returned so the caller's normal flow handles it uniformly.
     */
    @Override
    public FetchedChannelStateReader snapshotAndInsertBarriers(long checkpointId)
            throws IOException {

        // Barrier insertion and snapshot must occur within the same critical section so that the
        // snapshot's committed position reflects exactly the drain position at the moment barriers
        // were inserted, with no window for the drain thread to advance between.
        synchronized (lock) {
            for (RecoverableInputChannel ch : channels.allChannels) {
                ch.insertRecoveryCheckpointBarrierIfInRecovery(checkpointId);
            }
            if (drainFinished) {
                // Drain consumed everything and rootReader is (being) closed; nothing left to
                // snapshot. Return an empty reader so the caller's normal flow handles it.
                return FetchedChannelStateReader.emptyReader();
            }
            return rootReader.snapshot().reader();
        }
    }

    @Override
    public void close() throws IOException {
        rootReader.close();
    }
}
