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
import org.apache.flink.runtime.io.network.partition.consumer.RecoverableInputChannel;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Drains a {@link FetchedChannelState} into the physical recovered channels and inserts {@link
 * RecoveryCheckpointBarrier}s when a checkpoint fires during recovery.
 *
 * <p>FLINK-38544 transitional in-memory implementation: the in-memory recovery backend already
 * pushed every recovered buffer into the physical channels' own queues at conversion time (via
 * {@code requestPartitions(true)}), so draining is just appending the end-of-recovered-state
 * sentinel to each channel, and there is never an undrained residue to snapshot — inserting the
 * barrier into the in-recovery channels is enough. The disk-based drainer of the spilling backend
 * replaces this, reading segments off spill files and returning a reader over the undrained slice.
 */
@Internal
public final class FetchedChannelStateDrainer implements RecoveryCheckpointTrigger, Closeable {

    private final FetchedChannelState channelState;

    private final List<RecoverableInputChannel> channels;

    public FetchedChannelStateDrainer(
            FetchedChannelState channelState, List<RecoverableInputChannel> channels) {
        this.channelState = checkNotNull(channelState);
        this.channels = checkNotNull(channels);
    }

    /**
     * Appends the end-of-recovered-state sentinel to every converted channel. Each channel first
     * waits for its upstream to be ready, so this must run on the channelIOExecutor rather than
     * block the mailbox thread. Only once the sentinel is in place can the consume path flip the
     * channel out of recovery, which guarantees live data is never polled before the upstream
     * connection exists.
     */
    public void drain() throws IOException, InterruptedException {
        channelState.release();
        for (RecoverableInputChannel channel : channels) {
            channel.finishRecoveredBufferDelivery();
        }
    }

    /**
     * Inserts a {@link RecoveryCheckpointBarrier} into every channel that is still in recovery, so
     * that {@code checkpointStarted}'s in-recovery branch can persist exactly the pre-barrier
     * recovered data. There is no snapshot side for the in-memory backend: everything a checkpoint
     * must persist is already inside the channels' queues, so the snapshot is inherently empty.
     */
    @Override
    public FetchedChannelStateReader snapshotAndInsertBarriers(long checkpointId)
            throws IOException {
        for (RecoverableInputChannel channel : channels) {
            channel.insertRecoveryCheckpointBarrierIfInRecovery(checkpointId);
        }
        // No snapshot side for the in-memory backend: everything a checkpoint must persist is
        // already inside the channels' queues, so the returned reader is empty.
        return FetchedChannelStateReader.emptyReader();
    }

    @Override
    public void close() throws IOException {
        channelState.close();
    }
}
