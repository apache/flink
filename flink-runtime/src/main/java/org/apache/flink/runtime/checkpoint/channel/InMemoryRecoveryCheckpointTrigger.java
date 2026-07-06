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

import java.io.IOException;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link RecoveryCheckpointTrigger} for the in-memory recovery backend: inserts a {@link
 * RecoveryCheckpointBarrier} into every channel that is still in recovery, so that {@code
 * checkpointStarted}'s in-recovery branch can persist exactly the pre-barrier recovered data.
 *
 * <p>The snapshot side is inherently empty: the in-memory backend pushes all recovered buffers
 * into the physical channels in one shot at conversion time, so once this trigger is installed
 * there is never an undrained residue outside the channels' own queues — everything a checkpoint
 * must persist is already inside the channels, hence {@link
 * FetchedChannelStateReader#emptyReader()} is returned. The disk-based drainer of the spilling
 * backend replaces this class: draining incrementally, it does have an undrained residue to
 * snapshot and returns a reader over that slice.
 *
 * <p>FLINK-38544 transitional: removed when the spilling backend lands.
 */
@Internal
public final class InMemoryRecoveryCheckpointTrigger implements RecoveryCheckpointTrigger {

    private final List<RecoverableInputChannel> channels;

    public InMemoryRecoveryCheckpointTrigger(List<RecoverableInputChannel> channels) {
        this.channels = checkNotNull(channels);
    }

    @Override
    public FetchedChannelStateReader snapshotAndInsertBarriers(long checkpointId)
            throws IOException {
        for (RecoverableInputChannel channel : channels) {
            channel.insertRecoveryCheckpointBarrierIfInRecovery(checkpointId);
        }
        return FetchedChannelStateReader.emptyReader();
    }
}
