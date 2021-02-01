/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.io.checkpointing;

import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.FinalizeBarrier;
import org.apache.flink.runtime.io.network.partition.consumer.CheckpointableInput;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Responses for inserting barriers for channels that have received EndOfPartition but has not
 * processed yet. The inserting happens when a new checkpoint starts or FinalizeBarrier received
 * from one channel (which means this channel has received EndOfPartition).
 */
public class FinalizeBarrierComplementProcessor {

    private final CheckpointableInput[] inputs;

    /** The channels that have received EndOfPartition and their next barrier id. */
    private final Map<InputChannelInfo, Long> nextBarrierIds = new HashMap<>();

    /** The ongoing checkpoints. */
    private final TreeMap<Long, CheckpointBarrier> pendingCheckpoints = new TreeMap<>();

    /**
     * TODO: temporary flag to disable inserting barriers.
     *
     * <p>This flag is required since before the recovery process if modified, if some checkpoints
     * after tasks finished are done, after recovery the finished operators could not be skipped and
     * would cause data duplication. This flag would be remove together with flags at JM side after
     * all the PRs are merged.
     */
    private boolean allowComplementBarrier;

    public FinalizeBarrierComplementProcessor(CheckpointableInput... inputs) {
        this.inputs = inputs;
    }

    public void setAllowComplementBarrier(boolean allowComplementBarrier) {
        this.allowComplementBarrier = allowComplementBarrier;
    }

    public boolean isAllowComplementBarrier() {
        return allowComplementBarrier;
    }

    public void processFinalBarrier(
            FinalizeBarrier finalizeBarrier, InputChannelInfo inputChannelInfo) throws IOException {
        checkState(!nextBarrierIds.containsKey(inputChannelInfo));

        nextBarrierIds.put(inputChannelInfo, finalizeBarrier.getNextBarrierId());
        insertBarriers(inputChannelInfo);
    }

    public void onEndOfPartition(InputChannelInfo inputChannelInfo) {
        nextBarrierIds.remove(inputChannelInfo);
    }

    public void onCheckpointAlignmentStart(CheckpointBarrier receivedBarrier) throws IOException {
        onNewCheckpoint(receivedBarrier);
    }

    public void onCheckpointAlignmentEnd(long checkpointId) {
        pendingCheckpoints.headMap(checkpointId, true).clear();
    }

    public void onTriggeringCheckpoint(
            CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions)
            throws IOException {
        onNewCheckpoint(
                new CheckpointBarrier(
                        checkpointMetaData.getCheckpointId(),
                        checkpointMetaData.getTimestamp(),
                        checkpointOptions));
    }

    private void onNewCheckpoint(CheckpointBarrier receivedBarrier) throws IOException {
        // Checkpoints trigger via RPC would get notification again when received the first
        // inserted barrier, we should ignore the repeat notification.
        if (!pendingCheckpoints.containsKey(receivedBarrier.getId())) {
            pendingCheckpoints.put(receivedBarrier.getId(), receivedBarrier);
            for (InputChannelInfo inputChannelInfo : nextBarrierIds.keySet()) {
                insertBarriers(inputChannelInfo);
            }
        }
    }

    private void insertBarriers(InputChannelInfo inputChannelInfo) throws IOException {
        long nextBarrierId = nextBarrierIds.get(inputChannelInfo);

        NavigableMap<Long, CheckpointBarrier> barriersToInsert =
                pendingCheckpoints.tailMap(nextBarrierId, true);
        if (barriersToInsert.size() > 0) {
            for (Map.Entry<Long, CheckpointBarrier> entry : barriersToInsert.entrySet()) {
                if (allowComplementBarrier) {
                    inputs[inputChannelInfo.getGateIdx()].insertBarrierBeforeEndOfPartition(
                            inputChannelInfo.getInputChannelIdx(), entry.getValue());
                }
            }

            nextBarrierIds.put(inputChannelInfo, barriersToInsert.lastEntry().getKey() + 1);
        }
    }
}
