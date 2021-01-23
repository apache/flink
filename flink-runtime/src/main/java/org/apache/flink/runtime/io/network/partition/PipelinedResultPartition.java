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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.util.function.SupplierWithException;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A result output of a task, pipelined (streamed) to the receivers.
 *
 * <p>This result partition implementation is used both in batch and streaming. For streaming it
 * supports low latency transfers (ensure data is sent within x milliseconds) or unconstrained while
 * for batch it transfers only once a buffer is full. Additionally, for streaming use this typically
 * limits the length of the buffer backlog to not have too much data in flight, while for batch we
 * do not constrain this.
 *
 * <h2>Specifics of the PipelinedResultPartition</h2>
 *
 * <p>The PipelinedResultPartition cannot reconnect once a consumer disconnects (finished or
 * errored). Once all consumers have disconnected (released the subpartition, notified via the call
 * {@link #onConsumedSubpartition(int)}) then the partition as a whole is disposed and all buffers
 * are freed.
 */
public class PipelinedResultPartition extends BufferWritingResultPartition
        implements CheckpointedResultPartition, ChannelStateHolder {

    /**
     * The lock that guard release operations (which can be asynchronously propagated from the
     * networks threads.
     */
    private final Object releaseLock = new Object();

    /**
     * A flag for each subpartition indicating whether it was already consumed or not, to make
     * releases idempotent.
     */
    @GuardedBy("releaseLock")
    private final boolean[] consumedSubpartitions;

    /**
     * The total number of references to subpartitions of this result. The result partition can be
     * safely released, iff the reference count is zero.
     */
    @GuardedBy("releaseLock")
    private int numUnconsumedSubpartitions;

    public PipelinedResultPartition(
            String owningTaskName,
            int partitionIndex,
            ResultPartitionID partitionId,
            ResultPartitionType partitionType,
            ResultSubpartition[] subpartitions,
            int numTargetKeyGroups,
            ResultPartitionManager partitionManager,
            @Nullable BufferCompressor bufferCompressor,
            SupplierWithException<BufferPool, IOException> bufferPoolFactory) {

        super(
                owningTaskName,
                partitionIndex,
                partitionId,
                checkResultPartitionType(partitionType),
                subpartitions,
                numTargetKeyGroups,
                partitionManager,
                bufferCompressor,
                bufferPoolFactory);

        this.consumedSubpartitions = new boolean[subpartitions.length];
        this.numUnconsumedSubpartitions = subpartitions.length;
    }

    @Override
    public void setChannelStateWriter(ChannelStateWriter channelStateWriter) {
        for (final ResultSubpartition subpartition : subpartitions) {
            if (subpartition instanceof ChannelStateHolder) {
                ((PipelinedSubpartition) subpartition).setChannelStateWriter(channelStateWriter);
            }
        }
    }

    /**
     * The pipelined partition releases automatically once all subpartition readers are released.
     * That is because pipelined partitions cannot be consumed multiple times, or reconnect.
     */
    @Override
    void onConsumedSubpartition(int subpartitionIndex) {
        if (isReleased()) {
            return;
        }

        final int remainingUnconsumed;

        // we synchronize only the bookkeeping section, to avoid holding the lock during any
        // calls into other components
        synchronized (releaseLock) {
            if (consumedSubpartitions[subpartitionIndex]) {
                // repeated call - ignore
                return;
            }

            consumedSubpartitions[subpartitionIndex] = true;
            remainingUnconsumed = (--numUnconsumedSubpartitions);
        }

        LOG.debug(
                "{}: Received consumed notification for subpartition {}.", this, subpartitionIndex);

        if (remainingUnconsumed == 0) {
            partitionManager.onConsumedPartition(this);
        } else if (remainingUnconsumed < 0) {
            throw new IllegalStateException(
                    "Received consume notification even though all subpartitions are already consumed.");
        }
    }

    @Override
    public CheckpointedResultSubpartition getCheckpointedSubpartition(int subpartitionIndex) {
        return (CheckpointedResultSubpartition) subpartitions[subpartitionIndex];
    }

    @Override
    public void flushAll() {
        flushAllSubpartitions(false);
    }

    @Override
    public void flush(int targetSubpartition) {
        flushSubpartition(targetSubpartition, false);
    }

    @Override
    @SuppressWarnings("FieldAccessNotGuarded")
    public String toString() {
        return "PipelinedResultPartition "
                + partitionId.toString()
                + " ["
                + partitionType
                + ", "
                + subpartitions.length
                + " subpartitions, "
                + numUnconsumedSubpartitions
                + " pending consumptions]";
    }

    // ------------------------------------------------------------------------
    //   miscellaneous utils
    // ------------------------------------------------------------------------

    private static ResultPartitionType checkResultPartitionType(ResultPartitionType type) {
        checkArgument(
                type == ResultPartitionType.PIPELINED
                        || type == ResultPartitionType.PIPELINED_BOUNDED
                        || type == ResultPartitionType.PIPELINED_APPROXIMATE);
        return type;
    }

    @Override
    public void finishReadRecoveredState(boolean notifyAndBlockOnCompletion) throws IOException {
        for (ResultSubpartition subpartition : subpartitions) {
            ((CheckpointedResultSubpartition) subpartition)
                    .finishReadRecoveredState(notifyAndBlockOnCompletion);
        }
    }
}
