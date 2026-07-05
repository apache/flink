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
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;

import java.io.IOException;

@Internal
public interface RecoveryCheckpointTrigger {

    /**
     * Atomically snapshots the undrained recovered state and inserts matching {@link
     * RecoveryCheckpointBarrier}s into in-recovery channels.
     *
     * <p>FLINK-38544 transitional signature: once the spilling backend lands this returns an
     * independent reader over the remaining (undrained) spill segments that the caller owns and
     * must close. The in-memory backend hands all recovered buffers to the channels in one shot, so
     * there is never an undrained residue and nothing to return yet.
     */
    void snapshotAndInsertBarriers(long checkpointId) throws IOException, CheckpointException;

    /** Inserts no barriers (and there is no recovered state left to snapshot). */
    RecoveryCheckpointTrigger NO_OP = checkpointId -> {};

    RecoveryCheckpointTrigger NOT_READY =
            ign -> {
                // A checkpoint barrier reached the barrier handler before channel-state recovery
                // finished wiring up the real trigger: the input gates were already requested (so
                // barriers can arrive) but the trigger is still being installed on the mailbox
                // thread. With checkpointing-during-recovery this is an expected, transient
                // condition, not a fatal invariant violation. Decline the checkpoint as
                // TASK_NOT_READY so the coordinator aborts and retries it once recovery completes.
                // Throwing a CheckpointException (rather than a RuntimeException) lets
                // SingleCheckpointBarrierHandler#markCheckpointAlignedAndTransformState route it to
                // abortInternal instead of failing the task.
                throw new CheckpointException(
                        CheckpointFailureReason.CHECKPOINT_DECLINED_TASK_NOT_READY);
            };
}
