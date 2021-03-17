/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.common.state;

import org.apache.flink.annotation.Public;

/**
 * This interface is typically only needed for transactional interaction with the "outside world",
 * like committing external side effects on checkpoints. An example is committing external
 * transactions once a checkpoint completes.
 *
 * <h3>Invocation Guarantees</h3>
 *
 * <p>It is NOT guaranteed that the implementation will receive a notification for each completed or
 * aborted checkpoint. While these notifications come in most cases, notifications might not happen,
 * for example, when a failure/restore happens directly after a checkpoint completed.
 *
 * <p>To handle this correctly, implementation should follow the "Checkpoint Subsuming Contract"
 * described below.
 *
 * <h3>Exceptions</h3>
 *
 * <p>The notifications from this interface come "after the fact", meaning after the checkpoint has
 * been aborted or completed. Throwing an exception will not change the completion/abortion of the
 * checkpoint.
 *
 * <p>Exceptions thrown from this method result in task- or job failure and recovery.
 *
 * <h3>Checkpoint Subsuming Contract</h3>
 *
 * <p>Checkpoint IDs are strictly increasing. A checkpoint with higher ID always subsumes a
 * checkpoint with lower ID. For example, when checkpoint T is confirmed complete, the code can
 * assume that no checkpoints with lower ID (T-1, T-2, etc.) are pending any more. <b>No checkpoint
 * with lower ID will ever be committed after a checkpoint with a higher ID.</b>
 *
 * <p>This does not necessarily mean that all of the previous checkpoints actually completed
 * successfully. It is also possible that some checkpoint timed out or was not fully acknowledged by
 * all tasks. Implementations must then behave as if that checkpoint did not happen. The recommended
 * way to do this is to let the completion of a new checkpoint (higher ID) subsume the completion of
 * all earlier checkpoints (lower ID).
 *
 * <p>This property is easy to achieve for cases where increasing "offsets", "watermarks", or other
 * progress indicators are communicated on checkpoint completion. A newer checkpoint will have a
 * higher "offset" (more progress) than the previous checkpoint, so it automatically subsumes the
 * previous one. Remember the "offset to commit" for a checkpoint ID and commit it when that
 * specific checkpoint (by ID) gets the notification that it is complete.
 *
 * <p>If you need to publish some specific artifacts (like files) or acknowledge some specific IDs
 * after a checkpoint, you can follow a pattern like below.
 *
 * <h3>Implementing Checkpoint Subsuming for Committing Artifacts</h3>
 *
 * <p>The following is a sample pattern how applications can publish specific artifacts on
 * checkpoint. Examples would be operators that acknowledge specific IDs or publish specific files
 * on checkpoint.
 *
 * <ul>
 *   <li>During processing, have two sets of artifacts.
 *       <ol>
 *         <li>A "ready set": Artifacts that are ready to be published as part of the next
 *             checkpoint. Artifacts are added to this set as soon as they are ready to be
 *             committed. This set is "transient", it is not stored in Flink's state persisted
 *             anywhere.
 *         <li>A "pending set": Artifacts being committed with a checkpoint. The actual publishing
 *             happens when the checkpoint is complete. This is a map of "{@code long =>
 *             List<Artifact>}", mapping from the id of the checkpoint when the artifact was ready
 *             to the artifacts. /li>
 *       </ol>
 *   <li>On checkpoint, add that set of artifacts from the "ready set" to the "pending set",
 *       associated with the checkpoint ID. The whole "pending set" gets stored in the checkpoint
 *       state.
 *   <li>On {@code notifyCheckpointComplete()} publish all IDs/artifacts from the "pending set" up
 *       to the checkpoint with that ID. Remove these from the "pending set".
 *   <li/>
 * </ul>
 *
 * <p>That way, even if some checkpoints did not complete, or if the notification that they
 * completed got lost, the artifacts will be published as part of the next checkpoint that
 * completes.
 */
@Public
public interface CheckpointListener {

    /**
     * Notifies the listener that the checkpoint with the given {@code checkpointId} completed and
     * was committed.
     *
     * <p>These notifications are "best effort", meaning they can sometimes be skipped. To behave
     * properly, implementers need to follow the "Checkpoint Subsuming Contract". Please see the
     * {@link CheckpointListener class-level JavaDocs} for details.
     *
     * <p>Please note that checkpoints may generally overlap, so you cannot assume that the {@code
     * notifyCheckpointComplete()} call is always for the latest prior checkpoint (or snapshot) that
     * was taken on the function/operator implementing this interface. It might be for a checkpoint
     * that was triggered earlier. Implementing the "Checkpoint Subsuming Contract" (see above)
     * properly handles this situation correctly as well.
     *
     * <p>Please note that throwing exceptions from this method will not cause the completed
     * checkpoint to be revoked. Throwing exceptions will typically cause task/job failure and
     * trigger recovery.
     *
     * @param checkpointId The ID of the checkpoint that has been completed.
     * @throws Exception This method can propagate exceptions, which leads to a failure/recovery for
     *     the task. Not that this will NOT lead to the checkpoint being revoked.
     */
    void notifyCheckpointComplete(long checkpointId) throws Exception;

    /**
     * This method is called as a notification once a distributed checkpoint has been aborted.
     *
     * <p><b>Important:</b> The fact that a checkpoint has been aborted does NOT mean that the data
     * and artifacts produced between the previous checkpoint and the aborted checkpoint are to be
     * discarded. The expected behavior is as if this checkpoint was never triggered in the first
     * place, and the next successful checkpoint simply covers a longer time span. See the
     * "Checkpoint Subsuming Contract" in the {@link CheckpointListener class-level JavaDocs} for
     * details.
     *
     * <p>These notifications are "best effort", meaning they can sometimes be skipped.
     *
     * <p>This method is very rarely necessary to implement. The "best effort" guarantee, together
     * with the fact that this method should not result in discarding any data (per the "Checkpoint
     * Subsuming Contract") means it is mainly useful for earlier cleanups of auxiliary resources.
     * One example is to pro-actively clear a local per-checkpoint state cache upon checkpoint
     * failure.
     *
     * @param checkpointId The ID of the checkpoint that has been aborted.
     * @throws Exception This method can propagate exceptions, which leads to a failure/recovery for
     *     the task or job.
     */
    default void notifyCheckpointAborted(long checkpointId) throws Exception {}
}
