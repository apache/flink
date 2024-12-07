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

package org.apache.flink.runtime.operators.coordination;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.metrics.groups.OperatorCoordinatorMetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.messages.Acknowledge;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;

/**
 * A coordinator for runtime operators. The OperatorCoordinator runs on the master, associated with
 * the job vertex of the operator. It communicates with operators via sending operator events.
 *
 * <p>Operator coordinators are for example source and sink coordinators that discover and assign
 * work, or aggregate and commit metadata.
 *
 * <h2>Thread Model</h2>
 *
 * <p>All coordinator methods are called by the Job Manager's main thread (mailbox thread). That
 * means that these methods must not, under any circumstances, perform blocking operations (like I/O
 * or waiting on locks or futures). That would run a high risk of bringing down the entire
 * JobManager.
 *
 * <p>Coordinators that involve more complex operations should hence spawn threads to handle the I/O
 * work. The methods on the {@link Context} are safe to be called from another thread than the
 * thread that calls the Coordinator's methods.
 *
 * <h2>Consistency</h2>
 *
 * <p>The coordinator's view of the task execution is highly simplified, compared to the Scheduler's
 * view, but allows for consistent interaction with the operators running on the parallel subtasks.
 * In particular, the following methods are guaranteed to be called strictly in order:
 *
 * <ol>
 *   <li>{@link #executionAttemptReady(int, int, SubtaskGateway)}: Called once you can send events
 *       to the subtask execution attempt. The provided gateway is bound to that specific execution
 *       attempt. This is the start of interaction with the operator subtask attempt.
 *   <li>{@link #executionAttemptFailed(int, int, Throwable)}: Called for each subtask execution
 *       attempt as soon as the attempt failed or was cancelled. At this point, interaction with the
 *       subtask attempt should stop.
 *   <li>{@link #subtaskReset(int, long)} or {@link #resetToCheckpoint(long, byte[])}: Once the
 *       scheduler determined which checkpoint to restore, these methods notify the coordinator of
 *       that. The former method is called in case of a regional failure/recovery (affecting
 *       possible a subset of subtasks), the later method in case of a global failure/recovery. This
 *       method should be used to determine which actions to recover, because it tells you which
 *       checkpoint to fall back to. The coordinator implementation needs to recover the
 *       interactions with the relevant tasks since the checkpoint that is restored. It will be
 *       called only after {@link #executionAttemptFailed(int, int, Throwable)} has been called on
 *       all the attempts of the subtask.
 *   <li>{@link #executionAttemptReady(int, int, SubtaskGateway)}: Called again, once the recovered
 *       tasks (new attempts) are ready to go. This is later than {@link #subtaskReset(int, long)},
 *       because between those methods, the new attempts are scheduled and deployed.
 * </ol>
 */
@Internal
public interface OperatorCoordinator extends CheckpointListener, AutoCloseable {

    /**
     * The checkpoint ID passed to the restore methods when no completed checkpoint exists, yet. It
     * indicates that the restore is to the "initial state" of the coordinator or the failed
     * subtask.
     */
    long NO_CHECKPOINT = -1L;

    /** The checkpoint ID passed to the restore methods when batch scenarios. */
    long BATCH_CHECKPOINT_ID = -1L;

    // ------------------------------------------------------------------------

    /**
     * Starts the coordinator. This method is called once at the beginning, before any other
     * methods.
     *
     * @throws Exception Any exception thrown from this method causes a full job failure.
     */
    void start() throws Exception;

    /**
     * This method is called when the coordinator is disposed. This method should release currently
     * held resources. Exceptions in this method do not cause the job to fail.
     */
    @Override
    void close() throws Exception;

    // ------------------------------------------------------------------------

    /**
     * Hands an OperatorEvent coming from a parallel Operator instance (one attempt of the parallel
     * subtasks).
     *
     * @throws Exception Any exception thrown by this method results in a full job failure and
     *     recovery.
     */
    void handleEventFromOperator(int subtask, int attemptNumber, OperatorEvent event)
            throws Exception;

    // ------------------------------------------------------------------------

    /**
     * Takes a checkpoint of the coordinator. The checkpoint is identified by the given ID.
     *
     * <p>To confirm the checkpoint and store state in it, the given {@code CompletableFuture} must
     * be completed with the state. To abort or dis-confirm the checkpoint, the given {@code
     * CompletableFuture} must be completed exceptionally. In any case, the given {@code
     * CompletableFuture} must be completed in some way, otherwise the checkpoint will not progress.
     *
     * <h3>Exactly-once Semantics</h3>
     *
     * <p>The semantics are defined as follows:
     *
     * <ul>
     *   <li>The point in time when the checkpoint future is completed is considered the point in
     *       time when the coordinator's checkpoint takes place.
     *   <li>The OperatorCoordinator implementation must have a way of strictly ordering the sending
     *       of events and the completion of the checkpoint future (for example the same thread does
     *       both actions, or both actions are guarded by a mutex).
     *   <li>Every event sent before the checkpoint future is completed is considered before the
     *       checkpoint.
     *   <li>Every event sent after the checkpoint future is completed is considered to be after the
     *       checkpoint.
     * </ul>
     *
     * @throws Exception Any exception thrown by this method results in a full job failure and
     *     recovery.
     */
    void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> resultFuture)
            throws Exception;

    /**
     * We override the method here to remove the checked exception. Please check the Java docs of
     * {@link CheckpointListener#notifyCheckpointComplete(long)} for more detail semantic of the
     * method.
     */
    @Override
    void notifyCheckpointComplete(long checkpointId);

    /**
     * We override the method here to remove the checked exception. Please check the Java docs of
     * {@link CheckpointListener#notifyCheckpointAborted(long)} for more detail semantic of the
     * method.
     */
    @Override
    default void notifyCheckpointAborted(long checkpointId) {}

    /**
     * Resets the coordinator to the given checkpoint. When this method is called, the coordinator
     * can discard all other in-flight working state. All subtasks will also have been reset to the
     * same checkpoint.
     *
     * <p>This method is called in the case of a <i>global failover</i> of the system, which means a
     * failover of the coordinator (JobManager). This method is not invoked on a <i>partial
     * failover</i>; partial failovers call the {@link #subtaskReset(int, long)} method for the
     * involved subtasks.
     *
     * <p>This method is expected to behave synchronously with respect to other method calls and
     * calls to {@code Context} methods. For example, Events being sent by the Coordinator after
     * this method returns are assumed to take place after the checkpoint that was restored.
     *
     * <p>This method is called with a null state argument in the following situations:
     *
     * <ul>
     *   <li>There is a recovery and there was no completed checkpoint yet.
     *   <li>There is a recovery from a completed checkpoint/savepoint but it contained no state for
     *       the coordinator.
     * </ul>
     *
     * <p>In both cases, the coordinator should reset to an empty (new) state.
     *
     * <h2>Restoring implicitly notifies of Checkpoint Completion</h2>
     *
     * <p>Restoring to a checkpoint is a way of confirming that the checkpoint is complete. It is
     * safe to commit side-effects that are predicated on checkpoint completion after this call.
     *
     * <p>Even if no call to {@link #notifyCheckpointComplete(long)} happened, the checkpoint can
     * still be complete (for example when a system failure happened directly after committing the
     * checkpoint, before calling the {@link #notifyCheckpointComplete(long)} method).
     */
    void resetToCheckpoint(long checkpointId, @Nullable byte[] checkpointData) throws Exception;

    // ------------------------------------------------------------------------

    /**
     * Called if a subtask is recovered as part of a <i>partial failover</i>, meaning a failover
     * handled by the scheduler's failover strategy (by default recovering a pipelined region). The
     * method is invoked for each subtask involved in that partial failover.
     *
     * <p>In contrast to this method, the {@link #resetToCheckpoint(long, byte[])} method is called
     * in the case of a global failover, which is the case when the coordinator (JobManager) is
     * recovered.
     *
     * <p>Note that this method will not be called if an execution attempt of a subtask failed, if
     * the subtask is not entirely failed, i.e. if the subtask has other execution attempts that are
     * not failed/canceled.
     */
    void subtaskReset(int subtask, long checkpointId);

    /**
     * Called when any subtask execution attempt of the task running the coordinated operator is
     * failed/canceled.
     *
     * <p>This method is called every time an execution attempt is failed/canceled, regardless of
     * whether there it is caused by a partial failover or a global failover.
     */
    void executionAttemptFailed(int subtask, int attemptNumber, @Nullable Throwable reason);

    /**
     * This is called when a subtask execution attempt of the Operator becomes ready to receive
     * events. The given {@code SubtaskGateway} can be used to send events to the execution attempt.
     *
     * <p>The given {@code SubtaskGateway} is bound to that specific execution attempt that became
     * ready. All events sent through the gateway target that execution attempt; if the attempt is
     * no longer running by the time the event is sent, then the events are failed.
     */
    void executionAttemptReady(int subtask, int attemptNumber, SubtaskGateway gateway);

    /**
     * Whether the operator coordinator supports taking snapshot in no-checkpoint/batch scenarios.
     * If it returns true, the {@link OperatorCoordinator#checkpointCoordinator} and {@link
     * OperatorCoordinator#resetToCheckpoint} methods supports taking snapshot and restoring from a
     * snapshot in batch processing scenarios. In such scenarios, the checkpointId will always be
     * -1.
     */
    default boolean supportsBatchSnapshot() {
        return false;
    }

    // ------------------------------------------------------------------------
    // ------------------------------------------------------------------------

    /**
     * The context gives the OperatorCoordinator access to contextual information and provides a
     * gateway to interact with other components, such as sending operator events.
     */
    interface Context {

        /** Gets the {@link JobID} of the job to which the coordinator belongs. */
        JobID getJobID();

        /** Gets the ID of the operator to which the coordinator belongs. */
        OperatorID getOperatorId();

        /** Gets the metric group of the operator coordinator. */
        OperatorCoordinatorMetricGroup metricGroup();

        /**
         * Fails the job and trigger a global failover operation.
         *
         * <p>This operation restores the entire job to the latest complete checkpoint. This is
         * useful to recover from inconsistent situations (the view from the coordinator and its
         * subtasks as diverged), but is expensive and should be used with care.
         */
        void failJob(Throwable cause);

        /** Gets the current parallelism with which this operator is executed. */
        int currentParallelism();

        /**
         * Gets the classloader that contains the additional dependencies, which are not part of the
         * JVM's classpath.
         */
        ClassLoader getUserCodeClassloader();

        /**
         * Gets the {@link CoordinatorStore} instance for sharing information between {@link
         * OperatorCoordinator}s.
         */
        CoordinatorStore getCoordinatorStore();

        /**
         * Gets that whether the coordinator supports an execution vertex to have multiple
         * concurrent running execution attempts.
         */
        boolean isConcurrentExecutionAttemptsSupported();

        /** Gets the checkpoint coordinator of this job. Return null if checkpoint is disabled. */
        @Nullable
        CheckpointCoordinator getCheckpointCoordinator();
    }

    // ------------------------------------------------------------------------

    /**
     * The {@code SubtaskGateway} is the way to interact with a specific parallel instance of the
     * Operator (an Operator subtask), like sending events to the operator.
     */
    interface SubtaskGateway {

        /**
         * Sends an event to the parallel subtask with the given subtask index.
         *
         * <p>The returned future is completed successfully once the event has been received by the
         * target TaskManager. The future is completed exceptionally if the event cannot be sent.
         * That includes situations where the target task is not running.
         */
        CompletableFuture<Acknowledge> sendEvent(OperatorEvent evt);

        /**
         * Gets the execution attempt for the subtask execution attempt that this gateway
         * communicates with.
         */
        ExecutionAttemptID getExecution();

        /**
         * Gets the subtask index of the parallel operator instance this gateway communicates with.
         */
        int getSubtask();
    }

    // ------------------------------------------------------------------------

    /**
     * The provider creates an OperatorCoordinator and takes a {@link Context} to pass to the
     * OperatorCoordinator. This method is, for example, called on the job manager when the
     * scheduler and execution graph are created, to instantiate the OperatorCoordinator.
     *
     * <p>The factory is {@link Serializable}, because it is attached to the JobGraph and is part of
     * the serialized job graph that is sent to the dispatcher, or stored for recovery.
     */
    interface Provider extends Serializable {

        /** Gets the ID of the operator to which the coordinator belongs. */
        OperatorID getOperatorId();

        /** Creates the {@code OperatorCoordinator}, using the given context. */
        OperatorCoordinator create(Context context) throws Exception;
    }
}
