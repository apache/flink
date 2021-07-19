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

package org.apache.flink.runtime.state.track;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.state.StateObject;

import java.util.Collection;
import java.util.NoSuchElementException;

/**
 * A component responsible for managing state that is not shared across different TMs (i.e. not
 * managed by JM). Managing state here means {@link StateObject#discardState() deletion} once not
 * used.
 *
 * <p>Must not be shared between different jobs.
 *
 * <p>Explicitly aware of checkpointing so that tracking of associations between checkpoints and
 * snapshots can be reused (instead of implementing in each backend).
 *
 * <h1>State sharing inside TM</h1>
 *
 * <p>Inside each TM, the registry <strong>must</strong> be shared across state backends on the same
 * level as their state is, or higher. For example, if all state backends of operators of the same
 * job in TM use the same file on DFS then they <strong>must</strong> share the same registry. OTH,
 * if nothing is shared then the Registry can be per-backend (or shared).
 *
 * <h1>State sharing across TMs</h1>
 *
 * If a rescaling operation (or recovery with state sharing inside TM) results in some state being
 * shared by multiple TMs (or TaskStateRegistries), such relevant shared states must be communicated
 * to the registry so that it can ignore them. Implementations <strong>must</strong> ignore such
 * shared states.
 *
 * <h1>State identity</h1>
 *
 * For the purpose of matching state objects from the different calls (and from previous runs),
 * state can be identified by {@link StateObjectID}. A collection of such IDs can be obtained from
 * the {@link StateObject} by recursively traversing it. This is implementation specific; however,
 * it must be consistent across attempts, registries and backends.
 *
 * <h1>Basic usage</h1>
 *
 * <ol>
 *   <li>{@link #stateUsed(Collection) register state usage}
 *   <li>{@link #checkpointStarting(long) start checkpoint 1}
 *   <li>{@link #stateNotUsed(StateObject) unregister state usage}
 *   <li>{@link #checkpointStarting(long) start checkpoint 2} (state is not used as it was
 *       unregistered)
 *   <li>{@link #checkpointSubsumed(long) subsume checkpoint 1} (state can be discarded)
 * </ol>
 *
 * <p>It is not required to register state usage strictly before starting a checkpoint (mostly
 * because it is usually not known which state will be included into the snapshot when the
 * checkpoint starts).
 *
 * <p>Usually, state registration and un-registration is done automatically via {@link
 * #stateSnapshotCreated(StateObject, long)}.
 *
 * <h1>Consistency and dangling state</h1>
 *
 * There are several factors that can impact consistency: incremental checkpoints; out-of-order
 * calls from a single backend; multiple backends; multiple concurrent checkpoint. Potential issues
 * include:
 *
 * <ul>
 *   <li>Incremental checkpoint subsumption: if some state is shared across checkpoints then
 *       discarding an older checkpoint might invalidate subsequent checkpoints
 *   <li>Out-of-order checkpoint completion by a single backend: if a reported snapshot doesn't
 *       include some state anymore then that state can be discarded. Any <strong>earlier</strong>
 *       snapshots reported <strong>later</strong> still using this state later will be invalid
 *   <li>Out-of-order state (un)registration by multiple backends: if some state is shared then some
 *       backend might register and unregister its usage before the other starts using it. The
 *       other's backend snapshots with this state may be invalid.
 *   <li>With RETAIN_ON_CANCELLATION policy, last num-retained checkpoints shouldn't be discarded
 *       even if the job is cancelled (NOT IMPLEMENTED)
 * </ul>
 *
 * To allow implementations to address the above issues, client MUST:
 *
 * <ul>
 *   <li>list all the backends {@link #stateUsed(Collection) using some state} at once
 *   <li>list all state objects {@link #stateSnapshotCreated(StateObject, long) used in a snapshot}
 *       at once
 *   <li>notify that the checkpoint is {@link #checkpointStarting(long) started} before {@link
 *       #stateNotUsed(StateObject) unregistering} any state this checkpoint might be using (if a
 *       state was unregistered after a checkpoint was started it can still be included into the
 *       snapshot)
 *   <li>in particular, report the {@link #stateSnapshotCreated(StateObject, long)} snapshot} for a
 *       checkpoint only after notifying about its start
 *   <li>not {@link #checkpointStarting(long) start any checkpoint} that may have been {@link
 *       #checkpointSubsumed(long) subsumed}
 *   <li>for checkpoints, call either {@link #checkpointSubsumed(long) subsumed} or {@link
 *       #checkpointAborted(long) aborted} (otherwise, state won't be deleted until the job is
 *       terminated)
 * </ul>
 *
 * <h1>Thread safety</h1>
 *
 * Thread safety depends on the implementation. An implementation shared across different tasks (in
 * a single TM) MUST be thread-safe.
 *
 * <h1>Asynchronous discarding</h1>
 *
 * Production implementations might choose to discard the state asynchronously; however, they might
 * choose to block the caller if they don't keep up.
 */
@Internal
public interface TaskStateRegistry extends AutoCloseable {

    /**
     * Mark the given state as used by the given state backends. Should be called upon initial
     * creation of state object (e.g. upload to DFS). It can be called before or after {@link
     * #checkpointStarting(long) starting} a checkpoint using it.
     */
    void stateUsed(Collection<StateObject> states);

    /**
     * Mark the given state as not used anymore by the given backend (i.e. it will not be included
     * into any <strong>future</strong> snapshots); discard if not used by any other backend or
     * checkpoint. When using incremental checkpoints, it should be called upon materialization;
     * otherwise, on checkpoint subsumption (in addition to {@link #checkpointSubsumed(long)}. The
     * method does nothing if the state is not marked as used.
     *
     * <p>Note that there is no need to call this method during the shutdown - any state is
     * considered unused as no future checkpoints will be made.
     */
    void stateNotUsed(StateObject state);

    /**
     * Notify that the checkpoint is about to start. Until {@link #stateSnapshotCreated(StateObject,
     * long)} notified explicitly}, any state that is still in use by the backend is considered as
     * potentially used by this checkpoint.
     */
    void checkpointStarting(long checkpointId);

    /**
     * Notify about the state used in a snapshot for the given checkpoint (before or after sending
     * to JM). All state should be reported at once. The method serves the following goals:
     *
     * <ul>
     *   <li>more fine-grained tracking of state usage by checkpoints to allow deletion
     *   <li>automatic tracking of state usage (see below)
     * </ul>
     *
     * Must be called <strong>after</strong> the corresponding {@link #checkpointStarting(long)}.
     *
     * @throws NoSuchElementException if the state is not registered
     * @throws IllegalStateException if the given checkpoint was already performed
     */
    void stateSnapshotCreated(StateObject state, long checkpointId)
            throws NoSuchElementException, IllegalStateException;
    /**
     * Un-mark any states previously {@link #stateSnapshotCreated(StateObject, long) marked} as used
     * by given backend in the given checkpoint. Doesn't affect any other checkpoints. All state
     * objects that become unused are discarded.
     */
    void checkpointAborted(long checkpointId);

    /**
     * Un-mark any states previously {@link #stateSnapshotCreated(StateObject, long) marked} as used
     * by given backend in the given checkpoint. All earlier checkpoints of <strong>this
     * backend</strong> are also considered as subsumed. All state objects that become unused by any
     * backend or checkpoint anymore are discarded.
     *
     * <p>If the job is being finished (including stop-with-savepoint) then this method should be
     * called with the latest known info so that no extra checkpoints are left.
     */
    // TODO: call with the final subsumption info
    void checkpointSubsumed(long checkpointId);

    TaskStateRegistry NO_OP =
            new TaskStateRegistry() {

                @Override
                public void stateUsed(Collection<StateObject> state) {}

                @Override
                public void stateNotUsed(StateObject state) {}

                @Override
                public void checkpointStarting(long checkpointId) {}

                @Override
                public void stateSnapshotCreated(StateObject state, long checkpointId)
                        throws NoSuchElementException, IllegalStateException {}

                @Override
                public void checkpointAborted(long checkpointId) {}

                @Override
                public void checkpointSubsumed(long checkpointId) {}

                @Override
                public void close() {}
            };
}
