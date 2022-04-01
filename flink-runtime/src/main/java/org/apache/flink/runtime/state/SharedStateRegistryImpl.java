/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.jobgraph.RestoreMode;
import org.apache.flink.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** {@link SharedStateRegistry} implementation. */
@Internal
public class SharedStateRegistryImpl implements SharedStateRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(SharedStateRegistryImpl.class);

    /** All registered state objects by an artificial key */
    private final Map<SharedStateRegistryKey, SharedStateEntry> registeredStates;

    /** This flag indicates whether or not the registry is open or if close() was called */
    private boolean open;

    /** Executor for async state deletion */
    private final Executor asyncDisposalExecutor;

    /** Checkpoint ID below which no state is discarded, inclusive. */
    private long highestNotClaimedCheckpointID = -1L;

    /** Default uses direct executor to delete unreferenced state */
    public SharedStateRegistryImpl() {
        this(Executors.directExecutor());
    }

    public SharedStateRegistryImpl(Executor asyncDisposalExecutor) {
        this.registeredStates = new HashMap<>();
        this.asyncDisposalExecutor = checkNotNull(asyncDisposalExecutor);
        this.open = true;
    }

    public StreamStateHandle registerReference(
            SharedStateRegistryKey registrationKey, StreamStateHandle state, long checkpointID) {

        checkNotNull(state);

        StreamStateHandle scheduledStateDeletion = null;
        SharedStateEntry entry;

        synchronized (registeredStates) {
            checkState(open, "Attempt to register state to closed SharedStateRegistry.");

            entry = registeredStates.get(registrationKey);

            if (entry == null) {
                // Additional check that should never fail, because only state handles that are not
                // placeholders should
                // ever be inserted to the registry.
                checkState(
                        !isPlaceholder(state),
                        "Attempt to reference unknown state: " + registrationKey);

                entry = new SharedStateEntry(state, checkpointID);
                registeredStates.put(registrationKey, entry);
                LOG.trace("Registered new shared state {} under key {}.", entry, registrationKey);

            } else {
                // Delete if this is a real duplicate.
                // Note that task (backend) is not required to re-upload state
                // if the confirmation notification was missing.
                // However, it's also not required to use exactly the same handle or placeholder
                if (!Objects.equals(state, entry.stateHandle)) {
                    if (entry.confirmed || isPlaceholder(state)) {
                        scheduledStateDeletion = state;
                    } else {
                        // Old entry is not in a confirmed checkpoint yet, and the new one differs.
                        // This might result from (omitted KG range here for simplicity):
                        // 1. Flink recovers from a failure using a checkpoint 1
                        // 2. State Backend is initialized to UID xyz and a set of SST: { 01.sst }
                        // 3. JM triggers checkpoint 2
                        // 4. TM sends handle: "xyz-002.sst"; JM registers it under "xyz-002.sst"
                        // 5. TM crashes; everything is repeated from (2)
                        // 6. TM recovers from CP 1 again: backend UID "xyz", SST { 01.sst }
                        // 7. JM triggers checkpoint 3
                        // 8. TM sends NEW state "xyz-002.sst"
                        // 9. JM discards it as duplicate
                        // 10. checkpoint completes, but a wrong SST file is used
                        // So we use a new entry and discard the old one:
                        scheduledStateDeletion = entry.stateHandle;
                        entry.stateHandle = state;
                    }
                    LOG.trace(
                            "Identified duplicate state registration under key {}. New state {} was determined to "
                                    + "be an unnecessary copy of existing state {} and will be dropped.",
                            registrationKey,
                            state,
                            entry.stateHandle);
                }
                LOG.trace(
                        "Updating last checkpoint for {} from {} to {}",
                        registrationKey,
                        entry.lastUsedCheckpointID,
                        checkpointID);
                entry.advanceLastUsingCheckpointID(checkpointID);
            }
        }

        scheduleAsyncDelete(scheduledStateDeletion);
        return entry.stateHandle;
    }

    @Override
    public void unregisterUnusedState(long lowestCheckpointID) {
        LOG.debug(
                "Discard state created before checkpoint {} and not used afterwards",
                lowestCheckpointID);
        List<StreamStateHandle> subsumed = new ArrayList<>();
        // Iterate over all the registered state handles.
        // Using a simple loop and NOT index by checkpointID because:
        // 1. Maintaining index leads to the same time complexity and worse memory complexity
        // 2. Most of the entries are expected to be carried to the next checkpoint
        synchronized (registeredStates) {
            Iterator<SharedStateEntry> it = registeredStates.values().iterator();
            while (it.hasNext()) {
                SharedStateEntry entry = it.next();
                if (entry.lastUsedCheckpointID < lowestCheckpointID) {
                    if (entry.createdByCheckpointID > highestNotClaimedCheckpointID) {
                        subsumed.add(entry.stateHandle);
                    }
                    it.remove();
                }
            }
        }

        LOG.trace("Discard {} state asynchronously", subsumed.size());
        for (StreamStateHandle handle : subsumed) {
            scheduleAsyncDelete(handle);
        }
    }

    @Override
    public void registerAll(
            Iterable<? extends CompositeStateHandle> stateHandles, long checkpointID) {

        if (stateHandles == null) {
            return;
        }

        synchronized (registeredStates) {
            for (CompositeStateHandle stateHandle : stateHandles) {
                stateHandle.registerSharedStates(this, checkpointID);
            }
        }
    }

    @Override
    public void registerAllAfterRestored(CompletedCheckpoint checkpoint, RestoreMode mode) {
        registerAll(checkpoint.getOperatorStates().values(), checkpoint.getCheckpointID());
        // In NO_CLAIM and LEGACY restore modes, shared state of the initial checkpoints must be
        // preserved. This is achieved by advancing highestRetainCheckpointID here, and then
        // checking entry.createdByCheckpointID against it on checkpoint subsumption.
        // In CLAIM restore mode, the shared state of the initial checkpoints must be
        // discarded as soon as it becomes unused - so highestRetainCheckpointID is not updated.
        if (mode != RestoreMode.CLAIM) {
            highestNotClaimedCheckpointID =
                    Math.max(highestNotClaimedCheckpointID, checkpoint.getCheckpointID());
        }
    }

    @Override
    public void checkpointCompleted(long checkpointId) {
        for (SharedStateEntry entry : registeredStates.values()) {
            if (entry.lastUsedCheckpointID == checkpointId) {
                entry.confirmed = true;
            }
        }
    }

    @Override
    public String toString() {
        synchronized (registeredStates) {
            return "SharedStateRegistry{" + "registeredStates=" + registeredStates + '}';
        }
    }

    private void scheduleAsyncDelete(StreamStateHandle streamStateHandle) {
        // We do the small optimization to not issue discards for placeholders, which are NOPs.
        if (streamStateHandle != null && !isPlaceholder(streamStateHandle)) {
            LOG.trace("Scheduled delete of state handle {}.", streamStateHandle);
            AsyncDisposalRunnable asyncDisposalRunnable =
                    new AsyncDisposalRunnable(streamStateHandle);
            try {
                asyncDisposalExecutor.execute(asyncDisposalRunnable);
            } catch (RejectedExecutionException ex) {
                // TODO This is a temporary fix for a problem during
                // ZooKeeperCompletedCheckpointStore#shutdown:
                // Disposal is issued in another async thread and the shutdown proceeds to close the
                // I/O Executor pool.
                // This leads to RejectedExecutionException once the async deletes are triggered by
                // ZK. We need to
                // wait for all pending ZK deletes before closing the I/O Executor pool. We can
                // simply call #run()
                // because we are already in the async ZK thread that disposes the handles.
                asyncDisposalRunnable.run();
            }
        }
    }

    private boolean isPlaceholder(StreamStateHandle stateHandle) {
        return stateHandle instanceof PlaceholderStreamStateHandle;
    }

    @Override
    public void close() {
        synchronized (registeredStates) {
            open = false;
        }
    }

    /** Encapsulates the operation the delete state handles asynchronously. */
    private static final class AsyncDisposalRunnable implements Runnable {

        private final StateObject toDispose;

        public AsyncDisposalRunnable(StateObject toDispose) {
            this.toDispose = checkNotNull(toDispose);
        }

        @Override
        public void run() {
            try {
                toDispose.discardState();
            } catch (Exception e) {
                LOG.warn(
                        "A problem occurred during asynchronous disposal of a shared state object: {}",
                        toDispose,
                        e);
            }
        }
    }
    /** An entry in the registry, tracking the handle and the corresponding reference count. */
    private static final class SharedStateEntry {

        /** The shared state handle */
        StreamStateHandle stateHandle;

        private final long createdByCheckpointID;

        private long lastUsedCheckpointID;

        /** Whether this entry is included into a confirmed checkpoint. */
        private boolean confirmed;

        SharedStateEntry(StreamStateHandle value, long checkpointID) {
            this.stateHandle = value;
            this.createdByCheckpointID = checkpointID;
            this.lastUsedCheckpointID = checkpointID;
        }

        @Override
        public String toString() {
            return "SharedStateEntry{"
                    + "stateHandle="
                    + stateHandle
                    + ", createdByCheckpointID="
                    + createdByCheckpointID
                    + ", lastUsedCheckpointID="
                    + lastUsedCheckpointID
                    + '}';
        }

        private void advanceLastUsingCheckpointID(long checkpointID) {
            lastUsedCheckpointID = Math.max(checkpointID, lastUsedCheckpointID);
        }
    }
}
