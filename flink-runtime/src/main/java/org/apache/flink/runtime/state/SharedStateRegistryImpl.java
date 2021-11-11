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
import org.apache.flink.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
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

    /** Default uses direct executor to delete unreferenced state */
    public SharedStateRegistryImpl() {
        this(Executors.directExecutor());
    }

    public SharedStateRegistryImpl(Executor asyncDisposalExecutor) {
        this.registeredStates = new HashMap<>();
        this.asyncDisposalExecutor = checkNotNull(asyncDisposalExecutor);
        this.open = true;
    }

    @Override
    public Result registerReference(
            SharedStateRegistryKey registrationKey, StreamStateHandle state) {

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

                entry = new SharedStateEntry(state);
                registeredStates.put(registrationKey, entry);

            } else {
                // delete if this is a real duplicate
                if (!Objects.equals(state, entry.stateHandle)) {
                    scheduledStateDeletion = state;
                    LOG.trace(
                            "Identified duplicate state registration under key {}. New state {} was determined to "
                                    + "be an unnecessary copy of existing state {} and will be dropped.",
                            registrationKey,
                            state,
                            entry.stateHandle);
                }
                entry.increaseReferenceCount();
            }
        }

        scheduleAsyncDelete(scheduledStateDeletion);
        LOG.trace("Registered shared state {} under key {}.", entry, registrationKey);
        return new Result(entry);
    }

    @Override
    public Result unregisterReference(SharedStateRegistryKey registrationKey) {

        checkNotNull(registrationKey);

        final Result result;
        final StreamStateHandle scheduledStateDeletion;
        SharedStateEntry entry;

        synchronized (registeredStates) {
            entry = registeredStates.get(registrationKey);

            checkState(
                    entry != null,
                    "Cannot unregister a state that is not registered: %s",
                    registrationKey);

            entry.decreaseReferenceCount();

            // Remove the state from the registry when it's not referenced any more.
            if (entry.getReferenceCount() <= 0) {
                registeredStates.remove(registrationKey);
                scheduledStateDeletion = entry.getStateHandle();
                result = new Result(null, 0);
            } else {
                scheduledStateDeletion = null;
                result = new Result(entry);
            }
        }

        LOG.trace("Unregistered shared state {} under key {}.", entry, registrationKey);
        scheduleAsyncDelete(scheduledStateDeletion);
        return result;
    }

    @Override
    public void registerAll(Iterable<? extends CompositeStateHandle> stateHandles) {

        if (stateHandles == null) {
            return;
        }

        synchronized (registeredStates) {
            for (CompositeStateHandle stateHandle : stateHandles) {
                stateHandle.registerSharedStates(this);
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
}
