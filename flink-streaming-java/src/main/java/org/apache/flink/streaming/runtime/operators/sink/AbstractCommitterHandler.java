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

package org.apache.flink.streaming.runtime.operators.sink;

import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Abstract base class for operators that work with a {@link Committer} or a {@link
 * GlobalCommitter}.
 *
 * @param <CommT> The input and output type of the {@link Committer}.
 * @param <StateT> The type of the internal state.
 */
abstract class AbstractCommitterHandler<CommT, StateT> implements CommitterHandler<CommT> {

    /** Record all the committables until commit. */
    private final Deque<InternalCommittable<CommT>> committables = new ArrayDeque<>();

    /** The committables that need to be committed again after recovering from a failover. */
    private final List<InternalCommittable<StateT>> recoveredCommittables = new ArrayList<>();

    /**
     * Notifies a list of committables that might need to be committed again after recovering from a
     * failover.
     *
     * @param recovered A list of committables
     */
    protected void recoveredCommittables(List<InternalCommittable<StateT>> recovered)
            throws IOException {
        recoveredCommittables.addAll(checkNotNull(recovered));
    }

    protected List<InternalCommittable<StateT>> prependRecoveredCommittables(
            List<InternalCommittable<StateT>> committables) {
        if (recoveredCommittables.isEmpty()) {
            return committables;
        }
        List<InternalCommittable<StateT>> all =
                new ArrayList<>(recoveredCommittables.size() + committables.size());
        all.addAll(recoveredCommittables);
        all.addAll(committables);
        recoveredCommittables.clear();
        return all;
    }

    protected final CommitResult<InternalCommittable<StateT>> commit(
            List<InternalCommittable<StateT>> committables)
            throws IOException, InterruptedException {
        List<StateT> failed = commitInternal(unwrap(committables));
        if (failed.isEmpty()) {
            return new CommitResult<>(committables, Collections.emptyList());
        }

        // Assume that (Global)Committer#commit does not create a new instance for failed
        // committables. This assumption is documented in the respective JavaDoc.
        Set<StateT> failedRaw =
                Collections.newSetFromMap(new IdentityHashMap<>(committables.size()));
        failedRaw.addAll(failed);

        Map<Boolean, List<InternalCommittable<StateT>>> correlated =
                committables.stream()
                        .collect(
                                Collectors.groupingBy(
                                        stic -> failedRaw.contains(stic.getCommittable())));
        recoveredCommittables(correlated.get(true));
        return new CommitResult<>(correlated.get(false), correlated.get(true));
    }

    protected final void commit2(List<StateT> committables)
            throws IOException, InterruptedException {
        List<StateT> failed = commitInternal(committables);
        if (!failed.isEmpty()) {
            recoveredCommittables(failed);
        }
    }

    static <CommT> List<CommT> unwrap(List<InternalCommittable<CommT>> committables) {
        return committables.stream()
                .map(InternalCommittable::getCommittable)
                .collect(Collectors.toList());
    }

    /**
     * Commits a list of committables.
     *
     * @param committables A list of committables that is ready for committing.
     * @return A list of committables needed to re-commit.
     */
    abstract List<StateT> commitInternal(List<StateT> committables)
            throws IOException, InterruptedException;

    @Override
    public boolean needsRetry() {
        return !recoveredCommittables.isEmpty();
    }

    @Override
    public Collection<InternalCommittable<CommT>> retry() throws IOException, InterruptedException {
        return retry(prependRecoveredCommittables(Collections.emptyList()));
    }

    protected Collection<InternalCommittable<CommT>> retry(
            List<InternalCommittable<StateT>> recoveredCommittables)
            throws IOException, InterruptedException {
        commit(recoveredCommittables);
        return Collections.emptyList();
    }

    @Override
    public Collection<InternalCommittable<CommT>> processCommittables(
            Collection<InternalCommittable<CommT>> committables) {
        this.committables.addAll(committables);
        return Collections.emptyList();
    }

    protected List<InternalCommittable<CommT>> pollCommittables() {
        List<InternalCommittable<CommT>> committables = new ArrayList<>(this.committables);
        this.committables.clear();
        return committables;
    }

    @Override
    public void close() throws Exception {
        committables.clear();
    }

    static class CommitResult<CommT> {
        private final List<CommT> successful;
        private final List<CommT> failed;

        CommitResult(List<CommT> successful, List<CommT> failed) {
            this.successful = checkNotNull(successful);
            this.failed = checkNotNull(failed);
        }

        public List<CommT> getSuccessful() {
            return successful;
        }

        public List<CommT> getFailed() {
            return failed;
        }
    }
}
