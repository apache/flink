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
import java.util.Set;

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
    private final Deque<CommT> committables = new ArrayDeque<>();

    /** The committables that need to be committed again after recovering from a failover. */
    private final List<StateT> recoveredCommittables = new ArrayList<>();

    /**
     * Notifies a list of committables that might need to be committed again after recovering from a
     * failover.
     *
     * @param recovered A list of committables
     */
    protected void recoveredCommittables(List<StateT> recovered) throws IOException {
        recoveredCommittables.addAll(checkNotNull(recovered));
    }

    protected List<StateT> prependRecoveredCommittables(List<StateT> committables) {
        if (recoveredCommittables.isEmpty()) {
            return committables;
        }
        List<StateT> all = new ArrayList<>(recoveredCommittables.size() + committables.size());
        all.addAll(recoveredCommittables);
        all.addAll(committables);
        recoveredCommittables.clear();
        return all;
    }

    protected final Collection<StateT> commitAndReturnSuccess(List<StateT> committables)
            throws IOException, InterruptedException {
        Collection<StateT> failed = commit(committables);
        if (failed.isEmpty()) {
            return committables;
        }
        // Assume that (Global)Committer#commit does not create a new instance for failed
        // committables. This assumption is documented in the respective JavaDoc.
        Set<StateT> successful =
                Collections.newSetFromMap(new IdentityHashMap<>(committables.size()));
        successful.addAll(committables);
        successful.removeAll(failed);
        return successful;
    }

    protected final Collection<StateT> commit(List<StateT> committables)
            throws IOException, InterruptedException {
        List<StateT> failed = commitInternal(committables);
        recoveredCommittables(failed);
        return failed;
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
    public Collection<CommT> retry() throws IOException, InterruptedException {
        return retry(prependRecoveredCommittables(Collections.emptyList()));
    }

    protected Collection<CommT> retry(List<StateT> recoveredCommittables)
            throws IOException, InterruptedException {
        commit(recoveredCommittables);
        return Collections.emptyList();
    }

    @Override
    public Collection<CommT> processCommittables(Collection<CommT> committables) {
        this.committables.addAll(committables);
        return Collections.emptyList();
    }

    protected List<CommT> pollCommittables() {
        List<CommT> committables = new ArrayList<>(this.committables);
        this.committables.clear();
        return committables;
    }

    @Override
    public void close() throws Exception {
        committables.clear();
    }
}
