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

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

abstract class AbstractCommitterHandler<InputT, OutputT, RecoverT>
        implements CommitterHandler<InputT, OutputT> {

    /** Record all the committables until commit. */
    private final Deque<InputT> committables = new ArrayDeque<>();

    /** The committables that need to be committed again after recovering from a failover. */
    private final List<RecoverT> recoveredCommittables = new ArrayList<>();

    /**
     * Notifies a list of committables that might need to be committed again after recovering from a
     * failover.
     *
     * @param recovered A list of committables
     */
    protected void recoveredCommittables(List<RecoverT> recovered) throws IOException {
        recoveredCommittables.addAll(checkNotNull(recovered));
    }

    protected List<RecoverT> prependRecoveredCommittables(List<RecoverT> committables) {
        if (recoveredCommittables.isEmpty()) {
            return committables;
        }
        List<RecoverT> all = new ArrayList<>(recoveredCommittables.size() + committables.size());
        all.addAll(recoveredCommittables);
        all.addAll(committables);
        recoveredCommittables.clear();
        return all;
    }

    @Override
    public boolean needsRetry() {
        return !recoveredCommittables.isEmpty();
    }

    @Override
    public void retry() throws IOException, InterruptedException {
        retry(prependRecoveredCommittables(Collections.emptyList()));
    }

    protected abstract void retry(List<RecoverT> recoveredCommittables)
            throws IOException, InterruptedException;

    @Override
    public List<OutputT> processCommittables(List<InputT> committables) {
        this.committables.addAll(committables);
        return Collections.emptyList();
    }

    protected List<InputT> pollCommittables() {
        List<InputT> committables = new ArrayList<>(this.committables);
        this.committables.clear();
        return committables;
    }

    @Override
    public void close() throws Exception {
        committables.clear();
    }
}
