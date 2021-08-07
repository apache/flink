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

package org.apache.flink.streaming.runtime.operators.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link CommitterHandler} for executing {@link GlobalCommitter} in the streaming execution mode.
 *
 * @param <CommT> The committable type of the {@link GlobalCommitter}.
 * @param <GlobalCommT> The global committable type of the {@link GlobalCommitter}.
 */
@Internal
public final class GlobalStreamingCommitterHandler<CommT, GlobalCommT>
        extends AbstractStreamingCommitterHandler<CommT, GlobalCommT> {

    /**
     * Aggregate committables to global committables and commit the global committables to the
     * external system.
     */
    private final GlobalCommitter<CommT, GlobalCommT> globalCommitter;

    /**
     * The global committables that might need to be committed again after recovering from a
     * failover.
     */
    private final List<GlobalCommT> recoveredGlobalCommittables;

    private boolean endOfInput;

    public GlobalStreamingCommitterHandler(
            GlobalCommitter<CommT, GlobalCommT> globalCommitter,
            SimpleVersionedSerializer<GlobalCommT> committableSerializer) {
        super(committableSerializer);
        this.globalCommitter = checkNotNull(globalCommitter);

        this.recoveredGlobalCommittables = new ArrayList<>();
        this.endOfInput = false;
    }

    @Override
    void recoveredCommittables(List<GlobalCommT> committables) throws IOException {
        final List<GlobalCommT> recovered =
                globalCommitter.filterRecoveredCommittables(checkNotNull(committables));
        recoveredGlobalCommittables.addAll(recovered);
    }

    @Override
    List<GlobalCommT> prepareCommit(List<CommT> input) throws IOException {
        checkNotNull(input);
        final List<GlobalCommT> result = new ArrayList<>(recoveredGlobalCommittables);
        recoveredGlobalCommittables.clear();

        if (!input.isEmpty()) {
            result.add(globalCommitter.combine(input));
        }
        return result;
    }

    @Override
    List<GlobalCommT> commit(List<GlobalCommT> committables)
            throws IOException, InterruptedException {
        return globalCommitter.commit(checkNotNull(committables));
    }

    @Override
    public List<GlobalCommT> endOfInput() {
        endOfInput = true;
        return Collections.emptyList();
    }

    @Override
    public void close() throws Exception {
        globalCommitter.close();
        super.close();
    }

    @Override
    public Collection<GlobalCommT> notifyCheckpointCompleted(long checkpointId)
            throws IOException, InterruptedException {
        super.notifyCheckpointCompleted(checkpointId);
        commitUpTo(checkpointId);
        if (endOfInput) {
            globalCommitter.endOfInput();
        }
        return Collections.emptyList();
    }
}
