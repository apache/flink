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

    private boolean endOfInput;

    public GlobalStreamingCommitterHandler(
            GlobalCommitter<CommT, GlobalCommT> globalCommitter,
            SimpleVersionedSerializer<GlobalCommT> committableSerializer) {
        super(committableSerializer);
        this.globalCommitter = checkNotNull(globalCommitter);

        this.endOfInput = false;
    }

    @Override
    protected void recoveredCommittables(List<GlobalCommT> committables) throws IOException {
        super.recoveredCommittables(
                globalCommitter.filterRecoveredCommittables(checkNotNull(committables)));
    }

    @Override
    List<GlobalCommT> prepareCommit(List<CommT> input) throws IOException {
        return prependRecoveredCommittables(
                input.isEmpty()
                        ? Collections.emptyList()
                        : Collections.singletonList(globalCommitter.combine(input)));
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
