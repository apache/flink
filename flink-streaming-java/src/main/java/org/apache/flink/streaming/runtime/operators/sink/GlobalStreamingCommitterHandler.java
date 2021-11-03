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
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.streaming.runtime.operators.sink.CheckpointSummary.match;
import static org.apache.flink.streaming.runtime.operators.sink.CheckpointSummary.unwrap;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link CommitterHandler} for executing {@link GlobalCommitter} in the streaming execution mode.
 *
 * <p>This handler never emits output downstream.
 *
 * @param <CommT> The committable type of the {@link Committer}, which is the input and output type.
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
            SimpleVersionedSerializer<CommittableWrapper<GlobalCommT>> committableSerializer) {
        super(committableSerializer);
        this.globalCommitter = checkNotNull(globalCommitter);

        this.endOfInput = false;
    }

    @Override
    protected void recoveredCommittables(List<CommittableWrapper<GlobalCommT>> committables)
            throws IOException {
        CheckpointSummary.MatchResult<CommittableWrapper<GlobalCommT>> filtered =
                match(
                        committables,
                        globalCommitter.filterRecoveredCommittables(unwrap(committables)));
        super.recoveredCommittables(filtered.getMatched());
    }

    @Override
    List<CommittableWrapper<GlobalCommT>> prepareCommit(List<CommittableWrapper<CommT>> input)
            throws IOException {
        return prependRecoveredCommittables(
                input.isEmpty()
                        ? Collections.emptyList()
                        : CheckpointSummary.singletonList(globalCommitter.combine(unwrap(input))));
    }

    @Override
    List<GlobalCommT> commitInternal(List<GlobalCommT> committables)
            throws IOException, InterruptedException {
        return globalCommitter.commit(checkNotNull(committables));
    }

    @Override
    public Collection<CommittableWrapper<CommT>> endOfInput() {
        endOfInput = true;
        return Collections.emptyList();
    }

    @Override
    public void close() throws Exception {
        globalCommitter.close();
        super.close();
    }

    @Override
    public Collection<CommittableWrapper<CommT>> notifyCheckpointCompleted(long checkpointId)
            throws IOException, InterruptedException {
        super.notifyCheckpointCompleted(checkpointId);
        commitUpTo(checkpointId);
        if (endOfInput) {
            globalCommitter.endOfInput();
        }
        return Collections.emptyList();
    }

    /** The serializable factory of the handler. */
    public static class Factory<CommT, GlobalCommT>
            implements CommitterHandler.Factory<Sink<?, CommT, ?, GlobalCommT>, CommT> {
        @Override
        public CommitterHandler<CommT> create(Sink<?, CommT, ?, GlobalCommT> sink)
                throws IOException {
            return new GlobalStreamingCommitterHandler<>(
                    checkCommitterPresent(sink.createGlobalCommitter(), true),
                    new CheckpointSummary.Serializer<>(
                            checkSerializerPresent(sink.getGlobalCommittableSerializer(), true)));
        }
    }
}
