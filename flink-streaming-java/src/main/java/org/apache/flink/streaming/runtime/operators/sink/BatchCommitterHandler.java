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
import org.apache.flink.api.connector.sink.Sink;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Runtime {@link org.apache.flink.streaming.api.operators.StreamOperator} for executing {@link
 * Committer} in the batch execution mode.
 *
 * @param <CommT> The committable type of the {@link Committer}.
 */
@Internal
public final class BatchCommitterHandler<CommT> extends AbstractCommitterHandler<CommT, CommT> {

    /** Responsible for committing the committable to the external system. */
    private final Committer<CommT> committer;

    public BatchCommitterHandler(Committer<CommT> committer) {
        this.committer = checkNotNull(committer);
    }

    @Override
    protected Collection<CommittableWrapper<CommT>> retry(
            List<CommittableWrapper<CommT>> recoveredCommittables)
            throws IOException, InterruptedException {
        return commit(recoveredCommittables).getSuccessful();
    }

    @Override
    List<CommT> commitInternal(List<CommT> committables) throws IOException, InterruptedException {
        return committer.commit(committables);
    }

    @Override
    public Collection<CommittableWrapper<CommT>> endOfInput()
            throws IOException, InterruptedException {
        return commit(pollCommittables()).getSuccessful();
    }

    @Override
    public void close() throws Exception {
        committer.close();
        super.close();
    }

    /** The serializable factory of the handler. */
    public static class Factory<CommT>
            implements CommitterHandler.Factory<Sink<?, CommT, ?, ?>, CommT> {
        @Override
        public CommitterHandler<CommT> create(Sink<?, CommT, ?, ?> sink) throws IOException {
            return new BatchCommitterHandler<>(
                    checkCommitterPresent(sink.createCommitter(), false));
        }
    }
}
