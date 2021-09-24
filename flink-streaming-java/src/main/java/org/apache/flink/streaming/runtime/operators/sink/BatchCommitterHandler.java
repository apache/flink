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

import org.apache.flink.api.connector.sink.Committer;

import java.io.IOException;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Runtime {@link org.apache.flink.streaming.api.operators.StreamOperator} for executing {@link
 * Committer} in the batch execution mode.
 *
 * @param <InputT> The committable type of the {@link Committer}.
 */
final class BatchCommitterHandler<InputT, OutputT>
        extends AbstractCommitterHandler<InputT, OutputT, InputT> {

    /** Responsible for committing the committable to the external system. */
    private final Committer<InputT> committer;

    /**
     * The committer that is chained to this committer. It's either {@link
     * GlobalBatchCommitterHandler} or {@link NoopCommitterHandler}.
     */
    private final CommitterHandler<InputT, OutputT> chainedHandler;

    public BatchCommitterHandler(
            Committer<InputT> committer, CommitterHandler<InputT, OutputT> chainedHandler) {
        this.committer = checkNotNull(committer);
        this.chainedHandler = chainedHandler;
    }

    @Override
    public List<OutputT> processCommittables(List<InputT> committables) {
        super.processCommittables(committables);
        return chainedHandler.processCommittables(committables);
    }

    @Override
    public boolean needsRetry() {
        return super.needsRetry() || chainedHandler.needsRetry();
    }

    @Override
    protected void retry(List<InputT> recoveredCommittables)
            throws IOException, InterruptedException {
        if (!recoveredCommittables.isEmpty()) {
            recoveredCommittables(committer.commit(recoveredCommittables));
        }
        chainedHandler.retry();
    }

    @Override
    public List<OutputT> endOfInput() throws IOException, InterruptedException {
        List<InputT> allCommittables = pollCommittables();
        if (!allCommittables.isEmpty()) {
            recoveredCommittables(committer.commit(allCommittables));
        }
        return chainedHandler.endOfInput();
    }

    @Override
    public void close() throws Exception {
        committer.close();
        chainedHandler.close();
        super.close();
    }
}
