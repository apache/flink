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

import org.apache.flink.api.connector.sink.GlobalCommitter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link CommitterHandler} for executing {@link GlobalCommitter} in the batch execution mode.
 *
 * @param <CommT> The committable type of the {@link GlobalCommitter}
 * @param <GlobalCommT> The committable type of the {@link GlobalCommitter}
 */
final class GlobalBatchCommitterHandler<CommT, GlobalCommT>
        extends AbstractCommitterHandler<CommT, GlobalCommT, GlobalCommT> {

    private static final Logger LOG = LoggerFactory.getLogger(GlobalBatchCommitterHandler.class);

    /**
     * Aggregate committables to global committables and commit the global committables to the
     * external system.
     */
    private final GlobalCommitter<CommT, GlobalCommT> globalCommitter;

    public GlobalBatchCommitterHandler(GlobalCommitter<CommT, GlobalCommT> globalCommitter) {
        this.globalCommitter = checkNotNull(globalCommitter);
    }

    @Override
    public List<GlobalCommT> endOfInput() throws IOException, InterruptedException {
        List<CommT> allCommittables = pollCommittables();
        if (!allCommittables.isEmpty()) {
            final GlobalCommT globalCommittable = globalCommitter.combine(allCommittables);
            recoveredCommittables(
                    globalCommitter.commit(Collections.singletonList(globalCommittable)));
        }
        globalCommitter.endOfInput();
        return Collections.emptyList();
    }

    @Override
    protected void retry(List<GlobalCommT> recoveredCommittables)
            throws IOException, InterruptedException {
        recoveredCommittables(globalCommitter.commit(recoveredCommittables));
    }

    @Override
    public void close() throws Exception {
        globalCommitter.close();
        super.close();
    }
}
