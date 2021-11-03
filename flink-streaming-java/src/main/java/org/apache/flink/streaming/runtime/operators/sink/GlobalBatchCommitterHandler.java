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

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.apache.flink.streaming.runtime.operators.sink.CommittableWrapper.unwrap;
import static org.apache.flink.streaming.runtime.operators.sink.SinkMessage.EOF;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link CommitterHandler} for executing {@link GlobalCommitter} in the batch execution mode.
 *
 * <p>This handler never emits output downstream.
 *
 * @param <CommT> The committable type of the {@link Committer}, which is the input and output type.
 * @param <GlobalCommT> The committable type of the {@link GlobalCommitter}
 */
@Internal
public final class GlobalBatchCommitterHandler<CommT, GlobalCommT>
        extends AbstractCommitterHandler<CommT, GlobalCommT> {

    /**
     * Aggregate committables to global committables and commit the global committables to the
     * external system.
     */
    private final GlobalCommitter<CommT, GlobalCommT> globalCommitter;

    public GlobalBatchCommitterHandler(GlobalCommitter<CommT, GlobalCommT> globalCommitter) {
        this.globalCommitter = checkNotNull(globalCommitter);
    }

    @Override
    public Collection<CommittableWrapper<CommT>> endOfInput()
            throws IOException, InterruptedException {
        List<CommittableWrapper<CommT>> allCommittables = pollCommittables();
        if (!allCommittables.isEmpty()) {
            final GlobalCommT globalCommittable = globalCommitter.combine(unwrap(allCommittables));
            commit(singletonList(new CommittableWrapper<>(globalCommittable, 0, 0, EOF)));
        }
        globalCommitter.endOfInput();
        return Collections.emptyList();
    }

    @Override
    List<GlobalCommT> commitInternal(List<GlobalCommT> committables)
            throws IOException, InterruptedException {
        return globalCommitter.commit(checkNotNull(committables));
    }

    @Override
    public void close() throws Exception {
        globalCommitter.close();
        super.close();
    }

    /** The serializable factory of the handler. */
    public static class Factory<CommT, GlobalCommT>
            implements CommitterHandler.Factory<Sink<?, CommT, ?, GlobalCommT>, CommT> {
        @Override
        public CommitterHandler<CommT> create(Sink<?, CommT, ?, GlobalCommT> sink)
                throws IOException {
            return new GlobalBatchCommitterHandler<>(
                    checkCommitterPresent(sink.createGlobalCommitter(), true));
        }
    }
}
