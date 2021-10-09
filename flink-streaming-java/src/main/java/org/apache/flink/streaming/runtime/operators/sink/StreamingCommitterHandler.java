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
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link CommitterHandler} for executing {@link Committer} in the streaming execution mode.
 *
 * @param <CommT> The committable type of the {@link Committer}.
 */
final class StreamingCommitterHandler<CommT>
        extends AbstractStreamingCommitterHandler<CommT, CommT> {

    /** Responsible for committing the committable to the external system. * */
    private final Committer<CommT> committer;

    public StreamingCommitterHandler(
            Committer<CommT> committer, SimpleVersionedSerializer<CommT> committableSerializer) {
        super(committableSerializer);
        this.committer = checkNotNull(committer);
    }

    @Override
    List<CommT> prepareCommit(List<CommT> input) {
        return prependRecoveredCommittables(checkNotNull(input));
    }

    @Override
    List<CommT> commit(List<CommT> committables) throws IOException, InterruptedException {
        return committer.commit(checkNotNull(committables));
    }

    @Override
    public void close() throws Exception {
        committer.close();
        super.close();
    }

    @Override
    public Collection<CommT> notifyCheckpointCompleted(long checkpointId)
            throws IOException, InterruptedException {
        return commitUpTo(checkpointId);
    }
}
