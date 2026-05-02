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

package org.apache.flink.api.connector.sink2;

import org.apache.flink.annotation.PublicEvolving;

import java.io.IOException;

/**
 * A {@link Sink} for exactly-once semantics using a two-phase commit protocol. The {@link Sink}
 * consists of a {@link SinkWriter} that performs the precommits and a {@link Committer} that
 * actually commits the data. To facilitate the separation the {@link SinkWriter} creates
 * <i>committables</i> on checkpoint or end of input and then sends it to the {@link Committer}.
 *
 * <p>This interface is retained as a deprecated bridge for connectors compiled against Flink 1.x
 * that reference this type. It was originally removed in Flink 2.0 (FLINK-36245) but restored to
 * ease migration, as the connector ecosystem had not fully migrated to the mixin pattern introduced
 * in FLIP-372.
 *
 * <p>New connector implementations should directly implement {@link Sink} and {@link
 * SupportsCommitter} instead.
 *
 * @param <InputT> The type of the sink's input
 * @param <CommT> The type of the committables.
 * @deprecated Implement {@link Sink} and {@link SupportsCommitter} instead. This interface exists
 *     solely as a binary compatibility bridge for connectors compiled against Flink 1.x and will be
 *     removed in a future release.
 */
@PublicEvolving
@Deprecated
public interface TwoPhaseCommittingSink<InputT, CommT>
        extends Sink<InputT>, SupportsCommitter<CommT> {

    /**
     * Creates a {@link Committer} that permanently makes the previously written data visible
     * through {@link Committer#commit(java.util.Collection)}.
     *
     * @return A committer for the two-phase commit protocol.
     * @throws IOException for any failure during creation.
     * @deprecated Please use {@link #createCommitter(CommitterInitContext)}.
     */
    @Deprecated
    default Committer<CommT> createCommitter() throws IOException {
        throw new UnsupportedOperationException(
                "Override either createCommitter() or createCommitter(CommitterInitContext)");
    }

    /**
     * Creates a {@link Committer}. Delegates to the no-arg {@link #createCommitter()} by default
     * to support connectors compiled against Flink 1.x that only overrode the no-arg variant.
     */
    @Override
    default Committer<CommT> createCommitter(CommitterInitContext context) throws IOException {
        return createCommitter();
    }

    /**
     * A {@link SinkWriter} that performs the first part of a two-phase commit protocol.
     *
     * @deprecated Use {@link CommittingSinkWriter} directly.
     */
    @PublicEvolving
    @Deprecated
    interface PrecommittingSinkWriter<InputT, CommT>
            extends CommittingSinkWriter<InputT, CommT> {}
}
