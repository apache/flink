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

package org.apache.flink.streaming.api.connector.sink2;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.connector.sink2.Committer;

import java.util.Objects;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Provides metadata. The exposed exchange type between {@link
 * org.apache.flink.api.connector.sink2.CommittingSinkWriter} and {@link Committer}.
 */
@Experimental
public class CommittableWithLineage<CommT> implements CommittableMessage<CommT> {
    private final CommT committable;
    private final long checkpointId;
    private final int subtaskId;

    public CommittableWithLineage(CommT committable, long checkpointId, int subtaskId) {
        this.committable = checkNotNull(committable);
        this.checkpointId = checkpointId;
        this.subtaskId = subtaskId;
    }

    public CommT getCommittable() {
        return committable;
    }

    public int getSubtaskId() {
        return subtaskId;
    }

    public long getCheckpointId() {
        return checkpointId;
    }

    public <NewCommT> CommittableWithLineage<NewCommT> map(Function<CommT, NewCommT> mapper) {
        return new CommittableWithLineage<>(mapper.apply(committable), checkpointId, subtaskId);
    }

    /** Creates a shallow copy with the given subtaskId. */
    public CommittableWithLineage<CommT> withSubtaskId(int subtaskId) {
        return new CommittableWithLineage<>(committable, checkpointId, subtaskId);
    }

    @Override
    public String toString() {
        return "CommittableWithLineage{"
                + "committable="
                + committable
                + ", checkpointId="
                + checkpointId
                + ", subtaskId="
                + subtaskId
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CommittableWithLineage<?> that = (CommittableWithLineage<?>) o;
        return checkpointId == that.checkpointId
                && subtaskId == that.subtaskId
                && Objects.equals(committable, that.committable);
    }

    @Override
    public int hashCode() {
        return Objects.hash(committable, checkpointId, subtaskId);
    }
}
