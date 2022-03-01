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
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;

import javax.annotation.Nullable;

import java.util.OptionalLong;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Provides metadata. The exposed exchange type between {@link
 * TwoPhaseCommittingSink.PrecommittingSinkWriter} and {@link Committer}.
 */
@Experimental
public class CommittableWithLineage<CommT> implements CommittableMessage<CommT> {
    private final CommT committable;
    @Nullable private final Long checkpointId;
    private final int subtaskId;

    public CommittableWithLineage(CommT committable, @Nullable Long checkpointId, int subtaskId) {
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

    public OptionalLong getCheckpointId() {
        return checkpointId == null ? OptionalLong.empty() : OptionalLong.of(checkpointId);
    }
}
