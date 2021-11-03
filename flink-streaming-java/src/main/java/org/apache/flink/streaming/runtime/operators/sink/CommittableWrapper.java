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

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

class CommittableWrapper<CommT> implements SinkMessage {
    private final CommT committable;
    private final int subtaskId;
    private final long checkpointId;
    private final int committableIndex;

    public CommittableWrapper(
            CommT committable, int subtaskId, long checkpointId, int committableIndex) {
        this.committable = checkNotNull(committable);
        this.subtaskId = subtaskId;
        this.checkpointId = checkpointId;
        this.committableIndex = committableIndex;
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

    public int getCommittableIndex() {
        return committableIndex;
    }

    static <StateT> MatchResult<CommittableWrapper<StateT>> match(
            List<CommittableWrapper<StateT>> committableWrappers, List<StateT> innerCommittables) {
        // Assume that (Global)Committer#commit does not create a new instance for returned
        // committables. This assumption is documented in the respective JavaDoc.
        Set<StateT> lookup =
                Collections.newSetFromMap(new IdentityHashMap<>(committableWrappers.size()));
        lookup.addAll(innerCommittables);

        Map<Boolean, List<CommittableWrapper<StateT>>> matched =
                committableWrappers.stream()
                        .collect(Collectors.groupingBy(c -> lookup.contains(c.getCommittable())));
        return new MatchResult<>(
                matched.getOrDefault(true, Collections.emptyList()),
                matched.getOrDefault(false, Collections.emptyList()));
    }

    static <CommT> List<CommT> unwrap(List<CommittableWrapper<CommT>> committables) {
        return committables.stream()
                .map(CommittableWrapper::getCommittable)
                .collect(Collectors.toList());
    }

    static class MatchResult<T> {
        private final List<T> matched;
        private final List<T> unmatched;

        MatchResult(List<T> matched, List<T> unmatched) {
            this.unmatched = checkNotNull(unmatched);
            this.matched = checkNotNull(matched);
        }

        public List<T> getMatched() {
            return matched;
        }

        public List<T> getUnmatched() {
            return unmatched;
        }
    }
}
