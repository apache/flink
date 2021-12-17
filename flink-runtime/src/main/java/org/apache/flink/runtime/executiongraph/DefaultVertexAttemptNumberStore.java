/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Maintains the attempt number per subtask. */
public class DefaultVertexAttemptNumberStore implements MutableVertexAttemptNumberStore {
    private final Map<JobVertexID, List<Integer>> vertexSubtaskToAttemptCounts = new HashMap<>();

    @Override
    public SubtaskAttemptNumberStore getAttemptCounts(JobVertexID vertexId) {
        return new DefaultSubtaskAttemptNumberStore(
                Collections.unmodifiableList(
                        new ArrayList<>(
                                vertexSubtaskToAttemptCounts.getOrDefault(
                                        vertexId, Collections.emptyList()))));
    }

    @Override
    public void setAttemptCount(JobVertexID jobVertexId, int subtaskIndex, int attemptNumber) {
        Preconditions.checkArgument(subtaskIndex >= 0);
        Preconditions.checkArgument(attemptNumber >= 0);

        final List<Integer> attemptCounts =
                vertexSubtaskToAttemptCounts.computeIfAbsent(
                        jobVertexId, ignored -> new ArrayList<>(32));
        while (subtaskIndex >= attemptCounts.size()) {
            attemptCounts.add(0);
        }
        attemptCounts.set(subtaskIndex, attemptNumber);
    }
}
