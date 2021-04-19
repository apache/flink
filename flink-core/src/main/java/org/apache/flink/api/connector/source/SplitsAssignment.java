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

package org.apache.flink.api.connector.source;

import org.apache.flink.annotation.Public;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A class containing the splits assignment to the source readers.
 *
 * <p>The assignment is always incremental. In another word, splits in the assignment are simply
 * added to the existing assignment.
 */
@Public
public final class SplitsAssignment<SplitT extends SourceSplit> {

    private final Map<Integer, List<SplitT>> assignment;

    public SplitsAssignment(Map<Integer, List<SplitT>> assignment) {
        this.assignment = assignment;
    }

    public SplitsAssignment(SplitT split, int subtask) {
        this.assignment = new HashMap<>();
        this.assignment.put(subtask, Collections.singletonList(split));
    }

    /** @return A mapping from subtask ID to their split assignment. */
    public Map<Integer, List<SplitT>> assignment() {
        return assignment;
    }

    @Override
    public String toString() {
        return assignment.toString();
    }
}
