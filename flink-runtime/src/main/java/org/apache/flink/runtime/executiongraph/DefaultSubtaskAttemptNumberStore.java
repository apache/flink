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

import org.apache.flink.util.Preconditions;

import java.util.List;

/** Simple container for subtask attempt counts backed by a list. */
public class DefaultSubtaskAttemptNumberStore implements SubtaskAttemptNumberStore {
    private final List<Integer> attemptCounts;

    public DefaultSubtaskAttemptNumberStore(List<Integer> attemptCounts) {
        this.attemptCounts = attemptCounts;
    }

    @Override
    public int getAttemptCount(int subtaskIndex) {
        Preconditions.checkArgument(subtaskIndex >= 0);
        if (subtaskIndex >= attemptCounts.size()) {
            return 0;
        }
        return attemptCounts.get(subtaskIndex);
    }
}
