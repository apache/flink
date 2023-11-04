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

package org.apache.flink.runtime.rest.messages;

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.testutils.junit.extensions.parameterized.NoOpTestExtension;

import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Tests that the {@link SubtasksTimesInfo} can be marshalled and unmarshalled. */
@ExtendWith(NoOpTestExtension.class)
class SubtasksTimesInfoTest extends RestResponseMarshallingTestBase<SubtasksTimesInfo> {

    @Override
    protected Class<SubtasksTimesInfo> getTestResponseClass() {
        return SubtasksTimesInfo.class;
    }

    @Override
    protected SubtasksTimesInfo getTestResponseInstance() throws Exception {
        List<SubtasksTimesInfo.SubtaskTimeInfo> subtasks = new ArrayList<>();

        Map<ExecutionState, Long> subTimeMap1 = new HashMap<>();
        subTimeMap1.put(ExecutionState.RUNNING, 1L);
        subTimeMap1.put(ExecutionState.FAILED, 2L);
        subTimeMap1.put(ExecutionState.CANCELED, 3L);
        subtasks.add(new SubtasksTimesInfo.SubtaskTimeInfo(0, "local1", 1L, subTimeMap1));

        Map<ExecutionState, Long> subTimeMap2 = new HashMap<>();
        subTimeMap2.put(ExecutionState.RUNNING, 4L);
        subTimeMap2.put(ExecutionState.FAILED, 5L);
        subTimeMap2.put(ExecutionState.CANCELED, 6L);
        subtasks.add(new SubtasksTimesInfo.SubtaskTimeInfo(1, "local2", 2L, subTimeMap2));

        Map<ExecutionState, Long> subTimeMap3 = new HashMap<>();
        subTimeMap3.put(ExecutionState.SCHEDULED, 1L);
        subTimeMap3.put(ExecutionState.FAILED, 2L);
        subTimeMap3.put(ExecutionState.CANCELING, 3L);
        subtasks.add(new SubtasksTimesInfo.SubtaskTimeInfo(2, "local3", 3L, subTimeMap3));

        return new SubtasksTimesInfo("testId", "testName", System.currentTimeMillis(), subtasks);
    }
}
