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

package org.apache.flink.runtime.rest.messages.job;

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.messages.RestResponseMarshallingTestBase;
import org.apache.flink.testutils.junit.extensions.parameterized.NoOpTestExtension;

import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createExecutionAttemptId;

/** Tests (un)marshalling of the {@link SubtaskExecutionAttemptAccumulatorsInfo}. */
@ExtendWith(NoOpTestExtension.class)
class SubtaskExecutionAttemptAccumulatorsInfoTest
        extends RestResponseMarshallingTestBase<SubtaskExecutionAttemptAccumulatorsInfo> {

    @Override
    protected Class<SubtaskExecutionAttemptAccumulatorsInfo> getTestResponseClass() {
        return SubtaskExecutionAttemptAccumulatorsInfo.class;
    }

    @Override
    protected SubtaskExecutionAttemptAccumulatorsInfo getTestResponseInstance() throws Exception {

        final List<UserAccumulator> userAccumulatorList = new ArrayList<>();

        userAccumulatorList.add(new UserAccumulator("name1", "type1", "value1"));
        userAccumulatorList.add(new UserAccumulator("name2", "type1", "value1"));
        userAccumulatorList.add(new UserAccumulator("name3", "type2", "value3"));

        final ExecutionAttemptID executionAttemptId =
                createExecutionAttemptId(new JobVertexID(), 1, 2);
        return new SubtaskExecutionAttemptAccumulatorsInfo(
                executionAttemptId.getSubtaskIndex(),
                executionAttemptId.getAttemptNumber(),
                executionAttemptId.toString(),
                userAccumulatorList);
    }
}
