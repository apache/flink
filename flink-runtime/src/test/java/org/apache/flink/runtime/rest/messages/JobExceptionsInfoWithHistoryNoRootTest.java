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

import org.apache.flink.testutils.junit.extensions.parameterized.NoOpTestExtension;

import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Tests that the {@link JobExceptionsInfoWithHistory} with no root exception can be marshalled and
 * unmarshalled.
 */
@ExtendWith(NoOpTestExtension.class)
class JobExceptionsInfoWithHistoryNoRootTest
        extends RestResponseMarshallingTestBase<JobExceptionsInfoWithHistory> {
    @Override
    protected Class<JobExceptionsInfoWithHistory> getTestResponseClass() {
        return JobExceptionsInfoWithHistory.class;
    }

    @Override
    protected JobExceptionsInfoWithHistory getTestResponseInstance() throws Exception {
        List<JobExceptionsInfo.ExecutionExceptionInfo> executionTaskExceptionInfoList =
                new ArrayList<>();
        executionTaskExceptionInfoList.add(
                new JobExceptionsInfo.ExecutionExceptionInfo(
                        "exception1",
                        "task1",
                        "location1",
                        System.currentTimeMillis(),
                        "taskManagerId1"));
        executionTaskExceptionInfoList.add(
                new JobExceptionsInfo.ExecutionExceptionInfo(
                        "exception2",
                        "task2",
                        "location2",
                        System.currentTimeMillis(),
                        "taskManagerId2"));
        return new JobExceptionsInfoWithHistory(
                null,
                null,
                executionTaskExceptionInfoList,
                false,
                new JobExceptionsInfoWithHistory.JobExceptionHistory(
                        Arrays.asList(
                                new JobExceptionsInfoWithHistory.RootExceptionInfo(
                                        "global failure #0",
                                        "stacktrace #0",
                                        0L,
                                        Collections.emptyMap(),
                                        Collections.singletonList(
                                                new JobExceptionsInfoWithHistory.ExceptionInfo(
                                                        "local task failure #2",
                                                        "stacktrace #2",
                                                        2L,
                                                        Collections.emptyMap(),
                                                        "task name #2",
                                                        "location #2",
                                                        "taskManagerId #2"))),
                                new JobExceptionsInfoWithHistory.RootExceptionInfo(
                                        "local task failure #1",
                                        "stacktrace #1",
                                        1L,
                                        Collections.emptyMap(),
                                        "task name",
                                        "location",
                                        "taskManagerId",
                                        Collections.emptyList())),
                        false));
    }
}
