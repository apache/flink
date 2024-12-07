/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.util.FlinkException;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link JobManagerRunnerResult}. */
class JobManagerRunnerResultTest {

    private final ExecutionGraphInfo executionGraphInfo =
            new ExecutionGraphInfo(new ArchivedExecutionGraphBuilder().build());
    private final FlinkException testException = new FlinkException("test exception");

    @Test
    void testSuccessfulJobManagerResult() {
        final JobManagerRunnerResult jobManagerRunnerResult =
                JobManagerRunnerResult.forSuccess(executionGraphInfo);

        assertThat(jobManagerRunnerResult.isSuccess()).isTrue();
        assertThat(jobManagerRunnerResult.isInitializationFailure()).isFalse();
    }

    @Test
    void testInitializationFailureJobManagerResult() {
        final JobManagerRunnerResult jobManagerRunnerResult =
                JobManagerRunnerResult.forInitializationFailure(executionGraphInfo, testException);

        assertThat(jobManagerRunnerResult.isInitializationFailure()).isTrue();
        assertThat(jobManagerRunnerResult.isSuccess()).isFalse();
    }

    @Test
    void testGetArchivedExecutionGraphFromSuccessfulJobManagerResult() {
        final JobManagerRunnerResult jobManagerRunnerResult =
                JobManagerRunnerResult.forSuccess(executionGraphInfo);

        assertThat(jobManagerRunnerResult.getExecutionGraphInfo()).isEqualTo(executionGraphInfo);
    }

    @Test
    void testGetInitializationFailureFromFailedJobManagerResult() {
        final JobManagerRunnerResult jobManagerRunnerResult =
                JobManagerRunnerResult.forInitializationFailure(executionGraphInfo, testException);

        assertThat(jobManagerRunnerResult.getInitializationFailure()).isEqualTo(testException);
    }

    @Test
    void testGetInitializationFailureFromSuccessfulJobManagerResult() {
        final JobManagerRunnerResult jobManagerRunnerResult =
                JobManagerRunnerResult.forSuccess(executionGraphInfo);

        assertThatThrownBy(jobManagerRunnerResult::getInitializationFailure)
                .isInstanceOf(IllegalStateException.class);
    }
}
