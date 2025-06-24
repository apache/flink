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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.client.JobCancellationException;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.executiongraph.ErrorInfo;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.SerializedThrowable;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link JobResult}. */
class JobResultTest {

    @Test
    void testNetRuntimeMandatory() {
        assertThatThrownBy(() -> new JobResult.Builder().jobId(new JobID()).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("netRuntime must be greater than or equals 0");
    }

    @Test
    void testIsNotSuccess() {
        final JobResult jobResult =
                new JobResult.Builder()
                        .jobId(new JobID())
                        .serializedThrowable(new SerializedThrowable(new RuntimeException()))
                        .netRuntime(Long.MAX_VALUE)
                        .build();

        assertThat(jobResult.isSuccess()).isFalse();
    }

    @Test
    void testIsSuccess() {
        final JobResult jobResult =
                new JobResult.Builder().jobId(new JobID()).netRuntime(Long.MAX_VALUE).build();

        assertThat(jobResult.isSuccess()).isTrue();
    }

    @Test
    void testCancelledJobIsFailureResult() {
        final JobResult jobResult =
                JobResult.createFrom(
                        new ArchivedExecutionGraphBuilder()
                                .setJobID(new JobID())
                                .setState(JobStatus.CANCELED)
                                .build());

        assertThat(jobResult.isSuccess()).isFalse();
    }

    @Test
    void testFailedJobIsFailureResult() {
        final JobResult jobResult =
                JobResult.createFrom(
                        new ArchivedExecutionGraphBuilder()
                                .setJobID(new JobID())
                                .setState(JobStatus.FAILED)
                                .setFailureCause(
                                        new ErrorInfo(new FlinkException("Test exception"), 42L))
                                .build());

        assertThat(jobResult.isSuccess()).isFalse();
    }

    @Test
    void testCancelledJobThrowsJobCancellationException() {
        final FlinkException cause = new FlinkException("Test exception");
        final JobResult jobResult =
                JobResult.createFrom(
                        new ArchivedExecutionGraphBuilder()
                                .setJobID(new JobID())
                                .setState(JobStatus.CANCELED)
                                .setFailureCause(new ErrorInfo(cause, 42L))
                                .build());

        assertThatThrownBy(
                        () -> {
                            jobResult.toJobExecutionResult(getClass().getClassLoader());
                        })
                .isInstanceOf(JobCancellationException.class)
                .hasNoCause();
    }

    @Test
    void testFailedJobThrowsJobExecutionException() {
        final FlinkException cause = new FlinkException("Test exception");
        final JobResult jobResult =
                JobResult.createFrom(
                        new ArchivedExecutionGraphBuilder()
                                .setJobID(new JobID())
                                .setState(JobStatus.FAILED)
                                .setFailureCause(new ErrorInfo(cause, 42L))
                                .build());

        assertThatThrownBy(() -> jobResult.toJobExecutionResult(getClass().getClassLoader()))
                .isInstanceOf(JobExecutionException.class)
                .cause()
                .isEqualTo(cause);
    }

    @Test
    void testFailureResultRequiresFailureCause() {
        assertThatThrownBy(
                        () ->
                                JobResult.createFrom(
                                        new ArchivedExecutionGraphBuilder()
                                                .setJobID(new JobID())
                                                .setState(JobStatus.FAILED)
                                                .build()))
                .isInstanceOf(NullPointerException.class);
    }
}
