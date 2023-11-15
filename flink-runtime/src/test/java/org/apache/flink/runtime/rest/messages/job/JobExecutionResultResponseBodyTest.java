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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.rest.messages.RestResponseMarshallingTestBase;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.util.OptionalFailure;
import org.apache.flink.util.SerializedThrowable;
import org.apache.flink.util.SerializedValue;

import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link JobExecutionResultResponseBody}. */
@ExtendWith(ParameterizedTestExtension.class)
public class JobExecutionResultResponseBodyTest
        extends RestResponseMarshallingTestBase<JobExecutionResultResponseBody> {

    private static final JobID TEST_JOB_ID = new JobID();

    private static final long TEST_NET_RUNTIME = Long.MAX_VALUE;

    private static final byte[] TEST_ACCUMULATOR_VALUE = {1, 2, 3, 4, 5};

    private static final String TEST_ACCUMULATOR_NAME = "test";

    private static final Map<String, SerializedValue<OptionalFailure<Object>>> TEST_ACCUMULATORS =
            Collections.singletonMap(
                    TEST_ACCUMULATOR_NAME, SerializedValue.fromBytes(TEST_ACCUMULATOR_VALUE));

    @Parameters
    public static Collection<Object[]> data() throws IOException {
        return Arrays.asList(
                new Object[][] {
                    {
                        JobExecutionResultResponseBody.created(
                                new JobResult.Builder()
                                        .jobId(TEST_JOB_ID)
                                        .applicationStatus(ApplicationStatus.SUCCEEDED)
                                        .netRuntime(TEST_NET_RUNTIME)
                                        .accumulatorResults(TEST_ACCUMULATORS)
                                        .serializedThrowable(
                                                new SerializedThrowable(
                                                        new RuntimeException("expected")))
                                        .build())
                    },
                    {
                        JobExecutionResultResponseBody.created(
                                new JobResult.Builder()
                                        .jobId(TEST_JOB_ID)
                                        .applicationStatus(ApplicationStatus.FAILED)
                                        .netRuntime(TEST_NET_RUNTIME)
                                        .accumulatorResults(TEST_ACCUMULATORS)
                                        .build())
                    },
                    {JobExecutionResultResponseBody.inProgress()}
                });
    }

    private final JobExecutionResultResponseBody jobExecutionResultResponseBody;

    public JobExecutionResultResponseBodyTest(
            final JobExecutionResultResponseBody jobExecutionResultResponseBody) {
        this.jobExecutionResultResponseBody = jobExecutionResultResponseBody;
    }

    @Override
    protected Class<JobExecutionResultResponseBody> getTestResponseClass() {
        return JobExecutionResultResponseBody.class;
    }

    @Override
    protected JobExecutionResultResponseBody getTestResponseInstance() throws Exception {
        return jobExecutionResultResponseBody;
    }

    @Override
    protected void assertOriginalEqualsToUnmarshalled(
            final JobExecutionResultResponseBody expected,
            final JobExecutionResultResponseBody actual) {

        assertThat(actual.getStatus()).isEqualTo(actual.getStatus());

        final JobResult expectedJobExecutionResult = expected.getJobExecutionResult();
        final JobResult actualJobExecutionResult = actual.getJobExecutionResult();

        if (expectedJobExecutionResult != null) {
            assertThat(actualJobExecutionResult).isNotNull();

            assertThat(actualJobExecutionResult.getJobId())
                    .isEqualTo(expectedJobExecutionResult.getJobId());
            assertThat(actualJobExecutionResult.getApplicationStatus())
                    .isEqualTo(expectedJobExecutionResult.getApplicationStatus());
            assertThat(actualJobExecutionResult.getNetRuntime())
                    .isEqualTo(expectedJobExecutionResult.getNetRuntime());
            assertThat(actualJobExecutionResult.getAccumulatorResults())
                    .isEqualTo(expectedJobExecutionResult.getAccumulatorResults());

            final Optional<SerializedThrowable> expectedFailureCauseOptional =
                    expectedJobExecutionResult.getSerializedThrowable();
            expectedFailureCauseOptional.ifPresent(
                    expectedFailureCause -> {
                        final SerializedThrowable actualFailureCause =
                                actualJobExecutionResult
                                        .getSerializedThrowable()
                                        .orElseThrow(
                                                () ->
                                                        new AssertionError(
                                                                "actualFailureCause is not available"));
                        assertThat(actualFailureCause.getFullStringifiedStackTrace())
                                .isEqualTo(expectedFailureCause.getFullStringifiedStackTrace());
                        assertThat(actualFailureCause.getOriginalErrorClassName())
                                .isEqualTo(expectedFailureCause.getOriginalErrorClassName());
                        assertThat(expectedFailureCause.getSerializedException())
                                .isEqualTo(actualFailureCause.getSerializedException());
                    });

            if (expectedJobExecutionResult.getAccumulatorResults() != null) {
                assertThat(actualJobExecutionResult.getAccumulatorResults()).isNotNull();
                assertThat(
                                actualJobExecutionResult
                                        .getAccumulatorResults()
                                        .get(TEST_ACCUMULATOR_NAME)
                                        .getByteArray())
                        .isEqualTo(
                                expectedJobExecutionResult
                                        .getAccumulatorResults()
                                        .get(TEST_ACCUMULATOR_NAME)
                                        .getByteArray());
            }
        }
    }
}
