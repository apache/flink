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

package org.apache.flink.runtime.clusterframework;

import org.apache.flink.api.common.JobStatus;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link ApplicationStatus}. */
class ApplicationStatusTest {

    private static final int SUCCESS_EXIT_CODE = 0;

    @Test
    void succeededStatusMapsToSuccessExitCode() {
        int exitCode = ApplicationStatus.SUCCEEDED.processExitCode();
        assertThat(exitCode).isEqualTo(SUCCESS_EXIT_CODE);
    }

    @Test
    void cancelledStatusMapsToSuccessExitCode() {
        int exitCode = ApplicationStatus.CANCELED.processExitCode();
        assertThat(exitCode).isEqualTo(SUCCESS_EXIT_CODE);
    }

    @Test
    void notSucceededNorCancelledStatusMapsToNonSuccessExitCode() {
        Iterable<Integer> exitCodes = exitCodes(notSucceededNorCancelledStatus());
        assertThat(exitCodes).doesNotContain(SUCCESS_EXIT_CODE);
    }

    @Test
    void testJobStatusFromSuccessApplicationStatus() {
        assertThat(ApplicationStatus.SUCCEEDED.deriveJobStatus()).isEqualTo(JobStatus.FINISHED);
    }

    @Test
    void testJobStatusFromFailedApplicationStatus() {
        assertThat(ApplicationStatus.FAILED.deriveJobStatus()).isEqualTo(JobStatus.FAILED);
    }

    @Test
    void testJobStatusFromCancelledApplicationStatus() {
        assertThat(ApplicationStatus.CANCELED.deriveJobStatus()).isEqualTo(JobStatus.CANCELED);
    }

    @Test
    void testJobStatusFailsFromUnknownApplicationStatuses() {
        assertThatThrownBy(ApplicationStatus.UNKNOWN::deriveJobStatus)
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testSuccessApplicationStatusFromJobStatus() {
        assertThat(ApplicationStatus.fromJobStatus(JobStatus.FINISHED))
                .isEqualTo(ApplicationStatus.SUCCEEDED);
    }

    @Test
    void testFailedApplicationStatusFromJobStatus() {
        assertThat(ApplicationStatus.fromJobStatus(JobStatus.FAILED))
                .isEqualTo(ApplicationStatus.FAILED);
    }

    @Test
    void testCancelledApplicationStatusFromJobStatus() {
        assertThat(ApplicationStatus.fromJobStatus(JobStatus.CANCELED))
                .isEqualTo(ApplicationStatus.CANCELED);
    }

    @ParameterizedTest
    @EnumSource(
            value = JobStatus.class,
            names = {
                "INITIALIZING",
                "CREATED",
                "RUNNING",
                "FAILING",
                "CANCELLING",
                "RESTARTING",
                "SUSPENDED",
                "RECONCILING"
            })
    public void testUnknownApplicationStatusFromJobStatus(JobStatus jobStatus) {
        assertThat(ApplicationStatus.fromJobStatus(jobStatus)).isEqualTo(ApplicationStatus.UNKNOWN);
    }

    @Test
    void testUnknownApplicationStatusForMissingJobStatus() {
        assertThat(ApplicationStatus.fromJobStatus(null)).isEqualTo(ApplicationStatus.UNKNOWN);
    }

    private static Iterable<Integer> exitCodes(Iterable<ApplicationStatus> statuses) {
        return StreamSupport.stream(statuses.spliterator(), false)
                .map(ApplicationStatus::processExitCode)
                .collect(Collectors.toList());
    }

    private static Iterable<ApplicationStatus> notSucceededNorCancelledStatus() {
        return Arrays.stream(ApplicationStatus.values())
                .filter(ApplicationStatusTest::isNotSucceededNorCancelled)
                .collect(Collectors.toList());
    }

    private static boolean isNotSucceededNorCancelled(ApplicationStatus status) {
        return status != ApplicationStatus.SUCCEEDED && status != ApplicationStatus.CANCELED;
    }
}
