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

import org.apache.flink.api.common.JobStatus;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for the FLINK-39704 fix that exercise a real {@link
 * JobMasterServiceLeadershipRunner} together with a real {@link DefaultJobMasterServiceProcess}
 * (see {@link JobMasterServiceLeadershipRunnerTestHarness}).
 *
 * <p>They reproduce, deterministically, the race between a job reaching a globally terminal state
 * and its leadership being revoked, covering both event orderings. In every case the terminal
 * result must survive the revocation rather than being replaced by a suspended/not-finished result.
 */
class JobMasterServiceLeadershipRunnerITCase {

    /**
     * The result is observed while still leader, but leadership is lost before the runner manages
     * to forward it. The cached result must still be flushed on close.
     */
    @ParameterizedTest
    @EnumSource(
            value = JobStatus.class,
            names = {"FINISHED", "FAILED", "CANCELED"})
    void globallyTerminalResultObservedBeforeRevocationIsPreserved(JobStatus terminalState)
            throws Exception {
        try (JobMasterServiceLeadershipRunnerTestHarness harness =
                new JobMasterServiceLeadershipRunnerTestHarness()) {
            harness.start();
            harness.grantLeadership();

            // Hold the forwarding leadership check so revocation can win the race against it.
            harness.armDelayedForwardingCheck();
            harness.reachGloballyTerminalState(terminalState);
            harness.revokeLeadership();
            harness.releaseDelayedForwardingCheck();
            harness.completeCurrentServiceTermination();

            harness.closeRunnerAsync().get();

            assertThat(harness.getResultJobStatus()).isEqualTo(terminalState);
        }
    }

    /**
     * Leadership is revoked first, so the process is already closing when the result is observed.
     * The process defers its {@link JobNotFinishedException} long enough for the terminal result to
     * win, and the runner then flushes it on close.
     */
    @ParameterizedTest
    @EnumSource(
            value = JobStatus.class,
            names = {"FINISHED", "FAILED", "CANCELED"})
    void globallyTerminalResultObservedDuringCloseIsPreserved(JobStatus terminalState)
            throws Exception {
        try (JobMasterServiceLeadershipRunnerTestHarness harness =
                new JobMasterServiceLeadershipRunnerTestHarness()) {
            harness.start();
            harness.grantLeadership();

            harness.revokeLeadership();
            harness.reachGloballyTerminalState(terminalState);
            harness.completeCurrentServiceTermination();

            harness.closeRunnerAsync().get();

            assertThat(harness.getResultJobStatus()).isEqualTo(terminalState);
        }
    }

    /** Without a globally terminal result, closing after revocation suspends the job as before. */
    @Test
    void closeWithoutGloballyTerminalResultSuspendsJob() throws Exception {
        try (JobMasterServiceLeadershipRunnerTestHarness harness =
                new JobMasterServiceLeadershipRunnerTestHarness()) {
            harness.start();
            harness.grantLeadership();

            harness.revokeLeadership();
            harness.completeCurrentServiceTermination();

            harness.closeRunnerAsync().get();

            assertThat(harness.getResultJobStatus()).isEqualTo(JobStatus.SUSPENDED);
        }
    }
}
