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

package org.apache.flink.runtime.scheduler.adaptive.timeline;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Test for recording rescale history by {@link DefaultRescaleTimeline} or {@link
 * RescaleTimeline.NoOpRescaleTimeline}.
 */
class RescaleRecordingLogicITCase {
    static final String DISABLED_DESCRIPTION =
            "TODO: Blocked by FLINK-38343, the ITCases need the SchedulerNG#requstJob() to get the rescale history.";

    // Tests for rescale trigger causes.
    @Disabled(DISABLED_DESCRIPTION)
    @Test
    void testRecordingRescaleTriggerredByInitialSchedule() {}

    @Disabled(DISABLED_DESCRIPTION)
    @Test
    void testRecordingRescaleTriggerredByUpdateRequirement() {}

    @Disabled(DISABLED_DESCRIPTION)
    @Test
    void testRecordingRescaleTriggerredByNewResourceAvailable() {}

    @Disabled(DISABLED_DESCRIPTION)
    @Test
    void testRecordingRescaleTriggerredByRecoverableFailover() {}

    // End of tests for rescale trigger causes.

    // Start of tests for rescale terminated reasons and terminal state.

    @Disabled(DISABLED_DESCRIPTION)
    @Test
    void testRecordingRescaleTerminatedBySucceeded() {}

    @Disabled(DISABLED_DESCRIPTION)
    @Test
    void testRecordingRescaleTerminatedByJobFinished() {}

    @Disabled(DISABLED_DESCRIPTION)
    @Test
    void testRecordingRescaleTerminatedByJobCancelled() {}

    @Disabled(DISABLED_DESCRIPTION)
    @Test
    void testRecordingRescaleTerminatedByJobFailed() {}

    @Disabled(DISABLED_DESCRIPTION)
    @Test
    void testRecordingRescaleTerminatedByNoResourcesOrParallelismsChange() {}

    @Disabled(DISABLED_DESCRIPTION)
    @Test
    void testRecordingRescaleTerminatedByResourcesNotEnoughException() {}

    @Disabled(DISABLED_DESCRIPTION)
    @Test
    void testRecordingRescaleTerminatedByResourceRequirementsUpdated() {}

    @Disabled(DISABLED_DESCRIPTION)
    @Test
    void testRecordingRescaleTerminatedByJobRestarting() {}

    // End of tests for rescale terminated reasons and terminal state.

    @Disabled(DISABLED_DESCRIPTION)
    @Test
    void testRecordingRescaleWithoutPreRescaleInfo() {}

    @Disabled(DISABLED_DESCRIPTION)
    @Test
    void testRecordingRescaleWithPreRescaleInfo() {}

    @Disabled(DISABLED_DESCRIPTION)
    @Test
    void testUseNonTerminatedRescaleToRecordMergingWithNewRecoverableFailureTriggerCause() {}

    @Disabled(DISABLED_DESCRIPTION)
    @Test
    void testRecordingInProgressRescale() {}
}
