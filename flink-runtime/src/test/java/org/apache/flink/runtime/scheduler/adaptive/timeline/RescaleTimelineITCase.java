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
class RescaleTimelineITCase {
    static final String DISABLED_DESCRIPTION =
            "TODO: Blocked by FLINK-38343, the ITCases need the SchedulerNG#requstJob() to get the rescale history.";

    // Tests for rescale trigger causes.
    @Disabled(DISABLED_DESCRIPTION)
    @Test
    void testRecordRescaleForInitialScheduling() {}

    @Disabled(DISABLED_DESCRIPTION)
    @Test
    void testRecordRescaleForNewResourcesRequirements() {}

    @Disabled(DISABLED_DESCRIPTION)
    @Test
    void testRecordRescaleForNewAvailableResource() {}

    @Disabled(DISABLED_DESCRIPTION)
    @Test
    void testRecordRescaleForRecoverableFailover() {}

    // End of tests for rescale trigger causes.

    // Start of tests for rescale terminated reasons and terminal state.

    @Disabled(DISABLED_DESCRIPTION)
    @Test
    void testRescaleTerminatedBySucceeded() {}

    @Disabled(DISABLED_DESCRIPTION)
    @Test
    void testRescaleTerminatedByJobFinished() {}

    @Disabled(DISABLED_DESCRIPTION)
    @Test
    void testRescaleTerminatedByJobCancelled() {}

    @Disabled(DISABLED_DESCRIPTION)
    @Test
    void testRescaleTerminatedByJobFailed() {}

    @Disabled(DISABLED_DESCRIPTION)
    @Test
    void testRescaleTerminatedByNoResourcesOrNoParallelismsChange() {}

    @Disabled(DISABLED_DESCRIPTION)
    @Test
    void testRescaleTerminatedByResourcesNotEnoughException() {}

    @Disabled(DISABLED_DESCRIPTION)
    @Test
    void testRescaleTerminatedByResourceRequirementsUpdated() {}

    @Disabled(DISABLED_DESCRIPTION)
    @Test
    void testRescaleTerminatedByJobRestarting() {}

    // End of tests for rescale terminated reasons and terminal state.

    @Disabled(DISABLED_DESCRIPTION)
    @Test
    void testRecordRescaleWithoutPreRescaleInfo() {}

    @Disabled(DISABLED_DESCRIPTION)
    @Test
    void testRecordRescaleWithPreRescaleInfo() {}

    @Disabled(DISABLED_DESCRIPTION)
    @Test
    void testRecordNonTerminatedRescaleMergingWithNewRecoverableFailureTriggerCause() {}

    @Disabled(DISABLED_DESCRIPTION)
    @Test
    void testRecordInProgressRescale() {}
}
