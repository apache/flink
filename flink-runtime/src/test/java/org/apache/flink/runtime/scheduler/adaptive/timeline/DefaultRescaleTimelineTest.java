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

import org.apache.flink.runtime.scheduler.DefaultVertexParallelismStore;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DefaultRescaleTimeline}. */
class DefaultRescaleTimelineTest {

    private RescaleTimeline rescaleTimeline;
    private DefaultRescaleTimeline defaultRescaleTimeline;

    @BeforeEach
    void setUp() {
        this.rescaleTimeline =
                new DefaultRescaleTimeline(
                        () ->
                                new TestingJobInformation(
                                        Collections.emptySet(),
                                        Collections.emptyList(),
                                        new DefaultVertexParallelismStore()),
                        3);
        this.defaultRescaleTimeline = (DefaultRescaleTimeline) rescaleTimeline;
    }

    @Test
    void testIsIdling() {
        assertThat(rescaleTimeline.isIdling()).isTrue();
        rescaleTimeline.newRescale(true);
        assertThat(rescaleTimeline.isIdling()).isFalse();

        rescaleTimeline.updateRescale(
                rescaleToUpdate -> rescaleToUpdate.setTerminatedReason(TerminatedReason.SUCCEEDED));
        assertThat(rescaleTimeline.isIdling()).isTrue();
    }

    @Test
    void testNewRescaleWithIdInfoGenerationLogic() {
        assertThat(defaultRescaleTimeline.currentRescale()).isNull();
        rescaleTimeline.newRescale(true);
        Rescale rescale1 = defaultRescaleTimeline.currentRescale();
        assertThat(rescale1).isNotNull();
        RescaleIdInfo rescaleIdInfo1 = rescale1.getRescaleIdInfo();
        assertThat(rescaleIdInfo1.getRescaleAttemptId()).isOne();

        rescaleTimeline.updateRescale(r -> r.setTerminatedReason(TerminatedReason.SUCCEEDED));
        rescaleTimeline.newRescale(false);
        Rescale rescale2 = defaultRescaleTimeline.currentRescale();
        RescaleIdInfo rescaleIdInfo2 = rescale2.getRescaleIdInfo();
        assertThat(rescale2).isNotNull();
        assertThat(rescaleIdInfo2.getRescaleAttemptId()).isEqualTo(2L);
        assertThat(rescaleIdInfo2.getResourceRequirementsId())
                .isEqualTo(rescaleIdInfo1.getResourceRequirementsId());
        assertThat(rescaleIdInfo2.getRescaleUuid()).isNotEqualTo(rescaleIdInfo1.getRescaleUuid());

        rescaleTimeline.updateRescale(r -> r.setTerminatedReason(TerminatedReason.SUCCEEDED));
        rescaleTimeline.newRescale(true);
        Rescale rescale3 = defaultRescaleTimeline.currentRescale();
        RescaleIdInfo rescaleIdInfo3 = rescale3.getRescaleIdInfo();
        assertThat(rescale3).isNotNull();
        assertThat(rescaleIdInfo3.getRescaleAttemptId()).isOne();
        assertThat(rescaleIdInfo3.getResourceRequirementsId())
                .isNotEqualTo(rescaleIdInfo1.getResourceRequirementsId())
                .isNotEqualTo(rescaleIdInfo2.getResourceRequirementsId());
        assertThat(rescaleIdInfo3.getRescaleUuid())
                .isNotEqualTo(rescaleIdInfo1.getRescaleUuid())
                .isNotEqualTo(rescaleIdInfo2.getRescaleUuid());
    }

    @Test
    void testGetLatestRescale() {
        assertThat(rescaleTimeline.getLatestRescale(TerminalState.FAILED)).isNull();
        assertThat(rescaleTimeline.getLatestRescale(TerminalState.COMPLETED)).isNull();
        assertThat(rescaleTimeline.getLatestRescale(TerminalState.IGNORED)).isNull();

        rescaleTimeline.newRescale(true);
        rescaleTimeline.updateRescale(
                rescaleToUpdate -> rescaleToUpdate.setTerminatedReason(TerminatedReason.SUCCEEDED));
        Rescale firstLatestCompletedRescale = defaultRescaleTimeline.currentRescale();
        assertThat(rescaleTimeline.getLatestRescale(TerminalState.COMPLETED))
                .isNotNull()
                .isEqualTo(defaultRescaleTimeline.currentRescale());

        rescaleTimeline.newRescale(true);
        rescaleTimeline.updateRescale(
                rescaleToUpdate ->
                        rescaleToUpdate.setTerminatedReason(TerminatedReason.EXCEPTION_OCCURRED));
        assertThat(rescaleTimeline.getLatestRescale(TerminalState.FAILED))
                .isNotNull()
                .isEqualTo(defaultRescaleTimeline.currentRescale());

        rescaleTimeline.newRescale(true);
        rescaleTimeline.updateRescale(
                rescaleToUpdate ->
                        rescaleToUpdate.setTerminatedReason(TerminatedReason.JOB_FINISHED));
        assertThat(rescaleTimeline.getLatestRescale(TerminalState.IGNORED))
                .isNotNull()
                .isEqualTo(defaultRescaleTimeline.currentRescale());

        rescaleTimeline.newRescale(true);
        rescaleTimeline.updateRescale(
                rescaleToUpdate -> rescaleToUpdate.setTerminatedReason(TerminatedReason.SUCCEEDED));
        assertThat(rescaleTimeline.getLatestRescale(TerminalState.COMPLETED))
                .isNotNull()
                .isEqualTo(defaultRescaleTimeline.currentRescale())
                .isNotEqualTo(firstLatestCompletedRescale);
    }

    @Test
    void testUpdateRescale() {
        assertThat(defaultRescaleTimeline.currentRescale()).isNull();
        rescaleTimeline.newRescale(true);
        rescaleTimeline.updateRescale(
                rescaleToUpdate -> rescaleToUpdate.setTerminatedReason(TerminatedReason.SUCCEEDED));
        assertThat(defaultRescaleTimeline.currentRescale().getTerminalState())
                .isNotNull()
                .isEqualTo(TerminalState.COMPLETED);
        assertThat(defaultRescaleTimeline.currentRescale().getTerminatedReason())
                .isNotNull()
                .isEqualTo(TerminatedReason.SUCCEEDED);
    }
}
