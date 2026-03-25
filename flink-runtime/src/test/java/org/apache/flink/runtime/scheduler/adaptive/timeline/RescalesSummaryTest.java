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

import org.apache.flink.runtime.util.stats.StatsSummary;
import org.apache.flink.util.AbstractID;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link RescalesSummary}. */
class RescalesSummaryTest {

    private Rescale getRescale() {
        Rescale rescale = new Rescale(new RescaleIdInfo(new AbstractID(), 1L));
        rescale.setStartTimestamp(1L);
        rescale.setEndTimestamp(2L);
        return rescale;
    }

    private void assertSummary(
            StatsSummary summary,
            long expectedCount,
            long expectedSum,
            long expectedAvg,
            long expectedMax,
            long expectedMin) {
        assertThat(summary.getMinimum()).isEqualTo(expectedMin);
        assertThat(summary.getMaximum()).isEqualTo(expectedMax);
        assertThat(summary.getAverage()).isEqualTo(expectedAvg);
        assertThat(summary.getSum()).isEqualTo(expectedSum);
        assertThat(summary.getCount()).isEqualTo(expectedCount);
    }

    @Test
    void testAddInProgressAndTerminated() {
        RescalesSummary rescalesSummary = new RescalesSummary(5);
        Rescale rescale = getRescale();
        rescale.setStartTimestamp(1L);
        rescale.setEndTimestamp(2L);

        // Test adding unexpected non-terminated rescale.
        rescalesSummary.addTerminated(rescale);
        assertThat(rescalesSummary.getTotalRescalesCount()).isZero();
        assertSummary(rescalesSummary.getAllTerminatedSummary(), 0L, 0L, 0L, 0L, 0L);

        // Test adding unexpected terminated rescale.
        rescale.setTerminatedReason(TerminatedReason.SUCCEEDED);
        rescalesSummary.addInProgress(rescale);
        assertThat(rescalesSummary.getTotalRescalesCount()).isZero();
        assertSummary(rescalesSummary.getAllTerminatedSummary(), 0L, 0L, 0L, 0L, 0L);

        // Test add in-progress rescale.
        rescale = getRescale();
        rescalesSummary.addInProgress(rescale);
        assertThat(rescalesSummary.getTotalRescalesCount()).isOne();
        assertThat(rescalesSummary.getCompletedRescalesCount()).isZero();
        assertThat(rescalesSummary.getIgnoredRescalesCount()).isZero();
        assertThat(rescalesSummary.getFailedRescalesCount()).isZero();
        assertThat(rescalesSummary.getInProgressRescalesCount()).isOne();

        // Test add a completed rescale after adding a in-progress rescale.
        rescale.setTerminatedReason(TerminatedReason.SUCCEEDED);
        rescalesSummary.addTerminated(rescale);
        assertThat(rescalesSummary.getTotalRescalesCount()).isOne();
        assertThat(rescalesSummary.getCompletedRescalesCount()).isOne();
        assertThat(rescalesSummary.getIgnoredRescalesCount()).isZero();
        assertThat(rescalesSummary.getInProgressRescalesCount()).isZero();
        assertThat(rescalesSummary.getFailedRescalesCount()).isZero();
        assertSummary(rescalesSummary.getCompletedRescalesSummary(), 1L, 1L, 1L, 1L, 1L);
    }
}
