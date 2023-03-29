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

package org.apache.flink.runtime.source.coordinator;

import org.apache.flink.api.common.eventtime.WatermarkAlignmentParams;
import org.apache.flink.core.fs.AutoCloseableRegistry;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.source.event.ReportedWatermarkEvent;
import org.apache.flink.runtime.source.event.WatermarkAlignmentEvent;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for watermark alignment of the {@link SourceCoordinator}. */
@SuppressWarnings("serial")
class SourceCoordinatorAlignmentTest extends SourceCoordinatorTestBase {

    @Test
    void testWatermarkAlignment() throws Exception {
        try (AutoCloseableRegistry closeableRegistry = new AutoCloseableRegistry()) {
            SourceCoordinator<?, ?> sourceCoordinator1 =
                    getAndStartNewSourceCoordinator(
                            new WatermarkAlignmentParams(1000L, "group1", Long.MAX_VALUE),
                            closeableRegistry);

            int subtask0 = 0;
            int subtask1 = 1;
            reportWatermarkEvent(sourceCoordinator1, subtask0, 42);
            assertLatestWatermarkAlignmentEvent(subtask0, 1042);

            reportWatermarkEvent(sourceCoordinator1, subtask1, 44);
            assertLatestWatermarkAlignmentEvent(subtask0, 1042);
            assertLatestWatermarkAlignmentEvent(subtask1, 1042);

            reportWatermarkEvent(sourceCoordinator1, subtask0, 5000);
            assertLatestWatermarkAlignmentEvent(subtask0, 1044);
            assertLatestWatermarkAlignmentEvent(subtask1, 1044);
        }
    }

    @Test
    void testWatermarkAlignmentWithIdleness() throws Exception {
        try (AutoCloseableRegistry closeableRegistry = new AutoCloseableRegistry()) {
            SourceCoordinator<?, ?> sourceCoordinator1 =
                    getAndStartNewSourceCoordinator(
                            new WatermarkAlignmentParams(1000L, "group1", Long.MAX_VALUE),
                            closeableRegistry);

            int subtask0 = 0;
            int subtask1 = 1;
            reportWatermarkEvent(sourceCoordinator1, subtask0, 42);
            assertLatestWatermarkAlignmentEvent(subtask0, 1042);

            reportWatermarkEvent(sourceCoordinator1, subtask1, 44);
            assertLatestWatermarkAlignmentEvent(subtask0, 1042);
            assertLatestWatermarkAlignmentEvent(subtask1, 1042);

            // subtask0 becomes idle
            reportWatermarkEvent(sourceCoordinator1, subtask0, Long.MAX_VALUE);
            assertLatestWatermarkAlignmentEvent(subtask0, 1044);
            assertLatestWatermarkAlignmentEvent(subtask1, 1044);

            // subtask0 becomes active again
            reportWatermarkEvent(sourceCoordinator1, subtask0, 42);
            assertLatestWatermarkAlignmentEvent(subtask0, 1042);
            assertLatestWatermarkAlignmentEvent(subtask1, 1042);

            // all subtask becomes idle
            reportWatermarkEvent(sourceCoordinator1, subtask0, Long.MAX_VALUE);
            reportWatermarkEvent(sourceCoordinator1, subtask1, Long.MAX_VALUE);
            assertLatestWatermarkAlignmentEvent(subtask0, Long.MAX_VALUE);
            assertLatestWatermarkAlignmentEvent(subtask1, Long.MAX_VALUE);

            // subtask0 becomes active again
            reportWatermarkEvent(sourceCoordinator1, subtask0, 42);
            assertLatestWatermarkAlignmentEvent(subtask0, 1042);
            assertLatestWatermarkAlignmentEvent(subtask1, 1042);

            // subtask1 becomes active again
            reportWatermarkEvent(sourceCoordinator1, subtask1, 46);
            assertLatestWatermarkAlignmentEvent(subtask0, 1042);
            assertLatestWatermarkAlignmentEvent(subtask1, 1042);
        }
    }

    @Test
    void testWatermarkAlignmentWithTwoGroups() throws Exception {
        try (AutoCloseableRegistry closeableRegistry = new AutoCloseableRegistry()) {
            long maxDrift = 1000L;
            SourceCoordinator<?, ?> sourceCoordinator1 =
                    getAndStartNewSourceCoordinator(
                            new WatermarkAlignmentParams(maxDrift, "group1", Long.MAX_VALUE),
                            closeableRegistry);

            SourceCoordinator<?, ?> sourceCoordinator2 =
                    getAndStartNewSourceCoordinator(
                            new WatermarkAlignmentParams(maxDrift, "group2", Long.MAX_VALUE),
                            closeableRegistry);

            int subtask0 = 0;
            int subtask1 = 1;
            reportWatermarkEvent(sourceCoordinator1, subtask0, 42);
            assertLatestWatermarkAlignmentEvent(subtask0, 1042);

            reportWatermarkEvent(sourceCoordinator2, subtask1, 44);
            assertLatestWatermarkAlignmentEvent(subtask0, 1042);
            assertLatestWatermarkAlignmentEvent(subtask1, 1044);

            reportWatermarkEvent(sourceCoordinator1, subtask0, 5000);
            assertLatestWatermarkAlignmentEvent(subtask0, 6000);
            assertLatestWatermarkAlignmentEvent(subtask1, 1044);
        }
    }

    private SourceCoordinator<?, ?> getAndStartNewSourceCoordinator(
            WatermarkAlignmentParams watermarkAlignmentParams,
            AutoCloseableRegistry closeableRegistry)
            throws Exception {
        SourceCoordinator<?, ?> sourceCoordinator =
                getNewSourceCoordinator(watermarkAlignmentParams);
        closeableRegistry.registerCloseable(sourceCoordinator);
        sourceCoordinator.start();
        setAllReaderTasksReady(sourceCoordinator);

        return sourceCoordinator;
    }

    private void reportWatermarkEvent(
            SourceCoordinator<?, ?> sourceCoordinator1, int subtask, long watermark) {
        sourceCoordinator1.handleEventFromOperator(
                subtask, 0, new ReportedWatermarkEvent(watermark));
        waitForCoordinatorToProcessActions();
        sourceCoordinator1.announceCombinedWatermark();
    }

    private void assertLatestWatermarkAlignmentEvent(int subtask, long expectedWatermark) {
        List<OperatorEvent> events = receivingTasks.getSentEventsForSubtask(subtask);
        assertThat(events).isNotEmpty();
        assertThat(events.get(events.size() - 1))
                .isEqualTo(new WatermarkAlignmentEvent(expectedWatermark));
    }
}
