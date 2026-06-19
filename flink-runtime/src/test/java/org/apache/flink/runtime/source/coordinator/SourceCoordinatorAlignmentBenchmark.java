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
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.mocks.MockSource;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.MockOperatorCoordinatorContext;
import org.apache.flink.runtime.source.event.ReportedWatermarkEvent;

import java.util.Random;

import static org.apache.flink.runtime.source.coordinator.CoordinatorTestUtils.waitForCoordinatorToProcessActions;

/** The benchmark of watermark alignment. */
public class SourceCoordinatorAlignmentBenchmark {

    private static final OperatorID OPERATOR_ID = new OperatorID(1234L, 5678L);

    private SourceCoordinator<?, ?> sourceCoordinator;

    private int numSubtasks;

    private long second;

    private long[] randomMilliseconds;

    public SourceCoordinatorAlignmentBenchmark() {}

    public void setup(int numSubtasks) throws Exception {
        SourceCoordinatorProvider<MockSourceSplit> provider =
                new SourceCoordinatorProvider<>(
                        "SourceCoordinatorProviderTest",
                        OPERATOR_ID,
                        new MockSource(Boundedness.BOUNDED, 2),
                        1,
                        new WatermarkAlignmentParams(1000L, "group1", Long.MAX_VALUE),
                        null);
        this.sourceCoordinator =
                (SourceCoordinator<?, ?>)
                        provider.getCoordinator(
                                new MockOperatorCoordinatorContext(OPERATOR_ID, numSubtasks));
        this.sourceCoordinator.start();
        this.numSubtasks = numSubtasks;
        this.second = 0;
        this.randomMilliseconds = generateRandomMilliseconds(numSubtasks);

        // Initialize the watermark for all subtasks.
        sendReportedWatermarkToAllSubtasks();
    }

    public void teardown() throws Exception {
        sourceCoordinator.close();
    }

    public void sendReportedWatermarkToAllSubtasks() {
        for (int subtaskIndex = 0; subtaskIndex < numSubtasks; subtaskIndex++) {
            sourceCoordinator.handleEventFromOperator(
                    subtaskIndex,
                    0,
                    new ReportedWatermarkEvent(second + randomMilliseconds[subtaskIndex]));
        }
        waitForCoordinatorToProcessActions(sourceCoordinator.getContext());
        second += 100_000;
    }

    private long[] generateRandomMilliseconds(int numSubtasks) {
        Random random = new Random();
        long[] randomMilliseconds = new long[numSubtasks];
        for (int subtaskIndex = 0; subtaskIndex < numSubtasks; subtaskIndex++) {
            randomMilliseconds[subtaskIndex] = random.nextInt(1000);
        }
        return randomMilliseconds;
    }
}
