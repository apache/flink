/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;

import org.junit.jupiter.api.Test;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createExecutionAttemptId;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DefaultExecutionDeploymentTracker}. */
class DefaultExecutionDeploymentTrackerTest {

    @Test
    void testStartTracking() {
        final DefaultExecutionDeploymentTracker tracker = new DefaultExecutionDeploymentTracker();

        final ExecutionAttemptID attemptId1 = createExecutionAttemptId();
        final ResourceID resourceId1 = ResourceID.generate();
        tracker.startTrackingPendingDeploymentOf(attemptId1, resourceId1);

        assertThat(tracker.getExecutionsOn(resourceId1))
                .containsEntry(attemptId1, ExecutionDeploymentState.PENDING);

        tracker.completeDeploymentOf(attemptId1);

        assertThat(tracker.getExecutionsOn(resourceId1))
                .containsEntry(attemptId1, ExecutionDeploymentState.DEPLOYED);
    }

    @Test
    void testStopTrackingCompletedDeployment() {
        final DefaultExecutionDeploymentTracker tracker = new DefaultExecutionDeploymentTracker();

        final ExecutionAttemptID attemptId1 = createExecutionAttemptId();
        final ResourceID resourceId1 = ResourceID.generate();
        tracker.startTrackingPendingDeploymentOf(attemptId1, resourceId1);

        tracker.completeDeploymentOf(attemptId1);

        tracker.stopTrackingDeploymentOf(attemptId1);

        assertThat(tracker.getExecutionsOn(resourceId1).entrySet()).isEmpty();
    }

    @Test
    void testStopTrackingPendingDeployment() {
        final DefaultExecutionDeploymentTracker tracker = new DefaultExecutionDeploymentTracker();

        final ExecutionAttemptID attemptId1 = createExecutionAttemptId();
        final ResourceID resourceId1 = ResourceID.generate();
        tracker.startTrackingPendingDeploymentOf(attemptId1, resourceId1);

        tracker.stopTrackingDeploymentOf(attemptId1);

        assertThat(tracker.getExecutionsOn(resourceId1).entrySet()).isEmpty();
    }

    @Test
    void testStopTrackingDoesNotAffectOtherIds() {
        final DefaultExecutionDeploymentTracker tracker = new DefaultExecutionDeploymentTracker();

        final ExecutionAttemptID attemptId1 = createExecutionAttemptId();
        final ResourceID resourceId1 = ResourceID.generate();
        tracker.startTrackingPendingDeploymentOf(attemptId1, resourceId1);
        tracker.completeDeploymentOf(attemptId1);

        tracker.stopTrackingDeploymentOf(createExecutionAttemptId());

        assertThat(tracker.getExecutionsOn(resourceId1)).containsKey(attemptId1);
    }

    @Test
    void testCompleteDeploymentUnknownExecutionDoesNotThrowException() {
        final DefaultExecutionDeploymentTracker tracker = new DefaultExecutionDeploymentTracker();

        tracker.completeDeploymentOf(createExecutionAttemptId());
    }

    @Test
    void testStopTrackingUnknownExecutionDoesNotThrowException() {
        final DefaultExecutionDeploymentTracker tracker = new DefaultExecutionDeploymentTracker();

        final ExecutionAttemptID attemptId2 = createExecutionAttemptId();
        tracker.stopTrackingDeploymentOf(attemptId2);
    }

    @Test
    void testGetExecutionsReturnsEmptySetForUnknownHost() {
        final DefaultExecutionDeploymentTracker tracker = new DefaultExecutionDeploymentTracker();

        assertThat(tracker.getExecutionsOn(ResourceID.generate()).entrySet()).isEmpty();
    }
}
