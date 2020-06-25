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
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.IsCollectionContaining.hasItems;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link ExecutionDeploymentTrackerImpl}.
 */
public class ExecutionDeploymentTrackerImplTest extends TestLogger {

	@Test
	public void testStartTracking() {
		final ExecutionDeploymentTrackerImpl tracker = new ExecutionDeploymentTrackerImpl();

		final ExecutionAttemptID attemptId1 = new ExecutionAttemptID();
		final ResourceID resourceId1 = ResourceID.generate();
		tracker.startTrackingDeployment(attemptId1, resourceId1);

		assertThat(tracker.getExecutions(resourceId1), hasItems(attemptId1));
	}

	@Test
	public void testOnCompleteEquivalentToStartTracking() {
		final ExecutionDeploymentTrackerImpl tracker = new ExecutionDeploymentTrackerImpl();

		final ExecutionAttemptID attemptId1 = new ExecutionAttemptID();
		final ResourceID resourceId1 = ResourceID.generate();
		tracker.onCompletedDeployment(attemptId1, resourceId1);

		assertThat(tracker.getExecutions(resourceId1), hasItems(attemptId1));
	}

	@Test
	public void testStopTracking() {
		final ExecutionDeploymentTrackerImpl tracker = new ExecutionDeploymentTrackerImpl();

		final ExecutionAttemptID attemptId1 = new ExecutionAttemptID();
		final ResourceID resourceId1 = ResourceID.generate();
		tracker.startTrackingDeployment(attemptId1, resourceId1);

		tracker.stopTrackingDeployment(attemptId1);

		assertThat(tracker.getExecutions(resourceId1), empty());
	}

	@Test
	public void testStopTrackingDoesNotAffectOtherIds() {
		final ExecutionDeploymentTrackerImpl tracker = new ExecutionDeploymentTrackerImpl();

		final ExecutionAttemptID attemptId1 = new ExecutionAttemptID();
		final ResourceID resourceId1 = ResourceID.generate();
		tracker.startTrackingDeployment(attemptId1, resourceId1);

		tracker.stopTrackingDeployment(new ExecutionAttemptID());

		assertThat(tracker.getExecutions(resourceId1), hasItems(attemptId1));
	}

	@Test
	public void testStopTrackingUnknownExecutionDoesNotThrowException() {
		final ExecutionDeploymentTrackerImpl tracker = new ExecutionDeploymentTrackerImpl();

		final ExecutionAttemptID attemptId2 = new ExecutionAttemptID();
		tracker.stopTrackingDeployment(attemptId2);
	}

	@Test
	public void testGetExecutionsReturnsEmptySetForUnknownHost() {
		final ExecutionDeploymentTrackerImpl tracker = new ExecutionDeploymentTrackerImpl();

		assertThat(tracker.getExecutions(ResourceID.generate()), notNullValue());
	}
}
