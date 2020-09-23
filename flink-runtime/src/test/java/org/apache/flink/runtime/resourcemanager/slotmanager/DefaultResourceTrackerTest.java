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

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.util.TestLogger;

import org.hamcrest.collection.IsMapContaining;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertThat;

/**
 * Tests for the {@link DefaultResourceTracker}.
 *
 * <p>Note: The majority is of the tracking logic is covered by the {@link JobScopedResourceTrackerTest}.
 */
public class DefaultResourceTrackerTest extends TestLogger {

	@Test
	public void testInitialBehavior() {
		DefaultResourceTracker tracker = new DefaultResourceTracker();

		assertThat(tracker.isEmpty(), is(true));
		tracker.notifyLostResource(JobID.generate(), ResourceProfile.ANY);
		tracker.clear();
	}

	@Test
	public void testGetRequiredResources() {
		DefaultResourceTracker tracker = new DefaultResourceTracker();
		JobID jobId1 = JobID.generate();
		JobID jobId2 = JobID.generate();

		ResourceRequirement requirement1 = ResourceRequirement.create(ResourceProfile.ANY, 1);
		ResourceRequirement requirement2 = ResourceRequirement.create(ResourceProfile.ANY, 2);

		tracker.notifyResourceRequirements(jobId1, Collections.singletonList(requirement1));
		tracker.notifyResourceRequirements(jobId2, Collections.singletonList(requirement2));

		Map<JobID, Collection<ResourceRequirement>> requiredResources = tracker.getRequiredResources();
		assertThat(requiredResources, IsMapContaining.hasEntry(is(jobId1), contains(requirement1)));
		assertThat(requiredResources, IsMapContaining.hasEntry(is(jobId2), contains(requirement2)));
	}

	@Test
	public void testGetAcquiredResources() {
		DefaultResourceTracker tracker = new DefaultResourceTracker();
		JobID jobId1 = JobID.generate();
		JobID jobId2 = JobID.generate();

		ResourceRequirement requirement1 = ResourceRequirement.create(ResourceProfile.ANY, 1);
		ResourceRequirement requirement2 = ResourceRequirement.create(ResourceProfile.ANY, 2);

		tracker.notifyAcquiredResource(jobId1, requirement1.getResourceProfile());
		for (int x = 0; x < requirement2.getNumberOfRequiredSlots(); x++) {
			tracker.notifyAcquiredResource(jobId2, requirement2.getResourceProfile());
		}

		assertThat(tracker.getAcquiredResources(jobId1), contains(requirement1));
		assertThat(tracker.getAcquiredResources(jobId2), contains(requirement2));

		tracker.notifyLostResource(jobId1, requirement1.getResourceProfile());
		assertThat(tracker.getAcquiredResources(jobId1), empty());
	}

	@Test
	public void testTrackerRemovedOnRequirementReset() {
		DefaultResourceTracker tracker = new DefaultResourceTracker();
		JobID jobId1 = JobID.generate();

		tracker.notifyResourceRequirements(jobId1, Collections.singletonList(ResourceRequirement.create(ResourceProfile.ANY, 1)));
		assertThat(tracker.isEmpty(), is(false));

		tracker.notifyResourceRequirements(jobId1, Collections.emptyList());
		assertThat(tracker.isEmpty(), is(true));
	}

	@Test
	public void testTrackerRemovedOnResourceLoss() {
		DefaultResourceTracker tracker = new DefaultResourceTracker();
		JobID jobId1 = JobID.generate();

		tracker.notifyAcquiredResource(jobId1, ResourceProfile.ANY);
		assertThat(tracker.isEmpty(), is(false));

		tracker.notifyLostResource(jobId1, ResourceProfile.ANY);
		assertThat(tracker.isEmpty(), is(true));
	}

	@Test
	public void testTrackerRetainedOnResourceLossIfRequirementExists() {
		DefaultResourceTracker tracker = new DefaultResourceTracker();
		JobID jobId1 = JobID.generate();

		tracker.notifyAcquiredResource(jobId1, ResourceProfile.ANY);
		tracker.notifyResourceRequirements(jobId1, Collections.singletonList(ResourceRequirement.create(ResourceProfile.ANY, 1)));

		tracker.notifyLostResource(jobId1, ResourceProfile.ANY);
		assertThat(tracker.isEmpty(), is(false));

		tracker.notifyResourceRequirements(jobId1, Collections.emptyList());
		assertThat(tracker.isEmpty(), is(true));
	}

	@Test
	public void testTrackerRetainedOnRequirementResetIfResourceExists() {
		DefaultResourceTracker tracker = new DefaultResourceTracker();
		JobID jobId1 = JobID.generate();

		tracker.notifyAcquiredResource(jobId1, ResourceProfile.ANY);
		tracker.notifyResourceRequirements(jobId1, Collections.singletonList(ResourceRequirement.create(ResourceProfile.ANY, 1)));

		tracker.notifyResourceRequirements(jobId1, Collections.emptyList());
		assertThat(tracker.isEmpty(), is(false));

		tracker.notifyLostResource(jobId1, ResourceProfile.ANY);
		assertThat(tracker.isEmpty(), is(true));
	}
}
