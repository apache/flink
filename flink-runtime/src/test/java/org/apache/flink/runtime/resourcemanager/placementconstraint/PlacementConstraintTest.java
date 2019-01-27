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

package org.apache.flink.runtime.resourcemanager.placementconstraint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.util.TestLogger;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class PlacementConstraintTest extends TestLogger {

	@Test
	public void testApplyToSlotWithTags() {
		JobID jobId1 = JobID.generate();
		JobID jobId2 = JobID.generate();

		SlotTag tagAJob1 = new SlotTag("TAG_A", jobId1);
		SlotTag tagAJob2 = new SlotTag("TAG_A", jobId2);
		SlotTag tagBJob1 = new SlotTag("TAG_B", jobId1);
		SlotTag tagCJob1 = new SlotTag("TAG_C", jobId1);

		TaggedSlot jobScopeTaggedSlot = new TaggedSlot(true, Arrays.asList(tagAJob1, tagCJob1), SlotTagScope.JOB);
		PlacementConstraint jobScopePC = new PlacementConstraint(jobScopeTaggedSlot) {
			@Override
			public boolean check(List<List<SlotTag>> taskExecutorTags) {
				return true;
			}
		};
		Assert.assertTrue(jobScopePC.applyTo(Arrays.asList(tagAJob1, tagBJob1)));
		Assert.assertFalse(jobScopePC.applyTo(Arrays.asList(tagAJob2, tagBJob1)));
		Assert.assertFalse(jobScopePC.applyTo(Arrays.asList(tagBJob1)));

		TaggedSlot flinkScopeTaggedSlot = new TaggedSlot(true, Arrays.asList(tagAJob1, tagCJob1), SlotTagScope.FLINK);
		PlacementConstraint flinkScopePC = new PlacementConstraint(flinkScopeTaggedSlot) {
			@Override
			public boolean check(List<List<SlotTag>> taskExecutorTags) {
				return true;
			}
		};
		Assert.assertTrue(flinkScopePC.applyTo(Arrays.asList(tagAJob1, tagBJob1)));
		Assert.assertTrue(flinkScopePC.applyTo(Arrays.asList(tagAJob2, tagBJob1)));
		Assert.assertFalse(flinkScopePC.applyTo(Arrays.asList(tagBJob1)));
	}

	@Test
	public void testApplyToSlotWithoutTags() {
		JobID jobId1 = JobID.generate();
		JobID jobId2 = JobID.generate();

		SlotTag tagAJob1 = new SlotTag("TAG_A", jobId1);
		SlotTag tagAJob2 = new SlotTag("TAG_A", jobId2);
		SlotTag tagBJob1 = new SlotTag("TAG_B", jobId1);
		SlotTag tagCJob1 = new SlotTag("TAG_C", jobId1);

		TaggedSlot jobScopeTaggedSlot = new TaggedSlot(false, Arrays.asList(tagAJob1, tagCJob1), SlotTagScope.JOB);
		PlacementConstraint jobScopePC = new PlacementConstraint(jobScopeTaggedSlot) {
			@Override
			public boolean check(List<List<SlotTag>> taskExecutorTags) {
				return true;
			}
		};
		Assert.assertFalse(jobScopePC.applyTo(Arrays.asList(tagAJob1, tagBJob1)));
		Assert.assertTrue(jobScopePC.applyTo(Arrays.asList(tagAJob2, tagBJob1)));
		Assert.assertTrue(jobScopePC.applyTo(Arrays.asList(tagBJob1)));

		TaggedSlot flinkScopeTaggedSlot = new TaggedSlot(false, Arrays.asList(tagAJob1, tagCJob1), SlotTagScope.FLINK);
		PlacementConstraint flinkScopePC = new PlacementConstraint(flinkScopeTaggedSlot) {
			@Override
			public boolean check(List<List<SlotTag>> taskExecutorTags) {
				return true;
			}
		};
		Assert.assertFalse(flinkScopePC.applyTo(Arrays.asList(tagAJob1, tagBJob1)));
		Assert.assertFalse(flinkScopePC.applyTo(Arrays.asList(tagAJob2, tagBJob1)));
		Assert.assertTrue(flinkScopePC.applyTo(Arrays.asList(tagBJob1)));
	}
}
