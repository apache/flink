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
import java.util.Collections;

import static org.mockito.Mockito.mock;

public class InterSlotPlacementConstraintTest extends TestLogger {

	@Test
	public void testCheckContextContainSlots() {
		JobID jobId = JobID.generate();
		SlotTag tag1 = new SlotTag("TAG_1", jobId);
		SlotTag tag2 = new SlotTag("TAG_2", jobId);
		SlotTag tag3 = new SlotTag("TAG_3", jobId);
		SlotTag tag4 = new SlotTag("TAG_4", jobId);

		TaggedSlot applySlot = mock(TaggedSlot.class);
		InterSlotPlacementConstraint constraint =  new InterSlotPlacementConstraint(
			applySlot,
			new TaggedSlotContext(true, new TaggedSlot(true, Arrays.asList(tag1, tag2), SlotTagScope.JOB)),
			PlacementConstraintScope.TM);
		Assert.assertTrue(constraint.check(Arrays.asList(Arrays.asList(tag3, tag1), Arrays.asList(tag4))));
		Assert.assertTrue(constraint.check(Arrays.asList(Arrays.asList(tag3), Arrays.asList(tag4, tag2))));
		Assert.assertFalse(constraint.check(Arrays.asList(Arrays.asList(tag3), Arrays.asList(tag4))));
		Assert.assertFalse(constraint.check(Arrays.asList(Arrays.asList(tag3, tag4), Collections.emptyList())));
	}

	@Test
	public void testCheckContextNotContainSlots() {
		JobID jobId = JobID.generate();
		SlotTag tag1 = new SlotTag("TAG_1", jobId);
		SlotTag tag2 = new SlotTag("TAG_2", jobId);
		SlotTag tag3 = new SlotTag("TAG_3", jobId);
		SlotTag tag4 = new SlotTag("TAG_4", jobId);

		TaggedSlot applySlot = mock(TaggedSlot.class);
		InterSlotPlacementConstraint constraint =  new InterSlotPlacementConstraint(
			applySlot,
			new TaggedSlotContext(false, new TaggedSlot(true, Arrays.asList(tag1, tag2), SlotTagScope.JOB)),
			PlacementConstraintScope.TM);
		Assert.assertFalse(constraint.check(Arrays.asList(Arrays.asList(tag3, tag1), Arrays.asList(tag4))));
		Assert.assertFalse(constraint.check(Arrays.asList(Arrays.asList(tag3), Arrays.asList(tag4, tag2))));
		Assert.assertTrue(constraint.check(Arrays.asList(Arrays.asList(tag3), Arrays.asList(tag4))));
		Assert.assertTrue(constraint.check(Arrays.asList(Arrays.asList(tag3, tag4), Collections.emptyList())));
	}
}
