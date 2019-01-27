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

import java.util.List;

/**
 * A PlacementConstraint describes a constraint on the context to which certain slots should be placed.
 */
public abstract class PlacementConstraint {

	/** Describes the slots that this constraint applies to.  */
	protected final TaggedSlot slot;

	public PlacementConstraint(TaggedSlot slot) {
		this.slot = slot;
	}

	public TaggedSlot getSlot() {
		return slot;
	}

	public boolean applyTo(List<SlotTag> slotTags) {
		return slot.matchSlotWithTags(slotTags);
	}

	public abstract boolean check(List<List<SlotTag>> taskExecutorTags);
}
