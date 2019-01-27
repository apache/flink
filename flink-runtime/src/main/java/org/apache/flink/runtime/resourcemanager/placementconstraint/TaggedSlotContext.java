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
 * Describes contexts that contain or not contain slots with or without certain tags.
 */
public class TaggedSlotContext {
	private final boolean containSlot;
	private final TaggedSlot slot;

	public TaggedSlotContext(boolean containSlot, TaggedSlot slot) {
		this.containSlot = containSlot;
		this.slot = slot;
	}

	public boolean isContainSlot() { return containSlot; }

	public TaggedSlot getSlot() { return slot; }

	public boolean matchContextWithTags(List<List<SlotTag>> contextTags) {
		if (containSlot) {
			for (List<SlotTag> slotTags : contextTags) {
				if (slot.matchSlotWithTags(slotTags)) {
					return true;
				}
			}
			return false;
		} else {
			for (List<SlotTag> slotTags : contextTags) {
				if (slot.matchSlotWithTags(slotTags)) {
					return false;
				}
			}
			return true;
		}
	}
}
