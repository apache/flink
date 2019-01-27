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

import java.util.ArrayList;
import java.util.List;

/**
 * Describes slots with or without certain tags.
 */
public class TaggedSlot {
	private final boolean withTag;
	private final List<SlotTag> tags;
	private final SlotTagScope scope;

	public TaggedSlot(boolean withTag, List<SlotTag> tags, SlotTagScope scope) {
		this.withTag = withTag;
		this.tags = new ArrayList<>(tags);
		this.scope = scope;
	}

	public boolean isWithTag() { return withTag; }

	public List<SlotTag> getTags() { return tags; }

	public boolean matchSlotWithTags(List<SlotTag> tags) {
		if (withTag) {
			for (SlotTag tag1 : this.tags) {
				for (SlotTag tag2 : tags) {
					if (tag1.match(tag2, scope)) {
						return true;
					}
				}
			}
			return false;
		} else {
			for (SlotTag tag1 : this.tags) {
				for (SlotTag tag2 : tags) {
					if (tag1.match(tag2, scope)) {
						return false;
					}
				}
			}
			return true;
		}
	}
}
