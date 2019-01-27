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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The PlacementConstraintManager manages slot placement constraints of all the jobs, and provide constraint checking services.
 */
public class PlacementConstraintManager {
	private final Map<PlacementConstraintID, PlacementConstraint> placementConstraintTable = new HashMap<>();
	private final Map<JobID, Set<PlacementConstraintID>> jobConstraintIndex = new HashMap<>();
	private final Map<String, Set<PlacementConstraintID>> withTagNameConstraintIndex = new HashMap<>();
	private final Map<String, Set<PlacementConstraintID>> withoutTagNameConstraintIndex = new HashMap<>();

	public synchronized void setJobConstraints(JobID jobId, List<PlacementConstraint> newConstraints) {
		if (jobConstraintIndex.containsKey(jobId)) {
			for (PlacementConstraintID pcid : jobConstraintIndex.get(jobId)) {
				placementConstraintTable.remove(pcid);
			}
		}

		jobConstraintIndex.put(jobId, new HashSet<>());
		for (PlacementConstraint constraint : newConstraints) {
			PlacementConstraintID pcid = PlacementConstraintID.generate();
			placementConstraintTable.put(pcid, constraint);
			jobConstraintIndex.get(jobId).add(pcid);

			TaggedSlot slot = constraint.getSlot();
			if (slot.isWithTag()) {
				for (SlotTag tag : slot.getTags()) {
					if (!withTagNameConstraintIndex.containsKey(tag.getTagName())) {
						withTagNameConstraintIndex.put(tag.getTagName(), new HashSet<>());
					}
					withTagNameConstraintIndex.get(tag.getTagName()).add(pcid);
				}
			} else {
				for (SlotTag tag : slot.getTags()) {
					if (!withoutTagNameConstraintIndex.containsKey(tag.getTagName())) {
						withoutTagNameConstraintIndex.put(tag.getTagName(), new HashSet<>());
					}
					withoutTagNameConstraintIndex.get(tag.getTagName()).add(pcid);
				}
			}
		}
	}

	public synchronized boolean check(JobID jobId, List<SlotTag> slotTags, List<List<SlotTag>> taskExecutorTags) {
		Set<PlacementConstraint> relatedConstraints = new HashSet<>();

		// get related constraints from withTagIndex
		for (SlotTag tag : slotTags) {
			if (!withTagNameConstraintIndex.containsKey(tag.getTagName())) {
				continue;
			}
			Set<PlacementConstraintID> withTagConstraintIds = withTagNameConstraintIndex.get(tag.getTagName());
			Iterator<PlacementConstraintID> iterator = withTagConstraintIds.iterator();
			while (iterator.hasNext()) {
				PlacementConstraintID pcid = iterator.next();
				if (!placementConstraintTable.containsKey(pcid)) {
					iterator.remove();
					continue;
				}
				relatedConstraints.add(placementConstraintTable.get(pcid));
			}
			if (withTagConstraintIds.isEmpty()) {
				withTagNameConstraintIndex.remove(tag.getTagName());
			}
		}

		// get related constraints from withoutTagIndex
		for (String tagName : withoutTagNameConstraintIndex.keySet()) {
			SlotTag tag = new SlotTag(tagName, jobId);
			if (slotTags.contains(tag)) {
				continue;
			}
			Set<PlacementConstraintID> withoutTagConstraintIds = withoutTagNameConstraintIndex.get(tag.getTagName());
			Iterator<PlacementConstraintID> iterator = withoutTagConstraintIds.iterator();
			while (iterator.hasNext()) {
				PlacementConstraintID pcid = iterator.next();
				if (!placementConstraintTable.containsKey(pcid)) {
					iterator.remove();
					continue;
				}
				relatedConstraints.add(placementConstraintTable.get(pcid));
			}
			if (withoutTagConstraintIds.isEmpty()) {
				withoutTagNameConstraintIndex.remove(tag.getTagName());
			}
		}

		// check placement constraints
		for (PlacementConstraint pc : relatedConstraints) {
			if (pc.applyTo(slotTags) && !pc.check(taskExecutorTags)) {
				return false;
			}
		}
		return true;
	}
}
