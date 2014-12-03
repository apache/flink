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

package org.apache.flink.runtime.jobmanager.scheduler;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.runtime.AbstractID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.instance.AllocatedSlot;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.slf4j.Logger;


public class SlotSharingGroupAssignment implements Serializable {

	static final long serialVersionUID = 42L;
	
	private static final Logger LOG = Scheduler.LOG;
	
	private transient final Object lock = new Object();
	
	/** All slots currently allocated to this sharing group */
	private final Set<SharedSlot> allSlots = new LinkedHashSet<SharedSlot>();
	
	/** The slots available per vertex type (jid), keyed by instance, to make them locatable */
	private final Map<AbstractID, Map<Instance, List<SharedSlot>>> availableSlotsPerJid = new LinkedHashMap<AbstractID, Map<Instance, List<SharedSlot>>>();
	
	
	// --------------------------------------------------------------------------------------------
	
	
	public SubSlot addNewSlotWithTask(AllocatedSlot slot, ExecutionVertex vertex) {
		JobVertexID id = vertex.getJobvertexId();
		return addNewSlotWithTask(slot, id, id);
	}
	
	public SubSlot addNewSlotWithTask(AllocatedSlot slot, ExecutionVertex vertex, CoLocationConstraint constraint) {
		AbstractID groupId = constraint.getGroupId();
		return addNewSlotWithTask(slot, groupId, null);
	}
	
	private SubSlot addNewSlotWithTask(AllocatedSlot slot, AbstractID groupId, JobVertexID vertexId) {
		
		final SharedSlot sharedSlot = new SharedSlot(slot, this);
		final Instance location = slot.getInstance();
		
		synchronized (lock) {
			// add to the total bookkeeping
			allSlots.add(sharedSlot);
			
			// allocate us a sub slot to return
			SubSlot subslot = sharedSlot.allocateSubSlot(vertexId);
			
			// preserve the locality information
			subslot.setLocality(slot.getLocality());
			
			boolean entryForNewJidExists = false;
			
			// let the other vertex types know about this one as well
			for (Map.Entry<AbstractID, Map<Instance, List<SharedSlot>>> entry : availableSlotsPerJid.entrySet()) {
				
				if (entry.getKey().equals(groupId)) {
					entryForNewJidExists = true;
					continue;
				}
				
				Map<Instance, List<SharedSlot>> available = entry.getValue();
				putIntoMultiMap(available, location, sharedSlot);
			}
			
			// make sure an empty entry exists for this group, if no other entry exists
			if (!entryForNewJidExists) {
				availableSlotsPerJid.put(groupId, new LinkedHashMap<Instance, List<SharedSlot>>());
			}
			
			return subslot;
		}
	}
	
	/**
	 * Gets a slot suitable for the given task vertex. This method will prefer slots that are local
	 * (with respect to {@link ExecutionVertex#getPreferredLocations()}), but will return non local
	 * slots if no local slot is available. The method returns null, when no slot is available for the
	 * given JobVertexID at all.
	 * 
	 * @param vertex
	 * 
	 * @return A task vertex for a task with the given JobVertexID, or null, if none is available.
	 */
	public SubSlot getSlotForTask(ExecutionVertex vertex) {
		synchronized (lock) {
			Pair<SharedSlot, Locality> p = getSlotForTaskInternal(vertex.getJobvertexId(), vertex, vertex.getPreferredLocations(), false);
			
			if (p != null) {
				SharedSlot ss = p.getLeft();
				SubSlot slot = ss.allocateSubSlot(vertex.getJobvertexId());
				slot.setLocality(p.getRight());
				return slot;
			}
			else {
				return null;
			}
		}
		
	}
	
	public SubSlot getSlotForTask(ExecutionVertex vertex, CoLocationConstraint constraint) {
		
		synchronized (lock) {
			SharedSlot shared = constraint.getSharedSlot();
			
			if (shared != null && !shared.isDisposed()) {
				// initialized and set
				SubSlot subslot = shared.allocateSubSlot(null);
				subslot.setLocality(Locality.LOCAL);
				return subslot;
			}
			else if (shared == null) {
				// not initialized, grab a new slot. preferred locations are defined by the vertex
				// we only associate the slot with the constraint, if it was a local match
				Pair<SharedSlot, Locality> p = getSlotForTaskInternal(constraint.getGroupId(), vertex, vertex.getPreferredLocations(), false);
				if (p == null) {
					return null;
				} else {
					shared = p.getLeft();
					Locality l = p.getRight();
					
					SubSlot sub = shared.allocateSubSlot(null);
					sub.setLocality(l);
					
					if (l != Locality.NON_LOCAL) {
						constraint.setSharedSlot(shared);
					}
					return sub;
				}
			}
			else {
				// disposed. get a new slot on the same instance
				Instance location = shared.getAllocatedSlot().getInstance();
				Pair<SharedSlot, Locality> p = getSlotForTaskInternal(constraint.getGroupId(), vertex, Collections.singleton(location), true);
				if (p == null) {
					return null;
				} else {
					shared = p.getLeft();
					constraint.setSharedSlot(shared);
					SubSlot subslot = shared.allocateSubSlot(null);
					subslot.setLocality(Locality.LOCAL);
					return subslot;
				}
			}
		}
	}
	
	/**
	 * NOTE: This method is not synchronized by itself, needs to be synchronized externally.
	 * 
	 * @return An allocated sub slot, or {@code null}, if no slot is available.
	 */
	private Pair<SharedSlot, Locality> getSlotForTaskInternal(AbstractID groupId, ExecutionVertex vertex, Iterable<Instance> preferredLocations, boolean localOnly) {
		if (allSlots.isEmpty()) {
			return null;
		}
		
		Map<Instance, List<SharedSlot>> slotsForGroup = availableSlotsPerJid.get(groupId);
		
		// get the available slots for the group
		if (slotsForGroup == null) {
			// no task is yet scheduled for that group, so all slots are available
			slotsForGroup = new LinkedHashMap<Instance, List<SharedSlot>>();
			availableSlotsPerJid.put(groupId, slotsForGroup);
			
			for (SharedSlot availableSlot : allSlots) {
				putIntoMultiMap(slotsForGroup, availableSlot.getAllocatedSlot().getInstance(), availableSlot);
			}
		}
		else if (slotsForGroup.isEmpty()) {
			return null;
		}
		
		// check whether we can schedule the task to a preferred location
		boolean didNotGetPreferred = false;
		
		if (preferredLocations != null) {
			for (Instance location : preferredLocations) {
				
				// set the flag that we failed a preferred location. If one will be found,
				// we return early anyways and skip the flag evaluation
				didNotGetPreferred = true;
				
				SharedSlot slot = removeFromMultiMap(slotsForGroup, location);
				if (slot != null && !slot.isDisposed()) {
					if (LOG.isDebugEnabled()) {
						LOG.debug("Local assignment in shared group : " + vertex + " --> " + slot);
					}
					
					return new ImmutablePair<SharedSlot, Locality>(slot, Locality.LOCAL);
				}
			}
		}
		
		// if we want only local assignments, exit now with a "not found" result
		if (didNotGetPreferred && localOnly) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("No local assignment in shared possible for " + vertex);
			}
			return null;
		}
		
		// schedule the task to any available location
		SharedSlot slot = pollFromMultiMap(slotsForGroup);
		if (slot != null && !slot.isDisposed()) {
			if (LOG.isDebugEnabled()) {
				LOG.debug((didNotGetPreferred ? "Non-local" : "Unconstrained") + " assignment in shared group : " + vertex + " --> " + slot);
			}
			
			return new ImmutablePair<SharedSlot, Locality>(slot, didNotGetPreferred ? Locality.NON_LOCAL : Locality.UNCONSTRAINED);
		}
		else {
			return null;
		}
	}
	
	
	void releaseSubSlot(SubSlot subslot, SharedSlot sharedSlot) {
		
		AbstractID groupId = subslot.getGroupId();
		
		synchronized (lock) {

			if (!allSlots.contains(sharedSlot)) {
				throw new IllegalArgumentException("Slot was not associated with this SlotSharingGroup before.");
			}
			
			int slotsRemaining = sharedSlot.releaseSlot(subslot);
			
			if (slotsRemaining == 0) {
				// this was the last sub slot. remove this from the availability list 
				// and trigger disposal
				try {
					allSlots.remove(sharedSlot);
					
					Instance location = sharedSlot.getAllocatedSlot().getInstance();

					if (groupId != null) {
						for (Map.Entry<AbstractID, Map<Instance, List<SharedSlot>>> mapEntry : availableSlotsPerJid.entrySet()) {
							AbstractID id = mapEntry.getKey();
							
							// hack: we identify co location hint entries by the fact that they are keyed
							//       by an abstract id, rather than a job vertex id
							if (id.getClass() == AbstractID.class || id.equals(groupId)) {
								continue;
							}
							
							Map<Instance, List<SharedSlot>> map = mapEntry.getValue();
							List<SharedSlot> list = map.get(location);
							if (list == null || !list.remove(sharedSlot)) {
								throw new IllegalStateException("Bug: SharedSlot was not available to another vertex type that it was not allocated for before.");
							}
							if (list.isEmpty()) {
								map.remove(location);
							}
						}
					}
				} finally {
					sharedSlot.dispose();
				}
			}
			else if (groupId != null) {
				// make the shared slot available to tasks within the group it available to
				Map<Instance, List<SharedSlot>> slotsForJid = availableSlotsPerJid.get(groupId);
				
				// sanity check
				if (slotsForJid == null) {
					throw new IllegalStateException("Trying to return a slot for group " + groupId + 
							" when available slots indicated that all slots were available.");
				}
				
				putIntoMultiMap(slotsForJid, sharedSlot.getAllocatedSlot().getInstance(), sharedSlot);
			}
		}
	}
	
	
	
	
	// --------------------------------------------------------------------------------------------
	//  State
	// --------------------------------------------------------------------------------------------
	
	public int getNumberOfSlots() {
		return allSlots.size();
	}
	
	public int getNumberOfAvailableSlotsForJid(JobVertexID jid) {
		synchronized (lock) {
			Map<Instance, List<SharedSlot>> available = availableSlotsPerJid.get(jid);
			
			if (available != null) {
				Set<SharedSlot> set = new HashSet<SharedSlot>();
				
				for (List<SharedSlot> list : available.values()) {
					for (SharedSlot slot : list) {
						set.add(slot);
					}
				}
				
				return set.size();
			} else {
				return allSlots.size();
			}
		}
	}

	// --------------------------------------------------------------------------------------------
	
	
	
	// --------------------------------------------------------------------------------------------
	//  Utilities
	// --------------------------------------------------------------------------------------------

	private static final void putIntoMultiMap(Map<Instance, List<SharedSlot>> map, Instance location, SharedSlot slot) {
		List<SharedSlot> slotsForInstance = map.get(location);
		if (slotsForInstance == null) {
			slotsForInstance = new ArrayList<SharedSlot>();
			map.put(location, slotsForInstance);
		}
		slotsForInstance.add(slot);
	}
	
	private static final SharedSlot removeFromMultiMap(Map<Instance, List<SharedSlot>> map, Instance location) {
		List<SharedSlot> slotsForLocation = map.get(location);
		
		if (slotsForLocation == null) {
			return null;
		}
		else {
			SharedSlot slot = slotsForLocation.remove(slotsForLocation.size() - 1);
			if (slotsForLocation.isEmpty()) {
				map.remove(location);
			}
			
			return slot;
		}
	}
	
	private static final SharedSlot pollFromMultiMap(Map<Instance, List<SharedSlot>> map) {
		Iterator<Map.Entry<Instance, List<SharedSlot>>> iter = map.entrySet().iterator();
		
		while (iter.hasNext()) {
			List<SharedSlot> slots = iter.next().getValue();
			
			if (slots.isEmpty()) {
				iter.remove();
			}
			else if (slots.size() == 1) {
				SharedSlot slot = slots.remove(0);
				iter.remove();
				return slot;
			}
			else {
				return slots.remove(slots.size() - 1);
			}
		}
		
		return null;
	}
}
