/**
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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.instance.AllocatedSlot;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.jobgraph.JobVertexID;

public class SlotSharingGroupAssignment {
	
	/** All slots currently allocated to this sharing group */
	private final Set<SharedSlot> allSlots = new LinkedHashSet<SharedSlot>();
	
	/** The slots available per vertex type (jid), keyed by instance, to make them locatable */
	private final Map<JobVertexID, Map<Instance, List<SharedSlot>>> availableSlotsPerJid = new LinkedHashMap<JobVertexID, Map<Instance, List<SharedSlot>>>();
	
	
	// --------------------------------------------------------------------------------------------
	
	
	public SubSlot addSlotWithTask(AllocatedSlot slot, JobVertexID jid) {
		
		final SharedSlot sharedSlot = new SharedSlot(slot, this);
		final Instance location = slot.getInstance();
		
		synchronized (allSlots) {
			// add to the total bookkeeping
			allSlots.add(sharedSlot);
			
			// allocate us a sub slot to return
			SubSlot subslot = sharedSlot.allocateSubSlot(jid);
			
			// preserve the locality information
			subslot.setLocality(slot.getLocality());
			
			boolean entryForNewJidExists = false;
			
			// let the other vertex types know about this one as well
			
			for (Map.Entry<JobVertexID, Map<Instance, List<SharedSlot>>> entry : availableSlotsPerJid.entrySet()) {
				
				if (entry.getKey().equals(jid)) {
					entryForNewJidExists = true;
					continue;
				}
				
				Map<Instance, List<SharedSlot>> available = entry.getValue();
				putIntoMultiMap(available, location, sharedSlot);
			}
			
			// make sure an empty entry exists for this jid, if no other entry exists
			if (!entryForNewJidExists) {
				availableSlotsPerJid.put(jid, new LinkedHashMap<Instance, List<SharedSlot>>());
			}
			
			return subslot;
		}
	}
	
	public AllocatedSlot getSlotForTask(JobVertexID jid, ExecutionVertex vertex, boolean localOnly) {
		synchronized (allSlots) {
			return getSlotForTaskInternal(jid, vertex.getPreferredLocations(), localOnly);
		}
	}
	
	public boolean sharedSlotAvailableForJid(SharedSlot slot, JobVertexID jid, boolean lastSubSlot) {
		if (slot == null || jid == null) {
			throw new NullPointerException();
		}
		
		synchronized (allSlots) {
			if (!allSlots.contains(slot)) {
				throw new IllegalArgumentException("Slot was not associated with this SlotSharingGroup before.");
			}
			
			if (lastSubSlot) {
				// this was the last sub slot. unless there is something pending for this jid
				// remove this from the availability list of all jids and 
				// return that this one is good to release
				allSlots.remove(slot);
				
				Instance location = slot.getAllocatedSlot().getInstance();
				
				for (Map.Entry<JobVertexID, Map<Instance, List<SharedSlot>>> mapEntry : availableSlotsPerJid.entrySet()) {
					if (mapEntry.getKey().equals(jid)) {
						continue;
					}
					
					Map<Instance, List<SharedSlot>> map = mapEntry.getValue();
					List<SharedSlot> list = map.get(location);
					if (list == null || !list.remove(slot)) {
						throw new IllegalStateException("SharedSlot was not available to another vertex type that it was not allocated for before.");
					}
					if (list.isEmpty()) {
						map.remove(location);
					}
				}
				
				return true;
			}
			
			Map<Instance, List<SharedSlot>> slotsForJid = availableSlotsPerJid.get(jid);
			
			// sanity check
			if (slotsForJid == null) {
				throw new IllegalStateException("Trying to return a slot for jid " + jid + 
						" when available slots indicated that all slots were available.");
			}
			
			putIntoMultiMap(slotsForJid, slot.getAllocatedSlot().getInstance(), slot);
			
			// do not release, we are still depending on this shared slot
			return false;
		}
	}
	
	
	/**
	 * NOTE: This method is not synchronized by itself, needs to be synchronized externally.
	 * 
	 * @param jid
	 * @return An allocated sub slot, or {@code null}, if no slot is available.
	 */
	private AllocatedSlot getSlotForTaskInternal(JobVertexID jid, Iterable<Instance> preferredLocations, boolean localOnly) {
		if (allSlots.isEmpty()) {
			return null;
		}
		
		Map<Instance, List<SharedSlot>> slotsForJid = availableSlotsPerJid.get(jid);
		
		// get the available slots for the vertex type (jid)
		if (slotsForJid == null) {
			// no task is yet scheduled for that jid, so all slots are available
			slotsForJid = new LinkedHashMap<Instance, List<SharedSlot>>();
			availableSlotsPerJid.put(jid, slotsForJid);
			
			for (SharedSlot availableSlot : allSlots) {
				putIntoMultiMap(slotsForJid, availableSlot.getAllocatedSlot().getInstance(), availableSlot);
			}
		}
		else if (slotsForJid.isEmpty()) {
			return null;
		}
		
		// check whether we can schedule the task to a preferred location
		boolean didNotGetPreferred = false;
		
		if (preferredLocations != null) {
			for (Instance location : preferredLocations) {
				
				// set the flag that we failed a preferred location. If one will be found,
				// we return early anyways and skip the flag evaluation
				didNotGetPreferred = true;
				
				SharedSlot slot = removeFromMultiMap(slotsForJid, location);
				if (slot != null) {
					SubSlot subslot = slot.allocateSubSlot(jid);
					subslot.setLocality(Locality.LOCAL);
					return subslot;
				}
			}
		}
		
		// if we want only local assignments, exit now with a "not found" result
		if (didNotGetPreferred && localOnly) {
			return null;
		}
		
		// schedule the task to any available location
		SharedSlot slot = pollFromMultiMap(slotsForJid);
		if (slot != null) {
			SubSlot subslot = slot.allocateSubSlot(jid);
			subslot.setLocality(didNotGetPreferred ? Locality.NON_LOCAL : Locality.UNCONSTRAINED);
			return subslot;
		}
		else {
			return null;
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//  State
	// --------------------------------------------------------------------------------------------
	
	public int getNumberOfSlots() {
		return allSlots.size();
	}
	
	public int getNumberOfAvailableSlotsForJid(JobVertexID jid) {
		synchronized (allSlots) {
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
