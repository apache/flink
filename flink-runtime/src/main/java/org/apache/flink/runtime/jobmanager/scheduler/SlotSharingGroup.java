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

import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobgraph.JobVertexID;

/**
 * A slot sharing units defines which different task (from different job vertices) can be
 * deployed together within a slot. This is a soft permission, in contrast to the hard constraint
 * defined by a co-location hint.
 */
public class SlotSharingGroup implements java.io.Serializable {
	
	private static final long serialVersionUID = 1L;

	private final Set<JobVertexID> ids = new TreeSet<JobVertexID>();
	private final SlotSharingGroupId slotSharingGroupId = new SlotSharingGroupId();
	
	public SlotSharingGroup() {}
	
	public SlotSharingGroup(JobVertexID ... sharedVertices) {
		for (JobVertexID id : sharedVertices) {
			this.ids.add(id);
		}
	}

	// --------------------------------------------------------------------------------------------
	
	public void addVertexToGroup(JobVertexID id) {
		this.ids.add(id);
	}
	
	public void removeVertexFromGroup(JobVertexID id) {
		this.ids.remove(id);
	}
	
	public Set<JobVertexID> getJobVertexIds() {
		return Collections.unmodifiableSet(ids);
	}

	public SlotSharingGroupId getSlotSharingGroupId() {
		return slotSharingGroupId;
	}
	
	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------
	
	@Override
	public String toString() {
		return "SlotSharingGroup " + this.ids.toString();
	}
}
