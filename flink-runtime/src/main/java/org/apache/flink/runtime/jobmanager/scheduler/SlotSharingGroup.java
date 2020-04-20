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

import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A slot sharing units defines which different task (from different job vertices) can be
 * deployed together within a slot. This is a soft permission, in contrast to the hard constraint
 * defined by a co-location hint.
 */
public class SlotSharingGroup implements java.io.Serializable {

	private static final long serialVersionUID = 1L;

	private final Set<JobVertexID> ids = new TreeSet<>();

	private final SlotSharingGroupId slotSharingGroupId = new SlotSharingGroupId();

	/** Represents resources of all tasks in the group. Default to be zero.
	 * Any task with UNKNOWN resources will turn it to be UNKNOWN. */
	private ResourceSpec resourceSpec = ResourceSpec.ZERO;

	// --------------------------------------------------------------------------------------------

	public void addVertexToGroup(final JobVertexID id, final ResourceSpec resource) {
		ids.add(checkNotNull(id));
		resourceSpec = resourceSpec.merge(checkNotNull(resource));
	}

	public void removeVertexFromGroup(final JobVertexID id, final ResourceSpec resource) {
		ids.remove(checkNotNull(id));
		resourceSpec = resourceSpec.subtract(checkNotNull(resource));
	}

	public Set<JobVertexID> getJobVertexIds() {
		return Collections.unmodifiableSet(ids);
	}

	public SlotSharingGroupId getSlotSharingGroupId() {
		return slotSharingGroupId;
	}

	public ResourceSpec getResourceSpec() {
		return resourceSpec;
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		return "SlotSharingGroup " + this.ids.toString();
	}
}
