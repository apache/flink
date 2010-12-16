/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.instance.cluster;

import eu.stratosphere.nephele.instance.AbstractInstance;
import eu.stratosphere.nephele.instance.AllocatedResource;
import eu.stratosphere.nephele.instance.InstanceListener;

/**
 * This class is an auxiliary class to send the notification
 * about the availability of an {@link AbstractInstance} to the given {@link InstanceListener} object. The notification
 * must be sent from
 * a separate thread, otherwise the atomic operation of requesting an instance
 * for a vertex and switching to the state ASSINING could not be guaranteed.
 * This class is thread-safe.
 * 
 * @author warneke
 */
public class ClusterInstanceNotifier extends Thread {

	/**
	 * The {@link InstanceListener} object to send the notification to.
	 */
	private final InstanceListener instanceNotifiable;

	/**
	 * The instance the notification refers to.
	 */
	private final AllocatedSlice allocatedSlice;

	/**
	 * Constructs a new instance notifier object.
	 * 
	 * @param instanceNotifiable
	 *        the notifiable object to send the notification to
	 * @param allocatedSlice
	 *        the slice with has been allocated for the job
	 */
	public ClusterInstanceNotifier(InstanceListener instanceNotifiable, AllocatedSlice allocatedSlice) {
		this.instanceNotifiable = instanceNotifiable;
		this.allocatedSlice = allocatedSlice;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void run() {

		this.instanceNotifiable.resourceAllocated(this.allocatedSlice.getJobID(), new AllocatedResource(
			this.allocatedSlice.getHostingInstance(), this.allocatedSlice.getAllocationID()));

	}
}
