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

package eu.stratosphere.nephele.instance.ec2;

import java.util.List;

import eu.stratosphere.nephele.instance.AbstractInstance;
import eu.stratosphere.nephele.instance.AllocatedResource;
import eu.stratosphere.nephele.instance.InstanceListener;
import eu.stratosphere.nephele.jobgraph.JobID;

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
final class EC2CloudInstanceNotifier extends Thread {

	/**
	 * The {@link InstanceListener} object to send the notification to.
	 */
	private final InstanceListener instanceListener;

	private final List<AllocatedResource> allocatedResources;

	private final JobID jobID;

	/**
	 * Constructs a new instance notifier object.
	 * 
	 * @param instanceListener
	 *        the listener to send the notification to
	 * @param jobID
	 *        the ID of the job the notification refers to
	 * @param allocatedResources
	 *        the resources which have been allocated for the job with the given ID
	 */
	public EC2CloudInstanceNotifier(final InstanceListener instanceListener, final JobID jobID,
			final List<AllocatedResource> allocatedResources) {
		this.instanceListener = instanceListener;
		this.jobID = jobID;
		this.allocatedResources = allocatedResources;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void run() {
		this.instanceListener.resourcesAllocated(this.jobID, this.allocatedResources);
	}
}
