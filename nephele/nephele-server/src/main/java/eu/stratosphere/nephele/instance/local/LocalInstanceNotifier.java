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

package eu.stratosphere.nephele.instance.local;

import eu.stratosphere.nephele.instance.AllocatedResource;
import eu.stratosphere.nephele.instance.InstanceListener;
import eu.stratosphere.nephele.jobgraph.JobID;

/**
 * This class is an auxiliary class to send the notification
 * about the availability of an {@link AllocatedResource} to the given {@link InstanceListener} object. The notification
 * must be sent from
 * a separate thread, otherwise the atomic operation of requesting an instance
 * for a vertex and switching to the state ASSINING could not be guaranteed.
 * This class is thread-safe.
 * 
 * @author warneke
 */
public class LocalInstanceNotifier extends Thread {

	/**
	 * The {@link InstanceListener} object to send the notification to.
	 */
	private final InstanceListener instanceNotifiable;

	/**
	 * The ID of the job the new instance belongs to.
	 */
	private final JobID jobID;

	/**
	 * The resource allcoated for the job.
	 */
	private final AllocatedResource allocatedResource;

	/**
	 * Constructs a new instance notifier object.
	 * 
	 * @param instanceNotifiable
	 *        the notifiable object to send the notification to
	 * @param jobID
	 *        the ID of the job the newly allocated resources belongs to
	 * @param allocatedResource
	 *        the resource allocated for the job
	 */
	public LocalInstanceNotifier(InstanceListener instanceNotifiable, JobID jobID, AllocatedResource allocatedResource) {
		this.instanceNotifiable = instanceNotifiable;
		this.jobID = jobID;
		this.allocatedResource = allocatedResource;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void run() {

		this.instanceNotifiable.resourceAllocated(this.jobID, this.allocatedResource);

	}
}
