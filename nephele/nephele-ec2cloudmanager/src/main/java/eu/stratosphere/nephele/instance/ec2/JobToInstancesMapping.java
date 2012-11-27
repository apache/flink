/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import eu.stratosphere.nephele.instance.AbstractInstance;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;

/**
 * A JobToInstancesMapping represents the relationship between a job and its possessing cloud instances. A cloud
 * instance can be assigned or unassigned to a job.
 */
final class JobToInstancesMapping {

	/** The list of assigned cloud instances for the job. */
	private final Map<InstanceConnectionInfo, EC2CloudInstance> assignedInstances = new HashMap<InstanceConnectionInfo, EC2CloudInstance>();

	/** The access ID into Amazon Web Services. */
	private final String awsAccessId;

	/** The secret key used to generate signatures for authentication. */
	private final String awsSecretKey;

	/**
	 * Creates a new mapping for job to instances.
	 * 
	 * @param awsAccessId
	 *        the access ID into AWS
	 * @param awsSecretKey
	 *        the secret key used to generate signatures for authentication
	 */
	public JobToInstancesMapping(final String awsAccessId, final String awsSecretKey) {

		this.awsAccessId = awsAccessId;
		this.awsSecretKey = awsSecretKey;
	}

	/**
	 * Assigns a cloud instance to the job.
	 * 
	 * @param instance
	 *        the cloud instance which will be assigned
	 */
	public void assignInstanceToJob(final EC2CloudInstance instance) {

		synchronized (this.assignedInstances) {
			this.assignedInstances.put(instance.getInstanceConnectionInfo(), instance);
		}
	}

	/**
	 * Unassigns a cloud instance from the job.
	 * 
	 * @param instance
	 *        the cloud instance which will be unassigned
	 * @return <code>true</code> if the given cloud instance has been assigned to the job, <code>false</code> otherwise
	 */
	public boolean unassignInstanceFromJob(final AbstractInstance instance) {

		synchronized (this.assignedInstances) {
			return (this.assignedInstances.remove(instance.getInstanceConnectionInfo()) != null);
		}
	}

	/**
	 * Unassigns all currently assigned instances from that job and returns them.
	 * 
	 * @return the list of instances previously assigned to this job. The list is possibly empty.
	 */
	public List<EC2CloudInstance> unassignAllInstancesFromJob() {

		final List<EC2CloudInstance> unassignedInstances;

		synchronized (this.assignedInstances) {
			unassignedInstances = new ArrayList<EC2CloudInstance>(this.assignedInstances.values());
			this.assignedInstances.clear();
		}

		return unassignedInstances;
	}

	/**
	 * Returns the number of assigned cloud instances for the job.
	 * 
	 * @return the number of assigned cloud instances for the job
	 */
	public int getNumberOfAssignedInstances() {

		synchronized (this.assignedInstances) {
			return this.assignedInstances.size();
		}
	}

	/**
	 * Returns the cloud instance matching the given connection information.
	 * 
	 * @param instanceConnectionInfo
	 *        the {@link InstanceConnectionInfo} object identifying the instance
	 * @return the cloud instance matching the given connection information or <code>null</code> if no matching instance
	 *         exists
	 */
	public EC2CloudInstance getInstanceByConnectionInfo(final InstanceConnectionInfo instanceConnectionInfo) {

		if (instanceConnectionInfo == null) {
			return null;
		}

		synchronized (this.assignedInstances) {
			return this.assignedInstances.get(instanceConnectionInfo);
		}
	}

	/**
	 * Returns the access ID into AWS.
	 * 
	 * @return the access ID into AWS
	 */
	public String getAwsAccessId() {
		return this.awsAccessId;
	}

	/**
	 * Returns the secret key used to generate signatures for authentication.
	 * 
	 * @return the secret key used to generate signatures for authentication
	 */
	public String getAwsSecretKey() {
		return this.awsSecretKey;
	}

}
