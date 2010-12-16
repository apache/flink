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

package eu.stratosphere.nephele.instance.cloud;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.nephele.instance.InstanceConnectionInfo;

/**
 * A JobToInstancesMapping represents the relationship between a job and its possessing cloud instances. A cloud
 * instance can be assigned or unassigned to a job.
 */
public class JobToInstancesMapping {

	/** The list of assigned cloud instances for the job. */
	private final List<CloudInstance> assignedInstances = new ArrayList<CloudInstance>();

	/** The owner of the job. */
	private final String owner;

	/** The access ID into Amazon Web Services. */
	private final String awsAccessId;

	/** The secret key used to generate signatures for authentication. */
	private final String awsSecretKey;

	/**
	 * Creates a new mapping for job to instances.
	 * 
	 * @param owner
	 *        the owner of the job
	 * @param awsAccessId
	 *        the access ID into AWS
	 * @param awsSecretKey
	 *        the secret key used to generate signatures for authentication
	 */
	public JobToInstancesMapping(String owner, String awsAccessId, String awsSecretKey) {
		this.owner = owner;
		this.awsAccessId = awsAccessId;
		this.awsSecretKey = awsSecretKey;
	}

	/**
	 * Assigns a cloud instance to the job.
	 * 
	 * @param instance
	 *        the cloud instance which will be assigned
	 */
	public void assignInstanceToJob(CloudInstance instance) {

		System.out.println("Adding " + instance.getInstanceConnectionInfo() + " to job");

		synchronized (this.assignedInstances) {
			this.assignedInstances.add(instance);
		}
	}

	/**
	 * Unassigns a cloud instance from the job.
	 * 
	 * @param instance
	 *        the cloud instance which will be unassigned
	 * @return the unassigned cloud instance
	 */
	public boolean unassignedInstanceFromJob(CloudInstance instance) {

		synchronized (this.assignedInstances) {
			return this.assignedInstances.remove(instance);
		}
	}

	/**
	 * Returns the number of assigned cloud instances for the job.
	 * 
	 * @return the number of assigned cloud instances for the job
	 */
	public int getNumberOfAssignedInstances() {
		return this.assignedInstances.size();
	}

	/**
	 * Returns the list of assigned cloud instances for the job.
	 * 
	 * @return the list of assigned cloud instances for the job
	 */
	public List<CloudInstance> getAssignedInstances() {
		return this.assignedInstances;
	}

	/**
	 * Returns the cloud instance matching the given connection information.
	 * 
	 * @param instanceConnectionInfo
	 *        the {@link InstanceConnectionInfo} object identifying the instance
	 * @return the cloud instance matching the given connection information or <code>null</code> if no matching instance
	 *         exists
	 */
	public CloudInstance getInstanceByConnectionInfo(InstanceConnectionInfo instanceConnectionInfo) {

		if (instanceConnectionInfo == null) {
			return null;
		}

		synchronized (this.assignedInstances) {

			final Iterator<CloudInstance> it = this.assignedInstances.iterator();

			while (it.hasNext()) {

				final CloudInstance ci = it.next();
				if (instanceConnectionInfo.equals(ci.getInstanceConnectionInfo())) {
					return ci;
				}
			}
		}

		return null;
	}

	/**
	 * Returns the owner of the job.
	 * 
	 * @return the owner of the job
	 */
	public String getOwner() {
		return this.owner;
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
