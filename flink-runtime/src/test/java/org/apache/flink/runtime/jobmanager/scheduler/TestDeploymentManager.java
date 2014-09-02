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

import java.util.List;

import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.runtime.jobmanager.DeploymentManager;

/**
 * This class provides an implementation of the {@DeploymentManager} interface which is used during
 * the unit tests.
 * <p>
 * This class is thread-safe.
 * 
 */
public class TestDeploymentManager implements DeploymentManager {

	/**
	 * The ID of the job to be deployed.
	 */
	private volatile JobID jobID = null;

	/**
	 * The list of vertices to be deployed.
	 */
	private volatile List<ExecutionVertex> verticesToBeDeployed = null;

	/**
	 * Auxiliary object to synchronize on.
	 */
	private final Object synchronizationObject = new Object();


	@Override
	public void deploy(final JobID jobID, final Instance instance,
			final List<ExecutionVertex> verticesToBeDeployed) {

		this.jobID = jobID;
		this.verticesToBeDeployed = verticesToBeDeployed;

		synchronized (this.synchronizationObject) {
			this.synchronizationObject.notify();
		}
	}

	/**
	 * Returns the ID of the last deployed job.
	 */
	JobID getIDOfLastDeployedJob() {

		return this.jobID;
	}

	/**
	 * Returns a list of the last deployed vertices.
	 * 
	 * @return a list of the last deployed vertices
	 */
	List<ExecutionVertex> getListOfLastDeployedVertices() {

		return this.verticesToBeDeployed;
	}

	/**
	 * Clears the internal state of the test deployment manager.
	 */
	void clear() {

		this.jobID = null;
		this.verticesToBeDeployed = null;
	}

	/**
	 * Wait for the scheduler to complete the deployment.
	 */
	void waitForDeployment() {

		while (this.jobID == null) {
			synchronized (this.synchronizationObject) {
				try {
					this.synchronizationObject.wait(50);
				} catch (InterruptedException e) {
					// Ignore exception
				}
			}
		}
	}
}
