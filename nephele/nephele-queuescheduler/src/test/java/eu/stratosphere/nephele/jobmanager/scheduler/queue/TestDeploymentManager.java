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

package eu.stratosphere.nephele.jobmanager.scheduler.queue;

import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.instance.AbstractInstance;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.jobmanager.DeploymentManager;

/**
 * This class provides an implementation of the {@DeploymentManager} interface which is used during
 * the unit tests.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public class TestDeploymentManager implements DeploymentManager {

	/**
	 * The ID of the job to be deployed.
	 */
	private volatile JobID jobID = null;

	/**
	 * The list of vertices to be deployed.
	 */
	private final ArrayList<ExecutionVertex> verticesToBeDeployed = new ArrayList<ExecutionVertex>();

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void deploy(final JobID jobID, final AbstractInstance instance,
			final List<ExecutionVertex> verticesToBeDeployed) {

		this.jobID = jobID;
		synchronized (this.verticesToBeDeployed) {
			this.verticesToBeDeployed.addAll(verticesToBeDeployed);
			this.verticesToBeDeployed.notify();
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

		synchronized (this.verticesToBeDeployed) {
			return new ArrayList<ExecutionVertex>(this.verticesToBeDeployed);
		}
	}

	/**
	 * Clears the internal state of the test deployment manager.
	 */
	void clear() {

		this.jobID = null;
		synchronized (this.verticesToBeDeployed) {
			this.verticesToBeDeployed.clear();
		}
	}

	/**
	 * Wait for the scheduler to complete the deployment.
	 * 
	 * @param expectedNumberOfDeployedVertices
	 *        the expected number of vertices to be deployed
	 */
	void waitForDeployment(final int expectedNumberOfDeployedVertices) {

		try {
			synchronized (this.verticesToBeDeployed) {

				while (this.verticesToBeDeployed.size() < expectedNumberOfDeployedVertices) {
					this.verticesToBeDeployed.wait();
				}
			}
		} catch (InterruptedException ie) {
			// Ignore exception
		}
	}
}
