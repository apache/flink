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

package eu.stratosphere.pact.testing;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import eu.stratosphere.nephele.client.JobCancelResult;
import eu.stratosphere.nephele.client.JobProgressResult;
import eu.stratosphere.nephele.client.JobSubmissionResult;
import eu.stratosphere.nephele.event.job.AbstractEvent;
import eu.stratosphere.nephele.event.job.RecentJobEvent;
import eu.stratosphere.nephele.instance.InstanceType;
import eu.stratosphere.nephele.instance.InstanceTypeDescription;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.managementgraph.ManagementGraph;
import eu.stratosphere.nephele.managementgraph.ManagementVertexID;
import eu.stratosphere.nephele.protocols.ExtendedManagementProtocol;
import eu.stratosphere.nephele.topology.NetworkTopology;
import eu.stratosphere.nephele.types.IntegerRecord;
import eu.stratosphere.nephele.types.StringRecord;

final class MockJobManager implements ExtendedManagementProtocol {
	private static final MockJobManager INSTANCE = new MockJobManager();

	@Override
	public JobSubmissionResult submitJob(JobGraph job) throws IOException {
		return null;
	}

	@Override
	public IntegerRecord getRecommendedPollingInterval() throws IOException {
		return null;
	}

	@Override
	public JobProgressResult getJobProgress(JobID jobID) throws IOException {
		return null;
	}

	@Override
	public JobCancelResult cancelJob(JobID jobID) throws IOException {
		return null;
	}

	@Override
	public void killInstance(StringRecord instanceName) throws IOException {
	}

	@Override
	public List<RecentJobEvent> getRecentJobs() throws IOException {
		return null;
	}

	@Override
	public NetworkTopology getNetworkTopology(JobID jobID) throws IOException {
		return null;
	}

	@Override
	public Map<InstanceType, InstanceTypeDescription> getMapOfAvailableInstanceTypes() throws IOException {
		return MockInstanceManager.getInstance().getMapOfAvailableInstanceTypes();
	}

	@Override
	public ManagementGraph getManagementGraph(JobID jobID) throws IOException {
		return null;
	}

	@Override
	public List<AbstractEvent> getEvents(JobID jobID) throws IOException {
		return null;
	}

	@Override
	public void cancelTask(JobID jobID, ManagementVertexID id) throws IOException {
	}

	public static Object getInstance() {
		return INSTANCE;
	}

	@Override
	public void logBufferUtilization(JobID jobID) throws IOException {
	}
}