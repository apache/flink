/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.nephele.instance;


import java.util.Map;

import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.topology.NetworkTopology;

public interface InstanceManager {


	void shutdown();

	void releaseAllocatedResource(AllocatedResource allocatedResource) throws InstanceException;

	void reportHeartBeat(InstanceConnectionInfo instanceConnectionInfo);

	void registerTaskManager(InstanceConnectionInfo instanceConnectionInfo,
									HardwareDescription hardwareDescription, int numberOfSlots);
	void requestInstance(JobID jobID, Configuration conf,  int requiredSlots)
			throws InstanceException;

	NetworkTopology getNetworkTopology(JobID jobID);

	void setInstanceListener(InstanceListener instanceListener);

	Instance getInstanceByName(String name);

	int getNumberOfTaskTrackers();

	int getNumberOfSlots();
	
	Map<InstanceConnectionInfo, Instance> getInstances();
}
