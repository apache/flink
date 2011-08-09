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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.instance.AllocatedResource;
import eu.stratosphere.nephele.instance.AllocationID;
import eu.stratosphere.nephele.instance.HardwareDescription;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.instance.InstanceException;
import eu.stratosphere.nephele.instance.InstanceListener;
import eu.stratosphere.nephele.instance.InstanceManager;
import eu.stratosphere.nephele.instance.InstanceType;
import eu.stratosphere.nephele.instance.InstanceTypeDescription;
import eu.stratosphere.nephele.instance.InstanceTypeDescriptionFactory;
import eu.stratosphere.nephele.instance.local.LocalInstanceManager;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.topology.NetworkTopology;

/**
 * Minimalist, mocked {@link InstanceManager} having only localhost as its
 * resource.
 * 
 * @author Arvid Heise
 */
class MockInstanceManager implements InstanceManager {

	public static InstanceType DEFAULT_INSTANCE_TYPE = LocalInstanceManager.createDefaultInstanceType();

	@SuppressWarnings("serial")
	private static final HashMap<InstanceType, InstanceTypeDescription> TYPE_DESCRIPTIONS = new HashMap<InstanceType, InstanceTypeDescription>() {
		{
			put(DEFAULT_INSTANCE_TYPE,
				InstanceTypeDescriptionFactory.construct(DEFAULT_INSTANCE_TYPE, MockInstance.DESCRIPTION, 1));
		}
	};

	private static final NetworkTopology NETWORK_TOPOLOGY = NetworkTopology
		.createEmptyTopology();

	public final static MockInstanceManager INSTANCE = new MockInstanceManager();

	//
	// public static MockInstanceManager getInstance() {
	// return INSTANCE;
	// }

	private final AllocatedResource allocatedResource = new AllocatedResource(
		new MockInstance(DEFAULT_INSTANCE_TYPE, NETWORK_TOPOLOGY), DEFAULT_INSTANCE_TYPE, new AllocationID());

	private InstanceListener instanceListener;

	@Override
	public InstanceType getDefaultInstanceType() {
		return DEFAULT_INSTANCE_TYPE;
	}

	@Override
	public InstanceType getInstanceTypeByName(final String instanceTypeName) {
		return DEFAULT_INSTANCE_TYPE;
	}

	@Override
	public NetworkTopology getNetworkTopology(final JobID jobID) {
		return NETWORK_TOPOLOGY;
	}

	@Override
	public InstanceType getSuitableInstanceType(final int minNumComputeUnits,
			final int minNumCPUCores, final int minMemorySize,
			final int minDiskCapacity, final int maxPricePerHour) {
		return DEFAULT_INSTANCE_TYPE;
	}

	@Override
	public void releaseAllocatedResource(final JobID jobID,
			final Configuration conf, final AllocatedResource allocatedResource)
			throws InstanceException {
	}

	@Override
	public void reportHeartBeat(InstanceConnectionInfo instanceConnectionInfo,
			HardwareDescription hardwareDescription) {
	}

	@Override
	public void requestInstance(final JobID jobID, Configuration conf, Map<InstanceType, Integer> instanceMap,
			List<String> splitAffinityList) throws InstanceException {
		new Thread(new Runnable() {
			@Override
			public void run() {
				instanceListener.resourceAllocated(jobID, allocatedResource);
			}
		}).start();
	}

	@Override
	public void setInstanceListener(final InstanceListener instanceListener) {
		this.instanceListener = instanceListener;
	}

	@Override
	public void shutdown() {
	}

	@Override
	public Map<InstanceType, InstanceTypeDescription> getMapOfAvailableInstanceTypes() {
		return TYPE_DESCRIPTIONS;
	}

}