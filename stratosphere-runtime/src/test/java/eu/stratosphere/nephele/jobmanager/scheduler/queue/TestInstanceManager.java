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

package eu.stratosphere.nephele.jobmanager.scheduler.queue;

import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.nephele.instance.AbstractInstance;
import eu.stratosphere.nephele.instance.AllocatedResource;
import eu.stratosphere.nephele.instance.AllocationID;
import eu.stratosphere.nephele.instance.HardwareDescription;
import eu.stratosphere.nephele.instance.HardwareDescriptionFactory;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.instance.InstanceException;
import eu.stratosphere.nephele.instance.InstanceListener;
import eu.stratosphere.nephele.instance.InstanceManager;
import eu.stratosphere.nephele.instance.InstanceRequestMap;
import eu.stratosphere.nephele.instance.InstanceType;
import eu.stratosphere.nephele.instance.InstanceTypeDescription;
import eu.stratosphere.nephele.instance.InstanceTypeDescriptionFactory;
import eu.stratosphere.nephele.instance.InstanceTypeFactory;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.topology.NetworkNode;
import eu.stratosphere.nephele.topology.NetworkTopology;
import eu.stratosphere.util.StringUtils;

/**
 * A dummy implementation of an {@link InstanceManager} used for the {@link QueueScheduler} unit tests.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public final class TestInstanceManager implements InstanceManager {

	/**
	 * The default instance type to be used during the tests.
	 */
	private static final InstanceType INSTANCE_TYPE = InstanceTypeFactory.construct("test", 1, 1, 1024, 1024, 10);

	/**
	 * The instances this instance manager is responsible of.
	 */
	private final Map<InstanceType, InstanceTypeDescription> instanceMap = new HashMap<InstanceType, InstanceTypeDescription>();

	/**
	 * Counts the number of times the method releaseAllocatedResource is called.
	 */
	private volatile int numberOfReleaseCalls = 0;

	/**
	 * The instance listener.
	 */
	private volatile InstanceListener instanceListener = null;

	/**
	 * The list of resources allocated to a job.
	 */
	private final List<AllocatedResource> allocatedResources;

	/**
	 * Test implementation of {@link AbstractInstance}.
	 * 
	 * @author warneke
	 */
	private static final class TestInstance extends AbstractInstance {

		/**
		 * Constructs a new test instance.
		 * 
		 * @param instanceType
		 *        the instance type
		 * @param instanceConnectionInfo
		 *        the instance connection information
		 * @param parentNode
		 *        the parent node in the network topology
		 * @param networkTopology
		 *        the network topology
		 * @param hardwareDescription
		 *        the hardware description
		 */
		public TestInstance(final InstanceType instanceType, final InstanceConnectionInfo instanceConnectionInfo,
				final NetworkNode parentNode, final NetworkTopology networkTopology,
				final HardwareDescription hardwareDescription) {
			super(instanceType, instanceConnectionInfo, parentNode, networkTopology, hardwareDescription);
		}
	}

	/**
	 * Constructs a new test instance manager
	 */
	public TestInstanceManager() {

		final HardwareDescription hd = HardwareDescriptionFactory.construct(1, 1L, 1L);
		final InstanceTypeDescription itd = InstanceTypeDescriptionFactory.construct(INSTANCE_TYPE, hd, 1);
		instanceMap.put(INSTANCE_TYPE, itd);

		this.allocatedResources = new ArrayList<AllocatedResource>();
		try {
			final InstanceConnectionInfo ici = new InstanceConnectionInfo(Inet4Address.getLocalHost(), 1, 1);
			final NetworkTopology nt = new NetworkTopology();
			final TestInstance ti = new TestInstance(INSTANCE_TYPE, ici, nt.getRootNode(), nt, hd);
			this.allocatedResources.add(new AllocatedResource(ti, INSTANCE_TYPE, new AllocationID()));
		} catch (UnknownHostException e) {
			throw new RuntimeException(StringUtils.stringifyException(e));
		}
	}


	@Override
	public void requestInstance(final JobID jobID, final Configuration conf,
			final InstanceRequestMap instanceRequestMap, final List<String> splitAffinityList) throws InstanceException {

		if (instanceRequestMap.size() != 1) {
			throw new InstanceException(
				"requestInstance of TestInstanceManager expected to receive request for a single instance type");
		}

		if (instanceRequestMap.getMinimumNumberOfInstances(INSTANCE_TYPE) != 1) {
			throw new InstanceException(
				"requestInstance of TestInstanceManager expected to receive request for one instance of type "
					+ INSTANCE_TYPE.getIdentifier());
		}

		if (this.instanceListener == null) {
			throw new InstanceException("instanceListener not registered with TestInstanceManager");
		}

		final InstanceListener il = this.instanceListener;

		final Runnable runnable = new Runnable() {

			/**
			 * {@inheritDoc}
			 */
			@Override
			public void run() {
				il.resourcesAllocated(jobID, allocatedResources);
			}
		};

		new Thread(runnable).start();
	}


	@Override
	public void releaseAllocatedResource(final JobID jobID, final Configuration conf,
			final AllocatedResource allocatedResource) throws InstanceException {

		++this.numberOfReleaseCalls;
	}

	/**
	 * Returns the number of times the method releaseAllocatedResource has been called.
	 * 
	 * @return the number of times the method releaseAllocatedResource has been called
	 */
	int getNumberOfReleaseMethodCalls() {

		return this.numberOfReleaseCalls;
	}


	@Override
	public InstanceType getSuitableInstanceType(final int minNumComputeUnits, final int minNumCPUCores,
			final int minMemorySize, final int minDiskCapacity, final int maxPricePerHour) {
		throw new IllegalStateException("getSuitableInstanceType called on TestInstanceManager");
	}


	@Override
	public void reportHeartBeat(final InstanceConnectionInfo instanceConnectionInfo,
			final HardwareDescription hardwareDescription) {
		throw new IllegalStateException("reportHeartBeat called on TestInstanceManager");
	}


	@Override
	public InstanceType getInstanceTypeByName(final String instanceTypeName) {
		throw new IllegalStateException("getInstanceTypeByName called on TestInstanceManager");
	}


	@Override
	public InstanceType getDefaultInstanceType() {

		return INSTANCE_TYPE;
	}


	@Override
	public NetworkTopology getNetworkTopology(final JobID jobID) {
		throw new IllegalStateException("getNetworkTopology called on TestInstanceManager");
	}


	@Override
	public void setInstanceListener(final InstanceListener instanceListener) {

		this.instanceListener = instanceListener;
	}


	@Override
	public Map<InstanceType, InstanceTypeDescription> getMapOfAvailableInstanceTypes() {

		return this.instanceMap;
	}


	@Override
	public AbstractInstance getInstanceByName(final String name) {
		throw new IllegalStateException("getInstanceByName called on TestInstanceManager");
	}


	@Override
	public void cancelPendingRequests(final JobID jobID) {
		throw new IllegalStateException("cancelPendingRequests called on TestInstanceManager");

	}


	@Override
	public void shutdown() {
		throw new IllegalStateException("shutdown called on TestInstanceManager");
	}

	@Override
	public int getNumberOfTaskTrackers() {
		throw new IllegalStateException("getNumberOfTaskTrackers called on TestInstanceManager");
	}
}
