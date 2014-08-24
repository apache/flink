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

import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.instance.*;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.runtime.topology.NetworkNode;
import org.apache.flink.runtime.topology.NetworkTopology;
import org.apache.flink.util.StringUtils;

/**
 * A dummy implementation of an {@link org.apache.flink.runtime.instance.InstanceManager}.
 */
public final class TestInstanceManager implements InstanceManager {

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
	 * The test instance
	 */
	private final TestInstance testInstance;

	/**
	 * Test implementation of {@link org.apache.flink.runtime.instance.Instance}.
	 * 
	 */
	private static final class TestInstance extends Instance {

		/**
		 * Constructs a new test instance.
		 * 
		 * @param instanceConnectionInfo
		 *        the instance connection information
		 * @param parentNode
		 *        the parent node in the network topology
		 * @param networkTopology
		 *        the network topology
		 * @param hardwareDescription
		 *        the hardware description
		 * @param numberSlots
		 * 		  the number of slots available on the instance
		 */
		public TestInstance(final InstanceConnectionInfo instanceConnectionInfo,
				final NetworkNode parentNode, final NetworkTopology networkTopology,
				final HardwareDescription hardwareDescription, int numberSlots) {
			super(instanceConnectionInfo, parentNode, networkTopology, hardwareDescription, numberSlots);
		}
	}

	/**
	 * Constructs a new test instance manager
	 */
	public TestInstanceManager() {

		final HardwareDescription hd = HardwareDescriptionFactory.construct(1, 1L, 1L);

		this.allocatedResources = new ArrayList<AllocatedResource>();
		try {
			final InstanceConnectionInfo ici = new InstanceConnectionInfo(Inet4Address.getLocalHost(), 1, 1);
			final NetworkTopology nt = new NetworkTopology();
			this.testInstance = new TestInstance(ici, nt.getRootNode(), nt, hd, 1);
			this.allocatedResources.add(new AllocatedResource(testInstance, new AllocationID()));
		} catch (UnknownHostException e) {
			throw new RuntimeException(StringUtils.stringifyException(e));
		}
	}


	@Override
	public void requestInstance(final JobID jobID, final Configuration conf,
								int requiredSlots) throws InstanceException {

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
	public void releaseAllocatedResource(final AllocatedResource allocatedResource) throws InstanceException {
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
	public void reportHeartBeat(final InstanceConnectionInfo instanceConnectionInfo) {
		throw new IllegalStateException("reportHeartBeat called on TestInstanceManager");
	}

	@Override
	public void registerTaskManager(final InstanceConnectionInfo instanceConnectionInfo,
									final HardwareDescription hardwareDescription, int numberSlots){
		throw new IllegalStateException("registerTaskManager called on TestInstanceManager.");
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
	public Instance getInstanceByName(final String name) {
		throw new IllegalStateException("getInstanceByName called on TestInstanceManager");
	}

	@Override
	public void shutdown() {
		throw new IllegalStateException("shutdown called on TestInstanceManager");
	}

	@Override
	public int getNumberOfTaskManagers() {
		throw new IllegalStateException("getNumberOfTaskTrackers called on TestInstanceManager");
	}

	@Override
	public int getNumberOfSlots() {
		return this.testInstance.getNumberOfSlots();
	}


	@Override
	public Map<InstanceConnectionInfo, Instance> getInstances() {
		throw new IllegalStateException("getInstances called on TestInstanceManager");
	}
}
