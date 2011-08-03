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

package eu.stratosphere.nephele.instance.ec2;

import eu.stratosphere.nephele.instance.AbstractInstance;
import eu.stratosphere.nephele.instance.AllocatedResource;
import eu.stratosphere.nephele.instance.AllocationID;
import eu.stratosphere.nephele.instance.HardwareDescription;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.instance.InstanceType;
import eu.stratosphere.nephele.topology.NetworkNode;
import eu.stratosphere.nephele.topology.NetworkTopology;

/**
 * A CloudInstance is a concrete implementation of the {@link Instance} interface for instances running
 * inside a cloud. Typically a cloud instance represents a virtual machine that can be controlled by
 * a cloud management system.
 * 
 * @author warneke
 */
public class EC2CloudInstance extends AbstractInstance {

	/**
	 * The cached allocated resource object.
	 */
	private final AllocatedResource allocatedResource;

	/** The instance ID. */
	private final String instanceID;
	
	/** The AWS Access Key to access this machine */
	private String awsAccessKey;
	
	/** The AWS Secret Key to access this machine */
	private String awsSecretKey;
	
	/** The time the instance was allocated. */
	private final long launchTime;

	/** The last received heart beat. */
	private long lastReceivedHeartBeat = System.currentTimeMillis();
	
	

	/**
	 * Creates a new cloud instance.
	 * 
	 * @param instanceID
	 *        the instance ID assigned by the cloud management system
	 * @param type
	 *        the instance type
	 * @param instanceConnectionInfo
	 *        the information required to connect to the instance's task manager
	 * @param allocationTime
	 *        the time the instance was allocated
	 * @param parentNode
	 *        the parent node in the network topology
	 * @param hardwareDescription
	 *        the hardware description reported by the instance itself
	 * @param awsAccessKey
	 * 		  The AWS Access Key to access this machine
	 * @param awsSecretKey
	 * 		  The AWS Secret Key to access this machine
	 */
	public EC2CloudInstance(String instanceID, InstanceType type, 
			InstanceConnectionInfo instanceConnectionInfo, long launchTime, NetworkNode parentNode,
			NetworkTopology networkTopology, HardwareDescription hardwareDescription, String awsAccessKey, String awsSecretKey) {
		super(type, instanceConnectionInfo, parentNode, networkTopology, hardwareDescription);

		this.allocatedResource = new AllocatedResource(this, type, new AllocationID());

		this.instanceID = instanceID;

		this.launchTime = launchTime;
		
		this.awsAccessKey = awsAccessKey;
		
		this.awsSecretKey = awsSecretKey;
	}

	/**
	 * Returns the instance ID.
	 * 
	 * @return the instance ID
	 */
	public String getInstanceID() {
		return this.instanceID;
	}

	/**
	 * Returns the time of last received heart beat.
	 * 
	 * @return the time of last received heart beat
	 */
	public long getLastReceivedHeartBeat() {
		return this.lastReceivedHeartBeat;
	}

	/**
	 * Updates the time of last received heart beat to the current system time.
	 */
	public void updateLastReceivedHeartBeat() {
		this.lastReceivedHeartBeat = System.currentTimeMillis();
	}

	/**
	 * Returns the time the instance was allocated.
	 * 
	 * @return the time the instance was allocated
	 */
	public long getAllocationTime() {
		return this.launchTime;
	}

	public AllocatedResource asAllocatedResource() {
		return this.allocatedResource;
	}

	/**
	 * Returns this Instance as a FloatingInstance object
	 * @return
	 */
	public FloatingInstance asFloatingInstance(){
		return new FloatingInstance(this.instanceID, this.getInstanceConnectionInfo(), this.launchTime, this.getType(), this.awsAccessKey, this.awsSecretKey);
	}
}
