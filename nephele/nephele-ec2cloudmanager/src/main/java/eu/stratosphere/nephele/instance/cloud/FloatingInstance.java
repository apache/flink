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

import eu.stratosphere.nephele.instance.InstanceConnectionInfo;

/**
 * A FloatingInstance is an instance in the cloud allocated for a user. It is idle and carries out no task.
 * However, the owner of a floating instance can employ it for executing new jobs until it is terminated.
 */
public class FloatingInstance {

	/** The instance ID. */
	private final String instanceID;

	/** The information required to connect to the instance's task manager. */
	private final InstanceConnectionInfo instanceConnectionInfo;

	/** The time the instance was launched (in this case, the VM). */
	private final long launchTime;


	/** The last received heart beat. */
	private long lastHeartBeat;

	/**
	 * Creates a new floating instance.
	 * 
	 * @param instanceID
	 *        the instance ID assigned by the cloud management system
	 * @param instanceConnectionInfo
	 *        the information required to connect to the instance's task manager
	 * @param launchTime
	 *        the time the instance was allocated
	 * @param remainingTime
	 *        the survival time for the instance
	 */
	public FloatingInstance(String instanceID, InstanceConnectionInfo instanceConnectionInfo, long launchTime) {
		this.instanceID = instanceID;
		this.instanceConnectionInfo = instanceConnectionInfo;
		this.launchTime = launchTime;
		this.lastHeartBeat = System.currentTimeMillis();
	}

	/***
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
		return this.lastHeartBeat;
	}

	/**
	 * Updates the time of last received heart beat to the current system time.
	 */
	public void updateLastReceivedHeartBeat() {
		this.lastHeartBeat = System.currentTimeMillis();
	}

	/**
	 * Returns the information required to connect to the instance's task manager.
	 * 
	 * @return the information required to connect to the instance's task manager
	 */
	public InstanceConnectionInfo getInstanceConnectionInfo() {
		return this.instanceConnectionInfo;
	}

	/**
	 * Returns the time the instance was launched.
	 * 
	 * @return the time the instance was allocated
	 */
	public long getLaunchTime() {
		return this.launchTime;
	}


}
