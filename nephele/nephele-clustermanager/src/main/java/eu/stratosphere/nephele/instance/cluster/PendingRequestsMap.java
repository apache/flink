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

package eu.stratosphere.nephele.instance.cluster;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import eu.stratosphere.nephele.instance.InstanceType;

/**
 * This class represents a pending request, i.e. a request for a particular type and number of {@link AbstractInstance}
 * objects which could not be fulfilled yet.
 * <p>
 * This class is not thread-safe.
 * 
 * @author warneke
 */
public final class PendingRequestsMap {

	/**
	 * The map storing the pending instance requests for the job this pending request object belongs to.
	 */
	private final Map<InstanceType, Integer> pendingRequests = new HashMap<InstanceType, Integer>();

	/**
	 * Checks if the job this object belongs to has pending instance requests.
	 * 
	 * @return <code>true</code> if the job this object belongs to has pending instance requests, <code>false</code>
	 *         otherwise
	 */
	boolean hasPendingRequests() {

		return !(this.pendingRequests.isEmpty());
	}

	/**
	 * Adds the a pending request for the given number of instances of the given type to this map.
	 * 
	 * @param instanceType
	 *        the requested instance type
	 * @param numberOfInstances
	 *        the requested number of instances of this type
	 */
	void addRequest(final InstanceType instanceType, final int numberOfInstances) {

		Integer numberOfRemainingInstances = this.pendingRequests.get(instanceType);
		if (numberOfRemainingInstances == null) {
			numberOfRemainingInstances = Integer.valueOf(numberOfInstances);
		} else {
			numberOfRemainingInstances = Integer.valueOf(numberOfRemainingInstances.intValue() + numberOfInstances);
		}

		this.pendingRequests.put(instanceType, numberOfRemainingInstances);
	}

	/**
	 * Returns an iterator for the pending requests encapsulated in this map.
	 * 
	 * @return an iterator for the pending requests encapsulated in this map
	 */
	Iterator<Map.Entry<InstanceType, Integer>> iterator() {

		return this.pendingRequests.entrySet().iterator();
	}

	/**
	 * Decreases the number of remaining instances to request of the given type.
	 * 
	 * @param instanceType
	 *        the instance type for which the number of remaining instances shall be decreased
	 */
	void decreaseNumberOfPendingInstances(final InstanceType instanceType) {

		Integer numberOfRemainingInstances = this.pendingRequests.get(instanceType);
		if (numberOfRemainingInstances == null) {
			return;
		}

		numberOfRemainingInstances = Integer.valueOf(numberOfRemainingInstances.intValue() - 1);
		if (numberOfRemainingInstances.intValue() == 0) {
			this.pendingRequests.remove(instanceType);
		} else {
			this.pendingRequests.put(instanceType, numberOfRemainingInstances);
		}
	}
}
