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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * An instance request map specifies the required types of instances to run a specific job and the respective number
 * thereof. For each instance type it is possible to specify the minimum number of instances required to run the job. If
 * the {@link InstanceManager} cannot manage to provide at least this minimum numbers of instances for the given type,
 * the job will be rejected.
 * <p>
 * In addition, is it also possible to specify the optimal number of instances for a particular instance type. The
 * {@link InstanceManager} will try to provide this optimal number of instances, but will also start the job with less
 * instances.
 * <p>
 * This class is not thread-safe.
 * 
 */
public final class InstanceRequestMap {

	/**
	 * The map holding the minimum number of instances to be requested for each instance type.
	 */
	private final Map<InstanceType, Integer> minimumMap = new HashMap<InstanceType, Integer>();

	/**
	 * The map holding the maximum number of instances to be requested for each instance type.
	 */
	private final Map<InstanceType, Integer> maximumMap = new HashMap<InstanceType, Integer>();

	/**
	 * Sets the minimum number of instances to be requested from the given instance type.
	 * 
	 * @param instanceType
	 *        the type of instance to request
	 * @param number
	 *        the minimum number of instances to request
	 */
	public void setMinimumNumberOfInstances(final InstanceType instanceType, final int number) {

		this.minimumMap.put(instanceType, Integer.valueOf(number));
	}

	/**
	 * Sets the maximum number of instances to be requested from the given instance type.
	 * 
	 * @param instanceType
	 *        the type of instance to request
	 * @param number
	 *        the maximum number of instances to request
	 */
	public void setMaximumNumberOfInstances(final InstanceType instanceType, final int number) {

		this.maximumMap.put(instanceType, Integer.valueOf(number));
	}

	/**
	 * Sets both the minimum and the maximum number of instances to be requested from the given instance type.
	 * 
	 * @param instanceType
	 *        the type of instance to request
	 * @param number
	 *        the minimum and the maximum number of instances to request
	 */
	public void setNumberOfInstances(final InstanceType instanceType, final int number) {

		setMinimumNumberOfInstances(instanceType, number);
		setMaximumNumberOfInstances(instanceType, number);
	}

	/**
	 * Returns the minimum number of instances to be requested from the given instance type.
	 * 
	 * @param instanceType
	 *        the type of instance to request
	 * @return the minimum number of instances to be requested from the given instance type
	 */
	public int getMinimumNumberOfInstances(final InstanceType instanceType) {

		final Integer val = this.minimumMap.get(instanceType);
		if (val != null) {
			return val.intValue();
		}

		return 0;
	}

	/**
	 * Returns the maximum number of instances to be requested from the given instance type.
	 * 
	 * @param instanceType
	 *        the type of instance to request
	 * @return the maximum number of instances to be requested from the given instance type
	 */
	public int getMaximumNumberOfInstances(final InstanceType instanceType) {

		final Integer val = this.maximumMap.get(instanceType);
		if (val != null) {
			return val.intValue();
		}

		return 0;
	}

	/**
	 * Checks if this instance request map is empty, i.e. neither contains an entry for the minimum or maximum number of
	 * instances to be requested for any instance type.
	 * 
	 * @return <code>true</code> if the map is empty, <code>false</code> otherwise
	 */
	public boolean isEmpty() {

		if (!this.maximumMap.isEmpty()) {
			return false;
		}

		if (!this.minimumMap.isEmpty()) {
			return false;
		}

		return true;
	}

	/**
	 * Returns an {@link Iterator} object which allows to traverse the minimum number of instances to be requested for
	 * each instance type.
	 * 
	 * @return an iterator to traverse the minimum number of instances to be requested for each instance type
	 */
	public Iterator<Map.Entry<InstanceType, Integer>> getMaximumIterator() {

		return this.maximumMap.entrySet().iterator();
	}

	/**
	 * Returns an {@link Iterator} object which allows to traverse the maximum number of instances to be requested for
	 * each instance type.
	 * 
	 * @return an iterator to traverse the maximum number of instances to be requested for each instance type
	 */
	public Iterator<Map.Entry<InstanceType, Integer>> getMinimumIterator() {

		return this.minimumMap.entrySet().iterator();
	}

	/**
	 * Returns the number of different instance types stored in this request map.
	 * 
	 * @return the number of different instance types stored in this request map
	 */
	public int size() {

		final int s = this.maximumMap.size();

		if (s != this.minimumMap.size()) {
			throw new IllegalStateException("InstanceRequestMap is in an inconsistent state");
		}

		return s;
	}

	/**
	 * Clears the instance request map.
	 */
	public void clear() {

		this.maximumMap.clear();
		this.minimumMap.clear();
	}
}
