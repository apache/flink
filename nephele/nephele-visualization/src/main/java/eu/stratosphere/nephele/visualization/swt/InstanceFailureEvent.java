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

package eu.stratosphere.nephele.visualization.swt;

/**
 * This class implements an instance failure event which can be used to deliberately kill a particular instance after
 * the given period of time.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public final class InstanceFailureEvent extends AbstractFailureEvent {

	/**
	 * The name of the instance to be killed.
	 */
	private final String instanceName;

	/**
	 * Constructs a new instance failure event
	 * 
	 * @param interval
	 *        the interval in milliseconds until this event will occur
	 * @param instanceName
	 *        the name of the instance to be killed
	 */
	InstanceFailureEvent(final int interval, final String instanceName) {
		super(interval);

		if (instanceName == null) {
			throw new IllegalArgumentException("Argument instanceName must not be null");
		}

		this.instanceName = instanceName;
	}

	/**
	 * Returns the name of the instance to be killed.
	 * 
	 * @return the name of the instance to be killed
	 */
	public String getInstanceName() {

		return this.instanceName;
	}
}
