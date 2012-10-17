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

package eu.stratosphere.nephele.instance;

/**
 * An instance type description provides details of instance type. Is can comprise both the hardware description from
 * the instance type description (as provided by the operator/administrator of the instance) as well as the actual
 * hardware description which has been determined on the compute instance itself.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public final class InstanceTypeDescription {

	/**
	 * The instance type.
	 */
	private final InstanceType instanceType;

	/**
	 * The hardware description as created by the {@link InstanceManager}.
	 */
	private final HardwareDescription hardwareDescription;

	/**
	 * The maximum number of available instances of this type.
	 */
	private final int maximumNumberOfAvailableInstances;

	/**
	 * The default constructor required by kryo.
	 */
	@SuppressWarnings("unused")
	private InstanceTypeDescription() {
		this.instanceType = null;
		this.hardwareDescription = null;
		this.maximumNumberOfAvailableInstances = 0;
	}

	/**
	 * Constructs a new instance type description.
	 * 
	 * @param instanceType
	 *        the instance type
	 * @param hardwareDescription
	 *        the hardware description as created by the {@link InstanceManager}
	 * @param maximumNumberOfAvailableInstances
	 *        the maximum number of available instances of this type
	 */
	InstanceTypeDescription(final InstanceType instanceType, final HardwareDescription hardwareDescription,
			final int maximumNumberOfAvailableInstances) {

		this.instanceType = instanceType;
		this.hardwareDescription = hardwareDescription;
		this.maximumNumberOfAvailableInstances = maximumNumberOfAvailableInstances;
	}

	/**
	 * Returns the hardware description as created by the {@link InstanceManager}.
	 * 
	 * @return the instance's hardware description or <code>null</code> if no description is available
	 */
	public HardwareDescription getHardwareDescription() {
		return this.hardwareDescription;
	}

	/**
	 * Returns the instance type as determined by the {@link InstanceManager}.
	 * 
	 * @return the instance type
	 */
	public InstanceType getInstanceType() {
		return this.instanceType;
	}

	/**
	 * Returns the maximum number of instances the {@link InstanceManager} can at most allocate of this instance type
	 * (i.e. when no other jobs are occupying any resources).
	 * 
	 * @return the maximum number of instances of this type or <code>-1</code> if the number is unknown to the
	 *         {@link InstanceManager}
	 */
	public int getMaximumNumberOfAvailableInstances() {
		return this.maximumNumberOfAvailableInstances;
	}
}
