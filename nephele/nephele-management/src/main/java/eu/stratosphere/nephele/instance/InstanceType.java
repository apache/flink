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
 * An instance type describes the hardware resources a task manager runs on. According
 * to its type an instance has a specific number of CPU cores, computation units, a certain
 * amount of main memory and disk space. In addition, it has a specific price per hour.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public final class InstanceType {

	/**
	 * The identifier for this instance type.
	 */
	private final String identifier;

	/**
	 * The number of computational units of this instance type.
	 * A computational unit is a virtual compute capacity. A host with a
	 * single-core 2 GHz CPU may possess 20 compute units (1*20), while a
	 * dual-core 2.5 GHz CPU may possess 50 compute units (2*25). The
	 * specified number of compute units expresses the fraction of the
	 * CPU capacity promised to a user.
	 */
	private final int numberOfComputeUnits;

	/**
	 * The number of CPU cores of this instance type.
	 */
	private final int numberOfCores;

	/**
	 * The amount of main memory of this instance type (in MB).
	 */
	private final int memorySize;

	/**
	 * The disk capacity of this instance type (in GB).
	 */
	private final int diskCapacity;

	/**
	 * The price per hour that is charged for running instances of this type.
	 */
	private final int pricePerHour;

	/**
	 * Default constructor required by kryo.
	 */
	@SuppressWarnings("unused")
	private InstanceType() {
		this.identifier = null;
		this.numberOfComputeUnits = 0;
		this.numberOfCores = 0;
		this.memorySize = 0;
		this.diskCapacity = 0;
		this.pricePerHour = 0;
	}

	/**
	 * Creates a new instance type.
	 * 
	 * @param identifier
	 *        identifier for this instance type
	 * @param numberOfComputeUnits
	 *        number of computational units of this instance type
	 * @param numberOfCores
	 *        number of CPU cores of this instance type
	 * @param memorySize
	 *        amount of main memory of this instance type (in MB)
	 * @param diskCapacity
	 *        disk capacity of this instance type (in GB)
	 * @param pricePerHour
	 *        price per hour that is charged for running instances of this type
	 */
	InstanceType(final String identifier, final int numberOfComputeUnits, final int numberOfCores,
			final int memorySize,
			final int diskCapacity, final int pricePerHour) {

		this.identifier = identifier;
		this.numberOfComputeUnits = numberOfComputeUnits;
		this.numberOfCores = numberOfCores;
		this.memorySize = memorySize;
		this.diskCapacity = diskCapacity;
		this.pricePerHour = pricePerHour;
	}

	/**
	 * Returns the instance type's number of computational units.
	 * 
	 * @return the instance type's number of computational units
	 */
	public int getNumberOfComputeUnits() {
		return this.numberOfComputeUnits;
	}

	/**
	 * Returns the instance type's number of CPU cores.
	 * 
	 * @return the instance type's number of CPU cores
	 */
	public int getNumberOfCores() {
		return this.numberOfCores;
	}

	/**
	 * Returns the instance type's amount of main memory.
	 * 
	 * @return the instance type's amount of main memory
	 */
	public int getMemorySize() {
		return this.memorySize;
	}

	/**
	 * Returns the instance type's disk capacity.
	 * 
	 * @return the instance type's disk capacity
	 */
	public int getDiskCapacity() {
		return this.diskCapacity;
	}

	/**
	 * Returns the instance type's price per hour.
	 * 
	 * @return the instance type's price per hour
	 */
	public int getPricePerHour() {
		return this.pricePerHour;
	}

	/**
	 * Returns the instance type's identifier.
	 * 
	 * @return the instance type's identifier
	 */
	public String getIdentifier() {
		return this.identifier;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {

		final StringBuilder bld = new StringBuilder(32);
		bld.append(this.identifier);
		bld.append(' ');
		bld.append('(');
		bld.append(this.numberOfComputeUnits);
		bld.append(',');
		bld.append(this.numberOfCores);
		bld.append(',');
		bld.append(this.memorySize);
		bld.append(',');
		bld.append(this.diskCapacity);
		bld.append(',');
		bld.append(this.pricePerHour);
		bld.append(')');

		return bld.toString();
	}
}
