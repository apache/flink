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

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * An instance type describes the hardware resources a task manager runs on. According
 * to its type an instance has a specific number of CPU cores, computation units, a certain
 * amount of main memory and disk space. In addition, it has a specific price per hour.
 * 
 * @author warneke
 */
public class InstanceType {
	/**
	 * The pattern used to parse descriptions of instance types.
	 */
	private static Pattern descr_pattern = null;

	// ------------------------------------------------------------------------

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
	public InstanceType(String identifier, int numberOfComputeUnits, int numberOfCores, int memorySize,
			int diskCapacity, int pricePerHour) {

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
		return this.identifier;
	}

	/**
	 * Returns a String representation of this instance in the same form as it is parsed by the
	 * {@link #getTypeFromString(java.lang.String)} method.
	 * 
	 * @return A String representation of this instance type.
	 */
	public String toStringRepresentation() {
		StringBuilder bld = new StringBuilder(32);
		bld.append(identifier);
		bld.append(',');
		bld.append(numberOfComputeUnits);
		bld.append(',');
		bld.append(numberOfCores);
		bld.append(',');
		bld.append(memorySize);
		bld.append(',');
		bld.append(diskCapacity);
		bld.append(',');
		bld.append(pricePerHour);

		return bld.toString();
	}

	// ------------------------------------------------------------------------

	/**
	 * Gets an instance type parsed from its string description.
	 * 
	 * @param description
	 *        The string description of the instance type.
	 * @return An instance that corresponds to the description.
	 * @throws IllegalArgumentException
	 *         Thrown, if the string does not correctly describe an instance.
	 */
	public static final InstanceType getTypeFromString(String description) throws IllegalArgumentException {
		if (descr_pattern == null) {
			try {
				descr_pattern = Pattern.compile("^([^,]+),(\\d+),(\\d+),(\\d+),(\\d+),(\\d+)$");
			} catch (PatternSyntaxException psex) {
				throw new RuntimeException("Invalid Regex Pattern to parse instance description.", psex);
			}
		}

		try {
			final Matcher m = descr_pattern.matcher(description);

			if (!m.matches()) {
				throw new IllegalArgumentException("The value '" + description + "' does not match pattern "
					+ descr_pattern.toString());
			}

			final String identifier = m.group(1);
			final int numComputeUnits = Integer.parseInt(m.group(2));
			final int numCores = Integer.parseInt(m.group(3));
			final int memorySize = Integer.parseInt(m.group(4));
			final int diskCapacity = Integer.parseInt(m.group(5));
			final int pricePerHour = Integer.parseInt(m.group(6));

			return new InstanceType(identifier, numComputeUnits, numCores, memorySize, diskCapacity, pricePerHour);
		} catch (Exception e) {
			throw new IllegalArgumentException("The value '" + description + "' does not match pattern "
				+ descr_pattern.toString());
		}
	}
}
