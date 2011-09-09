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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.types.StringRecord;

/**
 * An instance type describes the hardware resources a task manager runs on. According
 * to its type an instance has a specific number of CPU cores, computation units, a certain
 * amount of main memory and disk space. In addition, it has a specific price per hour.
 * 
 * @author warneke
 */
public final class InstanceType implements IOReadableWritable {

	/**
	 * The identifier for this instance type.
	 */
	private String identifier;

	/**
	 * The number of computational units of this instance type.
	 * A computational unit is a virtual compute capacity. A host with a
	 * single-core 2 GHz CPU may possess 20 compute units (1*20), while a
	 * dual-core 2.5 GHz CPU may possess 50 compute units (2*25). The
	 * specified number of compute units expresses the fraction of the
	 * CPU capacity promised to a user.
	 */
	private int numberOfComputeUnits = 0;

	/**
	 * The number of CPU cores of this instance type.
	 */
	private int numberOfCores = 0;

	/**
	 * The amount of main memory of this instance type (in MB).
	 */
	private int memorySize = 0;

	/**
	 * The disk capacity of this instance type (in GB).
	 */
	private int diskCapacity = 0;

	/**
	 * The price per hour that is charged for running instances of this type.
	 */
	private int pricePerHour = 0;

	/**
	 * Public constructor required for the serialization process.
	 */
	public InstanceType() {
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
		return this.identifier;
	}

	/**
	 * Returns a String representation of this instance in the same form as it is parsed by the
	 * {@link #getTypeFromString(java.lang.String)} method.
	 * 
	 * @return A String representation of this instance type.
	 */
	public String toStringRepresentation() {
		
		final StringBuilder bld = new StringBuilder(32);
		bld.append(this.identifier);
		bld.append(',');
		bld.append(this.numberOfComputeUnits);
		bld.append(',');
		bld.append(this.numberOfCores);
		bld.append(',');
		bld.append(this.memorySize);
		bld.append(',');
		bld.append(this.diskCapacity);
		bld.append(',');
		bld.append(this.pricePerHour);

		return bld.toString();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {

		StringRecord.writeString(out, this.identifier);
		out.writeInt(this.numberOfComputeUnits);
		out.writeInt(this.numberOfCores);
		out.writeInt(this.memorySize);
		out.writeInt(this.diskCapacity);
		out.writeInt(this.pricePerHour);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final DataInput in) throws IOException {

		this.identifier = StringRecord.readString(in);
		this.numberOfComputeUnits = in.readInt();
		this.numberOfCores = in.readInt();
		this.memorySize = in.readInt();
		this.diskCapacity = in.readInt();
		this.pricePerHour = in.readInt();
	}
}
