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

package eu.stratosphere.pact.compiler;

/**
 * An enumeration tracking the different types of partitioning.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public enum PartitionProperty {
	/**
	 * Constant indicating no partitioning.
	 */
	NONE,

	/**
	 * Constant indicating a hash partitioning.
	 */
	HASH_PARTITIONED,

	/**
	 * Constant indicating a range partitioning.
	 */
	RANGE_PARTITIONED,

	/**
	 * Constant indicating any not further specified partitioning.
	 */
	ANY;

	/**
	 * Checks, if this property represents in fact a partitioning. That is,
	 * whether this property is not equal to <tt>PartitionProperty.NONE</tt>.
	 * 
	 * @return True, if this enum constant is unequal to <tt>PartitionProperty.NONE</tt>,
	 *         false otherwise.
	 */
	public boolean isPartitioned() {
		return this != PartitionProperty.NONE;
	}

	/**
	 * Checks, if this property represents a partitioning that is computable.
	 * Computable partitionings can be recreated through an algorithm. If two sets of data are to
	 * be co-partitioned, it is crucial, that the partitioning schemes are computable.
	 * <p>
	 * Examples for computable partitioning schemes are hash- or range-partitionings. An example for a non-computable
	 * partitioning is the implicit partitioning that exists though a globally unique key.
	 * 
	 * @return True, if this enum constant is a re-computable partitioning.
	 */
	public boolean isComputablyPartitioned() {
		return this == HASH_PARTITIONED || this == PartitionProperty.RANGE_PARTITIONED;
	}

	/**
	 * Checks whether this partition property is compatible with another one.
	 * 
	 * @param other
	 *        The other partition property to check against.
	 * @return True, if this partition property is compatible with the other, false if not.
	 */
	public boolean isCompatibleWith(PartitionProperty other) {
		return other == this && this != ANY && this != NONE;
	}
}
