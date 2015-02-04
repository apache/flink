/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.compiler.dataproperties;

/**
 * An enumeration tracking the different types of sharding strategies.
 */
public enum PartitioningProperty {

	/**
	 * Any data distribution, i.e., random partitioning or full replication.
	 */
	ANY_DISTRIBUTION,

	/**
	 * Constant indicating no particular partitioning (i.e. random) data distribution.
	 */
	RANDOM_PARTITIONED,

	/**
	 * Constant indicating a hash partitioning.
	 */
	HASH_PARTITIONED,

	/**
	 * Constant indicating a range partitioning.
	 */
	RANGE_PARTITIONED,

	/**
	 * Constant indicating any not further specified disjunct partitioning.
	 */
	ANY_PARTITIONING,
	
	/**
	 * Constant indicating full replication of the data to each parallel instance.
	 */
	FULL_REPLICATION,

	/**
	 * Constant indicating a forced even re-balancing.
	 */
	FORCED_REBALANCED,
	
	/**
	 * A custom partitioning, accompanied by a {@link org.apache.flink.api.common.functions.Partitioner}.
	 */
	CUSTOM_PARTITIONING;
	
	/**
	 * Checks, if this property represents in fact a partitioning. That is,
	 * whether this property is not equal to <tt>PartitionProperty.FULL_REPLICATION</tt>.
	 * 
	 * @return True, if this enum constant is unequal to <tt>PartitionProperty.FULL_REPLICATION</tt>,
	 *         false otherwise.
	 */
	public boolean isPartitioned() {
		return this != FULL_REPLICATION && this != FORCED_REBALANCED;
	}
	
	/**
	 * Checks, if this property represents a full replication.
	 * 
	 * @return True, if this enum constant is equal to <tt>PartitionProperty.FULL_REPLICATION</tt>,
	 *         false otherwise.
	 */
	public boolean isReplication() {
		return this == FULL_REPLICATION;
	}
	
	/**
	 * Checks if this property presents a partitioning that is not random, but on a partitioning key.
	 * 
	 * @return True, if the data is partitioned on a key.
	 */
	public boolean isPartitionedOnKey() {
		return isPartitioned() && this != RANDOM_PARTITIONED;
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
		return this == HASH_PARTITIONED || this == RANGE_PARTITIONED || this == CUSTOM_PARTITIONING;
	}
}
