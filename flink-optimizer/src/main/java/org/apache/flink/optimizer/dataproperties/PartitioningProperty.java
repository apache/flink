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

package org.apache.flink.optimizer.dataproperties;

/**
 * An enumeration of the different types of distributing data across partitions or parallel workers.
 */
public enum PartitioningProperty {

    /**
     * Any possible way of data distribution, including random partitioning and full replication.
     */
    ANY_DISTRIBUTION,

    /**
     * A random disjunct (non-replicated) data distribution, where each datum is contained in one
     * partition only. This is for example the result of parallel scans of data in a file system
     * like HDFS, or the result of a round-robin data distribution.
     */
    RANDOM_PARTITIONED,

    /** A hash partitioning on a certain key. */
    HASH_PARTITIONED,

    /** A range partitioning on a certain key. */
    RANGE_PARTITIONED,

    /**
     * A not further specified partitioning on a key (hash-, or range partitioning, or some other
     * scheme even).
     */
    ANY_PARTITIONING,

    /** Full replication of the data to each parallel instance. */
    FULL_REPLICATION,

    /**
     * A forced even re-balancing. All partitions are guaranteed to have almost the same number of
     * records.
     */
    FORCED_REBALANCED,

    /**
     * A custom partitioning, accompanied by a {@link
     * org.apache.flink.api.common.functions.Partitioner}.
     */
    CUSTOM_PARTITIONING;

    /**
     * Checks, if this property represents in fact a partitioning. That is, whether this property is
     * not equal to <tt>PartitionProperty.FULL_REPLICATION</tt>.
     *
     * @return True, if this enum constant is unequal to
     *     <tt>PartitionProperty.FULL_REPLICATION</tt>, false otherwise.
     */
    public boolean isPartitioned() {
        return this != FULL_REPLICATION && this != FORCED_REBALANCED && this != ANY_DISTRIBUTION;
    }

    /**
     * Checks, if this property represents a full replication.
     *
     * @return True, if this enum constant is equal to <tt>PartitionProperty.FULL_REPLICATION</tt>,
     *     false otherwise.
     */
    public boolean isReplication() {
        return this == FULL_REPLICATION;
    }

    /**
     * Checks if this property presents a partitioning that is not random, but on a partitioning
     * key.
     *
     * @return True, if the data is partitioned on a key.
     */
    public boolean isPartitionedOnKey() {
        return isPartitioned() && this != RANDOM_PARTITIONED;
    }

    /**
     * Checks, if this property represents a partitioning that is computable. A computable
     * partitioning can be recreated through an algorithm. If two sets of data are to be
     * co-partitioned, it is crucial, that the partitioning schemes are computable.
     *
     * <p>Examples for computable partitioning schemes are hash- or range-partitioning. An example
     * for a non-computable partitioning is the implicit partitioning that exists though a globally
     * unique key.
     *
     * @return True, if this enum constant is a re-computable partitioning.
     */
    public boolean isComputablyPartitioned() {
        return this == HASH_PARTITIONED || this == RANGE_PARTITIONED || this == CUSTOM_PARTITIONING;
    }
}
