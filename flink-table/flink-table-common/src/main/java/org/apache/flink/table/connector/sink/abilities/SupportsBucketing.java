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

package org.apache.flink.table.connector.sink.abilities;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.catalog.TableDistribution;
import org.apache.flink.table.connector.sink.DynamicTableSink;

import java.util.Set;

/**
 * Enables to write bucketed data into a {@link DynamicTableSink}.
 *
 * <p>Buckets enable load balancing in an external storage system by splitting data into disjoint
 * subsets. These subsets group rows with potentially "infinite" keyspace into smaller and more
 * manageable chunks that allow for efficient parallel processing.
 *
 * <p>Bucketing depends heavily on the semantics of the underlying connector. However, a user can
 * influence the bucketing behavior by specifying the number of buckets, the bucketing algorithm,
 * and (if the algorithm allows it) the columns which are used for target bucket calculation.
 *
 * <p>All bucketing components (i.e. bucket number, distribution algorithm, bucket key columns) are
 * optional from a SQL syntax perspective. This ability interface defines which algorithms ({@link
 * #listAlgorithms()}) are effectively supported and whether a bucket count is mandatory ({@link
 * #requiresBucketCount()}). The planner will perform necessary validation checks.
 *
 * <p>Given the following SQL statements:
 *
 * <pre>{@code
 * -- Example 1
 * CREATE TABLE MyTable (uid BIGINT, name STRING) DISTRIBUTED BY HASH(uid) INTO 4 BUCKETS;
 *
 * -- Example 2
 * CREATE TABLE MyTable (uid BIGINT, name STRING) DISTRIBUTED BY (uid) INTO 4 BUCKETS;
 *
 * -- Example 3
 * CREATE TABLE MyTable (uid BIGINT, name STRING) DISTRIBUTED BY (uid);
 *
 * -- Example 4
 * CREATE TABLE MyTable (uid BIGINT, name STRING) DISTRIBUTED INTO 4 BUCKETS;
 * }</pre>
 *
 * <p>Example 1 declares a hash function on a fixed number of 4 buckets (i.e. HASH(uid) % 4 = target
 * bucket). Example 2 leaves the selection of an algorithm up to the connector, represented as
 * {@link TableDistribution.Kind#UNKNOWN}. Additionally, Example 3 leaves the number of buckets up
 * to the connector. In contrast, Example 4 only defines the number of buckets.
 *
 * <p>A sink can implement both {@link SupportsPartitioning} and {@link SupportsBucketing}.
 * Conceptually, a partition can be seen as kind of "directory" whereas buckets correspond to
 * "files" per directory. Partitioning splits the data on a small, human-readable keyspace (e.g. by
 * year or by geographical region). This enables efficient selection via equality, inequality, or
 * ranges due to knowledge about existing partitions. Bucketing operates within partitions on a
 * potentially large and infinite keyspace.
 *
 * @see SupportsPartitioning
 */
@PublicEvolving
public interface SupportsBucketing {

    /**
     * Returns the set of supported bucketing algorithms.
     *
     * <p>The set must be non-empty. Otherwise, the planner will throw an error during validation.
     *
     * <p>If specifying an algorithm is optional, this set must include {@link
     * TableDistribution.Kind#UNKNOWN}.
     */
    Set<TableDistribution.Kind> listAlgorithms();

    /**
     * Returns whether the {@link DynamicTableSink} requires a bucket count.
     *
     * <p>If this method returns {@code true}, the {@link DynamicTableSink} will require a bucket
     * count.
     *
     * <p>If this method return {@code false}, the {@link DynamicTableSink} may or may not consider
     * the provided bucket count.
     */
    boolean requiresBucketCount();
}
