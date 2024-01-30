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
 * <p>Buckets split the data stored in an external system into disjoint subsets. Each row will
 * belong to one bucket based on physical columns in the row.
 *
 * <p>As an example, a table could be bucketed in using a hash function on the first column of the
 * table into 4 buckets.
 *
 * <p>For the above example, the SQL syntax would be:
 *
 * <pre>{@code
 * CREATE TABLE MyTable (uid BIGINT, name STRING) DISTRIBUTED BY HASH(uid) INTO 4 BUCKETS
 * }</pre>
 */
@PublicEvolving
public interface SupportsBucketing {
    /**
     * Returns the set of supported bucketing algorithms.
     *
     * <p>The set must be non-empty. Otherwise, the planner will throw an error during validation.
     *
     * @return the set of supported bucketing algorithms.
     */
    Set<TableDistribution.Kind> listAlgorithms();

    /**
     * Returns whether the {@link DynamicTableSink} requires a bucket count.
     *
     * <p>If this method returns {@code true}, the {@link DynamicTableSink} will require a bucket
     * count.
     *
     * <p>If this method return {@code false}, the {@link DynamicTableSink} may or may not observe a
     * provided bucket count.
     *
     * @return whether the {@link DynamicTableSink} requires a bucket count.
     */
    boolean requiresBucketCount();
}
