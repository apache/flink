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

package org.apache.flink.table.connector.source.partitioning;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.connector.source.abilities.SupportsPartitioning;

import java.util.List;
import java.util.Optional;

/**
 * An interface to represent the {@link Distribution#HASH} data partitioning for a data source,
 * which is returned by {@link SupportsPartitioning#outputPartitioning()}.
 */
@PublicEvolving
public interface HashPartitioning extends Partitioning {
    default Distribution getDistribution() {
        return Distribution.HASH;
    }

    /**
     * Returns the partitioned columns. If the source provider does not expose this information,
     * then {@link Optional#empty()} is returned. In this case, the the partitioned columns are
     * derived from PARTITIONED BY clause. If the result of {@link HashPartitioning#getColumns()} is
     * non-empty, it overrides the columns given in PARTITIONED BY clause.
     */
    Optional<List<Integer>> getColumns();
}
