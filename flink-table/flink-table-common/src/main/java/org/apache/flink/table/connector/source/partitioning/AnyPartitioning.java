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

import java.util.Optional;

/**
 * An interface to represent the {@link Distribution#ANY} data partitioning for a data source, which
 * is returned by {@link SupportsPartitioning#outputPartitioning()}.
 */
@PublicEvolving
public class AnyPartitioning implements Partitioning {
    @Override
    public Optional<Integer> numPartitions() {
        return Optional.empty();
    }

    @Override
    public Distribution getDistribution() {
        return Distribution.ANY;
    }
}
