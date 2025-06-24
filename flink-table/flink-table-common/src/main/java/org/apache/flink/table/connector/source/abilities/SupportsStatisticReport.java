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

package org.apache.flink.table.connector.source.abilities;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.plan.stats.TableStats;

/**
 * Enables to report the estimated statistics provided by the {@link DynamicTableSource}.
 *
 * <p>Statistics are one of the most important inputs to the optimizer cost model, which will
 * generate the most effective execution plan with the lowest cost among all considered candidate
 * plans.
 *
 * <p>This interface is used to compensate for the missing of statistics in the catalog. The planner
 * will call {@link #reportStatistics} method when the planner detects the statistics from catalog
 * is unknown. Therefore, the method will be executed as needed.
 *
 * <p>Note: This method is called at plan optimization phase, the implementation of this interface
 * should be as light as possible, but more complete information.
 */
@PublicEvolving
public interface SupportsStatisticReport {

    /**
     * Returns the estimated statistics of this {@link DynamicTableSource}, else {@link
     * TableStats#UNKNOWN} if some situations are not supported or cannot be handled.
     */
    TableStats reportStatistics();
}
