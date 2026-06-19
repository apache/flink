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
import org.apache.flink.table.connector.source.ScanTableSource;

import java.util.List;

/**
 * Pushes dynamic filter into {@link ScanTableSource}, the table source can filter the partitions
 * even the input data in runtime to reduce scan I/O.
 *
 * <p>Given the following SQL:
 *
 * <pre>{@code
 * SELECT * FROM partitioned_fact_table t1, non_partitioned_dim_table t2
 *   WHERE t1.part_key = t2.col1 AND t2.col2 = 100
 * }</pre>
 *
 * <p>In the example above, `partitioned_fact_table` is partition table whose partition key is
 * `part_key`, and `non_partitioned_dim_table` is a non-partition table which data contains all
 * partition values of `partitioned_fact_table`. With the filter {@code t2.col2 = 100}, only a small
 * part of the partitions need to be scanned out to do the join operation. The specific partitions
 * is not available in the optimization phase but in the execution phase.
 *
 * <p>Unlike {@link SupportsPartitionPushDown}, the conditions in the WHERE clause are analyzed to
 * determine in advance which partitions can be safely skipped in the optimization phase. For such
 * queries, the specific partitions is not available in the optimization phase but in the execution
 * phase.
 *
 * <p>By default, if this interface is not implemented, the data is read entirely with a subsequent
 * filter operation after the source.
 *
 * <p>If this interface is implemented, this interface just tells the source which fields can be
 * applied for filtering and the source needs to pick the fields that can be supported and return
 * them to planner. Then the planner will build the plan and construct the operator which will send
 * the data to the source in runtime.
 *
 * <p>In the future, more flexible filtering can be pushed into the source connectors through this
 * interface.
 */
@PublicEvolving
public interface SupportsDynamicFiltering {

    /**
     * Return the filter fields this partition table source supported. This method is can tell the
     * planner which fields can be used as dynamic filtering fields, the planner will pick some
     * fields from the returned fields based on the query, and create dynamic filtering operator.
     */
    List<String> listAcceptedFilterFields();

    /**
     * Applies the candidate filter fields into the table source. The data corresponding the filter
     * fields will be provided in runtime, which can be used to filter the partitions or the input
     * data.
     *
     * <p>NOTE: the candidate filter fields are always from the result of {@link
     * #listAcceptedFilterFields()}.
     */
    void applyDynamicFiltering(List<String> candidateFilterFields);
}
