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
import org.apache.flink.table.expressions.AggregateExpression;
import org.apache.flink.table.types.DataType;

import java.util.List;

/**
 * Enables to push down local aggregates into a {@link ScanTableSource}.
 *
 * <p>Given the following SQL:
 *
 * <pre>{@code
 * SELECT sum(a), max(a), b FROM t GROUP BY b;
 * }</pre>
 *
 * <p>In the example above, {@CODE sum(a), max(a)} and {@CODE group by b} are aggregate functions
 * and grouping sets. The optimized plan will be: TableSourceScan -> LocalHashAggregate -> Exchange
 * -> HashAggregate -> Calc
 *
 * <p>By default, if this interface is not implemented, local aggregates are applied in a subsequent
 * operation after the source.
 *
 * <p>For efficiency, a source can push local aggregates further down in order to reduce the network
 * and computing overhead. The passed aggregate functions and grouping sets are in the original
 * order. The downstream storage which has aggregation capability can directly return the aggregated
 * values to the exchange operator.
 *
 * <p>Note: The local aggregate push down strategy is all or nothing, it can only be pushed down if
 * all aggregate functions are supported.
 *
 * <p>A {@link ScanTableSource} extending this interface is able to aggregate records before
 * returning.
 */
@PublicEvolving
public interface SupportsAggregatePushDown {

    /**
     * Check and pick all aggregate expressions this table source can support. The passed in
     * aggregates and grouping sets have been keep in original order.
     *
     * <p>Note: The final output data type emitted by the source changes from the original produced
     * data type to the emitted data type of local aggregate. The passed {@code DataType
     * producedDataType} is the updated data type for convenience.
     *
     * @param groupingSets a array list of the grouping sets.
     * @param aggregateExpressions a list contains all of aggregates, you should check if all of
     *     aggregate functions can be processed by downstream system. The applying strategy is all
     *     or nothing.
     * @param producedDataType the final output type of the source.
     * @return Whether all of the aggregates push down succeeds
     */
    boolean applyAggregates(
            List<int[]> groupingSets,
            List<AggregateExpression> aggregateExpressions,
            DataType producedDataType);
}
