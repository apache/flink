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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.expressions.AggregateExpression;
import org.apache.flink.table.types.DataType;

import java.util.List;

/**
 * Enables to push down local aggregates into a {@link ScanTableSource}.
 *
 * <p>Given the following example inventory table:
 *
 * <pre>{@code
 * CREATE TABLE inventory (
 *   id INT,
 *   name STRING,
 *   amount INT,
 *   price DOUBLE,
 *   type STRING,
 * )
 * }</pre>
 *
 * <p>And we have a simple aggregate sql:
 *
 * <pre>{@code
 * SELECT
 *   sum(amount),
 *   max(price),
 *   avg(price),
 *   count(1),
 *   name,
 *   type
 * FROM inventory
 *   group by name, type
 * }</pre>
 *
 * <p>In the example above, {@code sum(amount), max(price), avg(price), count(1)} and {@code group
 * by name, type} are aggregate functions and grouping sets. By default, if this interface is not
 * implemented, local aggregates are applied in a subsequent operation after the source. The
 * optimized plan will be the following without local aggregate push down:
 *
 * <pre>{@code
 * Calc(select=[EXPR$0, EXPR$1, EXPR$2, EXPR$3, name, type])
 * +- HashAggregate(groupBy=[name, type], select=[name, type, Final_SUM(sum$0) AS EXPR$0, Final_MAX(max$1) AS EXPR$1, Final_AVG(sum$2, count$3) AS EXPR$2, Final_COUNT(count1$4) AS EXPR$3])
 *    +- Exchange(distribution=[hash[name, type]])
 *       +- LocalHashAggregate(groupBy=[name, type], select=[name, type, Partial_SUM(amount) AS sum$0, Partial_MAX(price) AS max$1, Partial_AVG(price) AS (sum$2, count$3), Partial_COUNT(*) AS count1$4])
 *          +- TableSourceScan(table=[[inventory, project=[name, type, amount, price]]], fields=[name, type, amount, price])
 * }</pre>
 *
 * <p>For efficiency, a source can push local aggregates further down in order to reduce the network
 * and computing overhead. The passed aggregate functions and grouping sets are in the original
 * order. The downstream storage which has aggregation capability can directly return the aggregated
 * values if the underlying database or storage system has aggregation capability. The optimized
 * plan will change to the following pattern with local aggregate push down:
 *
 * <pre>{@code
 * Calc(select=[EXPR$0, EXPR$1, EXPR$2, EXPR$3, name, type])
 * +- HashAggregate(groupBy=[name, type], select=[name, type, Final_SUM(sum$0) AS EXPR$0, Final_MAX(max$1) AS EXPR$1, Final_AVG(sum$2, count$3) AS EXPR$2, Final_COUNT(count1$4) AS EXPR$3])
 *    +- Exchange(distribution=[hash[name, type]])
 *       +- TableSourceScan(table=[[inventory, project=[name, type, amount, price], aggregates=[grouping=[name,type], aggFunctions=[IntSumAggFunction(amount),DoubleMaxAggFunction(price),DoubleSum0AggFunction(price),CountAggFunction(price),Count1AggFunction()]]]], fields=[name, type, sum$0, max$1, sum$2, count$3, count1$4])
 * }</pre>
 *
 * <p>We can see the original {@code LocalHashAggregate} has been removed and pushed down into
 * {@code TableSourceScan}. Meanwhile the output datatype of {@code TableSourceScan} has changed,
 * which is the pattern of {@code grouping sets} + {@code the output of aggregate functions}.
 *
 * <p>Due to the complexity of aggregate, only limited aggregate functions are supported at present.
 *
 * <ul>
 *   <li>Only support sum/min/max/count/avg(will convert to sum0 + count) aggregate function push
 *       down.
 *   <li>Only support simple group type, cube and roll up will not be pushed down.
 *   <li>If expression is involved in aggregate or group by, e.g. max (col1 + col2) or group by
 *       (col1 + col2), aggregate will not be pushed down.
 *   <li>Window aggregate function will not be pushed down.
 *   <li>Aggregate function with filter will not be pushed down.
 * </ul>
 *
 * <p>For the above example inventory table, and we have the below test sql:
 *
 * <pre>{@code
 * SELECT
 *   sum(amount),
 *   max(price),
 *   avg(price),
 *   count(distinct price),
 *   name,
 *   type
 * FROM inventory
 *   group by name, type
 * }</pre>
 *
 * <p>Since there is a count(distinct) aggregate function in it, the entire computational semantics
 * will change obviously. And the optimized plan as shown below. The local aggregate will not be
 * pushed down in this scenario.
 *
 * <pre>{@code
 * Calc(select=[EXPR$0, EXPR$1, EXPR$2, EXPR$3, name, type])
 * +- HashAggregate(groupBy=[name, type], select=[name, type, Final_MIN(min$0) AS EXPR$0, Final_MIN(min$1) AS EXPR$1, Final_MIN(min$2) AS EXPR$2, Final_COUNT(count$3) AS EXPR$3])
 *    +- Exchange(distribution=[hash[name, type]])
 *       +- LocalHashAggregate(groupBy=[name, type], select=[name, type, Partial_MIN(EXPR$0) FILTER $g_1 AS min$0, Partial_MIN(EXPR$1) FILTER $g_1 AS min$1, Partial_MIN(EXPR$2) FILTER $g_1 AS min$2, Partial_COUNT(price) FILTER $g_0 AS count$3])
 *          +- Calc(select=[name, type, price, EXPR$0, EXPR$1, EXPR$2, =(CASE(=($e, 0:BIGINT), 0:BIGINT, 1:BIGINT), 0) AS $g_0, =(CASE(=($e, 0:BIGINT), 0:BIGINT, 1:BIGINT), 1) AS $g_1])
 *             +- HashAggregate(groupBy=[name, type, price, $e], select=[name, type, price, $e, Final_SUM(sum$0) AS EXPR$0, Final_MAX(max$1) AS EXPR$1, Final_AVG(sum$2, count$3) AS EXPR$2])
 *                +- Exchange(distribution=[hash[name, type, price, $e]])
 *                   +- LocalHashAggregate(groupBy=[name, type, price, $e], select=[name, type, price, $e, Partial_SUM(amount) AS sum$0, Partial_MAX(price_0) AS max$1, Partial_AVG(price_0) AS (sum$2, count$3)])
 *                      +- Expand(projects=[name, type, amount, price, $e, price_0], projects=[{name, type, amount, price, 0 AS $e, price AS price_0}, {name, type, amount, null AS price, 1 AS $e, price AS price_0}])
 *                         +- TableSourceScan(table=[[inventory, project=[name, type, amount, price]]], fields=[name, type, amount, price])
 * }</pre>
 *
 * <p>Note: The local aggregate push down strategy is all or nothing, it can only be pushed down if
 * all aggregate functions are supported.
 *
 * <p>Regardless if this interface is implemented or not, a final aggregation is always applied in a
 * subsequent operation after the source.
 *
 * <p>Note: currently, the {@link SupportsAggregatePushDown} is not supported by planner.
 */
@PublicEvolving
public interface SupportsAggregatePushDown {

    /**
     * Provides a list of aggregate expressions and the grouping keys. The source should pick all
     * the aggregates or nothing and return whether all the aggregates have been pushed down into
     * the source.
     *
     * <p>Note: Use the passed data type instead of {@link TableSchema#toPhysicalRowDataType()} for
     * describing the final output data type when creating {@link TypeInformation}. The projection
     * of grouping keys and aggregate values is already considered in the given output data type.
     * The passed data type pattern is {@code grouping sets} + {@code aggregate function result},
     * downstream storage need to organize the returned aggregate data strictly in this manner.
     *
     * @param groupingSets a array list of the grouping sets.
     * @param aggregateExpressions a list contains all of aggregates, you should check if all of
     *     aggregate functions can be processed by downstream system. The applying strategy is all
     *     or nothing.
     * @param producedDataType the final output type of the source.
     * @return true if all the aggregates have been pushed down into source, false otherwise.
     */
    boolean applyAggregates(
            List<int[]> groupingSets,
            List<AggregateExpression> aggregateExpressions,
            DataType producedDataType);
}
