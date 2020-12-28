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

package org.apache.flink.table.functions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.dataview.ListView;
import org.apache.flink.table.api.dataview.MapView;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.types.extraction.TypeInferenceExtractor;
import org.apache.flink.table.types.inference.TypeInference;

/**
 * Base class for a user-defined aggregate function. A user-defined aggregate function maps scalar
 * values of multiple rows to a new scalar value.
 *
 * <p>The behavior of an {@link AggregateFunction} is centered around the concept of an accumulator.
 * The accumulator is an intermediate data structure that stores the aggregated values until a final
 * aggregation result is computed.
 *
 * <p>For each set of rows that needs to be aggregated, the runtime will create an empty accumulator
 * by calling {@link #createAccumulator()}. Subsequently, the {@code accumulate()} method of the
 * function is called for each input row to update the accumulator. Once all rows have been
 * processed, the {@link #getValue(Object)} method of the function is called to compute and return
 * the final result.
 *
 * <p>The main behavior of an {@link AggregateFunction} can be defined by implementing a custom
 * accumulate method. An accumulate method must be declared publicly, not static, and named <code>
 * accumulate</code>. Accumulate methods can also be overloaded by implementing multiple methods
 * named <code>accumulate</code>.
 *
 * <p>By default, input, accumulator, and output data types are automatically extracted using
 * reflection. This includes the generic argument {@code ACC} of the class for determining an
 * accumulator data type and the generic argument {@code T} for determining an accumulator data
 * type. Input arguments are derived from one or more {@code accumulate()} methods. If the
 * reflective information is not sufficient, it can be supported and enriched with {@link
 * DataTypeHint} and {@link FunctionHint} annotations.
 *
 * <p>An {@link AggregateFunction} needs at least three methods:
 *
 * <ul>
 *   <li>{@code createAccumulator}
 *   <li>{@code accumulate}
 *   <li>{@code getValue}
 * </ul>
 *
 * <p>There are a few other methods that are optional:
 *
 * <ul>
 *   <li>{@code retract}
 *   <li>{@code merge}
 * </ul>
 *
 * <p>All these methods must be declared publicly, not static, and named exactly as the names
 * mentioned above to be called by generated code.
 *
 * <p>For storing a user-defined function in a catalog, the class must have a default constructor
 * and must be instantiable during runtime.
 *
 * <pre>{@code
 * Processes the input values and updates the provided accumulator instance. The method
 * accumulate can be overloaded with different custom types and arguments. An aggregate function
 * requires at least one accumulate() method.
 *
 * param: accumulator           the accumulator which contains the current aggregated results
 * param: [user defined inputs] the input value (usually obtained from new arrived data).
 *
 * public void accumulate(ACC accumulator, [user defined inputs])
 * }</pre>
 *
 * <pre>{@code
 * Retracts the input values from the accumulator instance. The current design assumes the
 * inputs are the values that have been previously accumulated. The method retract can be
 * overloaded with different custom types and arguments. This method must be implemented for
 * bounded OVER aggregates over unbounded tables.
 *
 * param: accumulator           the accumulator which contains the current aggregated results
 * param: [user defined inputs] the input value (usually obtained from new arrived data).
 *
 * public void retract(ACC accumulator, [user defined inputs])
 * }</pre>
 *
 * <pre>{@code
 * Merges a group of accumulator instances into one accumulator instance. This method must be
 * implemented for unbounded session window and hop window grouping aggregates and
 * bounded grouping aggregates. Besides, implementing this method will be helpful for optimizations.
 * For example, two phase aggregation optimization requires all the {@link AggregateFunction}s
 * support "merge" method.
 *
 * param: accumulator the accumulator which will keep the merged aggregate results. It should
 *                    be noted that the accumulator may contain the previous aggregated
 *                    results. Therefore user should not replace or clean this instance in the
 *                    custom merge method.
 * param: iterable    an java.lang.Iterable pointed to a group of accumulators that will be
 *                    merged.
 *
 * public void merge(ACC accumulator, java.lang.Iterable<ACC> iterable)
 * }</pre>
 *
 * <p>If this aggregate function can only be applied in an OVER window, this can be declared by
 * returning the requirement {@link FunctionRequirement#OVER_WINDOW_ONLY} in {@link
 * #getRequirements()}.
 *
 * <p>If an accumulator needs to store large amounts of data, {@link ListView} and {@link MapView}
 * provide advanced features for leveraging Flink's state backends in unbounded data scenarios.
 *
 * <p>The following examples show how to specify an aggregate function:
 *
 * <pre>{@code
 * // a function that counts STRING arguments that are not null and emits them as STRING
 * // the accumulator is BIGINT
 * public static class CountFunction extends AggregateFunction<String, CountFunction.MyAccumulator> {
 *   public static class MyAccumulator {
 *     public long count = 0L;
 *   }
 *
 *   {@literal @}Override
 *   public MyAccumulator createAccumulator() {
 *     return new MyAccumulator();
 *   }
 *
 *   public void accumulate(MyAccumulator accumulator, Integer i) {
 *     if (i != null) {
 *       accumulator.count += i;
 *     }
 *   }
 *
 *   {@literal @}Override
 *   public String getValue(MyAccumulator accumulator) {
 *     return "Result: " + accumulator.count;
 *   }
 * }
 *
 * // a function that determines the maximum of either BIGINT or STRING arguments
 * // the accumulator and the output is either BIGINT or STRING
 * public static class MaxFunction extends AggregateFunction<Object, Row> {
 *   {@literal @}Override
 *   public Row createAccumulator() {
 *     return new Row(1);
 *   }
 *
 *   {@literal @}FunctionHint(
 *     accumulator = {@literal @}DataTypeHint("ROW<max BIGINT>"),
 *     output = {@literal @}DataTypeHint("BIGINT")
 *   )
 *   public void accumulate(Row accumulator, Long l) {
 *     final Long max = (Long) accumulator.getField(0);
 *     if (max == null || l > max) {
 *       accumulator.setField(0, l);
 *     }
 *   }
 *
 *   {@literal @}FunctionHint(
 *     accumulator = {@literal @}DataTypeHint("ROW<max STRING>"),
 *     output = {@literal @}DataTypeHint("STRING")
 *   )
 *   public void accumulate(Row accumulator, String s) {
 *     final String max = (String) accumulator.getField(0);
 *     if (max == null || s.compareTo(max) > 0) {
 *       accumulator.setField(0, s);
 *     }
 *   }
 *
 *   {@literal @}Override
 *   public Object getValue(Row accumulator) {
 *     return accumulator.getField(0);
 *   }
 * }
 * }</pre>
 *
 * @param <T> final result type of the aggregation
 * @param <ACC> intermediate result type during the aggregation
 */
@PublicEvolving
public abstract class AggregateFunction<T, ACC> extends ImperativeAggregateFunction<T, ACC> {

    /**
     * Called every time when an aggregation result should be materialized. The returned value could
     * be either an early and incomplete result (periodically emitted as data arrives) or the final
     * result of the aggregation.
     *
     * @param accumulator the accumulator which contains the current intermediate results
     * @return the aggregation result
     */
    public abstract T getValue(ACC accumulator);

    @Override
    public final FunctionKind getKind() {
        return FunctionKind.AGGREGATE;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInferenceExtractor.forAggregateFunction(typeFactory, (Class) getClass());
    }
}
