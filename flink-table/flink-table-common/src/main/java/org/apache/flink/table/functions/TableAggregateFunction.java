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
import org.apache.flink.util.Collector;

/**
 * Base class for a user-defined table aggregate function. A user-defined table aggregate function
 * maps scalar values of multiple rows to zero, one, or multiple rows (or structured types). If an
 * output record consists of only one field, the structured record can be omitted, and a scalar
 * value can be emitted that will be implicitly wrapped into a row by the runtime.
 *
 * <p>Similar to an {@link AggregateFunction}, the behavior of a {@link TableAggregateFunction} is
 * centered around the concept of an accumulator. The accumulator is an intermediate data structure
 * that stores the aggregated values until a final aggregation result is computed.
 *
 * <p>For each set of rows that needs to be aggregated, the runtime will create an empty accumulator
 * by calling {@link #createAccumulator()}. Subsequently, the {@code accumulate()} method of the
 * function is called for each input row to update the accumulator. Once all rows have been
 * processed, the {@code emitValue()} or {@code emitUpdateWithRetract()} method of the function is
 * called to compute and return the final result.
 *
 * <p>The main behavior of an {@link TableAggregateFunction} can be defined by implementing a custom
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
 * <p>A {@link TableAggregateFunction} needs at least three methods:
 *
 * <ul>
 *   <li>{@code createAccumulator}
 *   <li>{@code accumulate}
 *   <li>{@code emitValue} or {@code emitUpdateWithRetract}
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
 * and must be instantiable during runtime. Anonymous functions in Table API can only be persisted
 * if the function is not stateful (i.e. containing only transient and static fields).
 *
 * <pre>{@code
 * Processes the input values and updates the provided accumulator instance. The method
 * accumulate can be overloaded with different custom types and arguments. A table aggregate function
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
 * param: [user defined inputs] the input value (usually obtained from a new arrived data).
 *
 * public void retract(ACC accumulator, [user defined inputs])
 * }</pre>
 *
 * <pre>{@code
 * Merges a group of accumulator instances into one accumulator instance. This method must be
 * implemented for unbounded session and hop window grouping aggregates and
 * bounded grouping aggregates.
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
 * <pre>{@code
 * Called every time when an aggregation result should be materialized. The returned value could
 * be either an early and incomplete result (periodically emitted as data arrives) or the final
 * result of the aggregation.
 *
 * param: accumulator           the accumulator which contains the current aggregated results
 * param: out                   the collector used to output data.
 *
 * public void emitValue(ACC accumulator, org.apache.flink.util.Collector<T> out)
 * }</pre>
 *
 * <pre>{@code
 * Called every time when an aggregation result should be materialized. The returned value could
 * be either an early and incomplete result (periodically emitted as data arrives) or the final
 * result of the aggregation.
 *
 * Compared to emitValue(), emitUpdateWithRetract() is used to emit values that have been updated. This method
 * outputs data incrementally in retraction mode (also known as "update before" and "update after"). Once
 * there is an update, we have to retract old records before sending new updated ones. The emitUpdateWithRetract()
 * method will be used in preference to the emitValue() method if both methods are defined in the table aggregate
 * function, because the method is treated to be more efficient than emitValue as it can output
 * values incrementally.
 *
 * param: accumulator           the accumulator which contains the current aggregated results
 * param: out                   the retractable collector used to output data. Use the collect() method
 *                              to output(add) records and use retract method to retract(delete)
 *                              records.
 *
 * public void emitUpdateWithRetract(ACC accumulator, RetractableCollector<T> out)
 * }</pre>
 *
 * <p>If an accumulator needs to store large amounts of data, {@link ListView} and {@link MapView}
 * provide advanced features for leveraging Flink's state backends in unbounded data scenarios.
 *
 * @param <T> final result type of the aggregation
 * @param <ACC> intermediate result type during the aggregation
 */
@PublicEvolving
public abstract class TableAggregateFunction<T, ACC> extends ImperativeAggregateFunction<T, ACC> {

    /**
     * Collects a record and forwards it. The collector can output retract messages with the retract
     * method. Note: This collector can only be used in the {@code emitUpdateWithRetract()} method.
     */
    @PublicEvolving
    public interface RetractableCollector<T> extends Collector<T> {

        /**
         * Retract a record.
         *
         * @param record The record to retract.
         */
        void retract(T record);
    }

    @Override
    public final FunctionKind getKind() {
        return FunctionKind.TABLE_AGGREGATE;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInferenceExtractor.forTableAggregateFunction(typeFactory, (Class) getClass());
    }
}
