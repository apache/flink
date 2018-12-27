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

/**
 * Base class for user-defined table aggregates.
 *
 * <p>The behavior of a {@link TableAggregateFunction} can be defined by implementing a series of custom
 * methods. A {@link TableAggregateFunction} needs at least three methods:
 *  - <code>createAccumulator</code>,
 *  - <code>accumulate</code>, and
 *  - <code>emitValue</code> or <code>emitValueWithRetract</code>.
 *
 * <p>There are a few other methods that can be optional to have:
 *  - <code>retract</code>,
 *  - <code>merge</code>, and
 *  - <code>resetAccumulator</code>.
 *
 * <p>All these methods must be declared publicly, not static, and named exactly as the names
 * mentioned above. The method {@link #createAccumulator()} is defined in
 * the {@link UserDefinedAggregateFunction} functions, while other methods are explained below.
 *
 * <pre>
 * {@code
 * Processes the input values and update the provided accumulator instance. The method
 * accumulate can be overloaded with different custom types and arguments. A TableAggregateFunction
 * requires at least one accumulate() method.
 *
 * param: accumulator           the accumulator which contains the current aggregated results
 * param: [user defined inputs] the input value (usually obtained from a new arrived data).
 *
 * public void accumulate(ACC accumulator, [user defined inputs])
 * }
 * </pre>
 *
 * <pre>
 * {@code
 * Retracts the input values from the accumulator instance. The current design assumes the
 * inputs are the values that have been previously accumulated. The method retract can be
 * overloaded with different custom types and arguments.
 *
 * param: accumulator           the accumulator which contains the current aggregated results
 * param: [user defined inputs] the input value (usually obtained from a new arrived data).
 *
 * public void retract(ACC accumulator, [user defined inputs])
 * }
 * </pre>
 *
 * <pre>
 * {@code
 * Merges a group of accumulator instances into one accumulator instance. This function must be
 * implemented for data stream session window grouping aggregates and data set grouping aggregates.
 *
 * param: accumulator the accumulator which will keep the merged aggregate results. It should
 *                    be noted that the accumulator may contain the previous aggregated
 *                    results. Therefore user should not replace or clean this instance in the
 *                    custom merge method.
 * param: its         an java.lang.Iterable pointed to a group of accumulators that will be
 *                    merged.
 *
 * public void merge(ACC accumulator, java.lang.Iterable<ACC> iterable)
 * }
 * </pre>
 *
 * <pre>
 * {@code
 * Resets the accumulator for this TableAggregateFunction. This function must be implemented for
 * data set grouping aggregates.
 *
 * param: accumulator the accumulator which needs to be reset
 *
 * public void resetAccumulator(ACC accumulator)
 * }
 * </pre>
 *
 * <pre>
 * {@code
 * Output data incrementally in upsert or append mode. For example, if we emit data for a TopN
 * TableAggregateFunction, we don't need to output all top N elements each time a record comes.
 * It is more efficient to output data incrementally in upsert mode, i.e, only output data whose
 * rank has been changed.
 *
 * param: accumulator           the accumulator which contains the current aggregated results
 * param: out                   the collector used to output data.
 *
 * public void emitValue(ACC accumulator, RetractableCollector out)
 * }
 * </pre>
 *
 * <pre>
 * {@code
 * Output data incrementally in retract mode. Once there is an update, we have to retract old
 * records before sending new updated ones.
 *
 * param: accumulator           the accumulator which contains the current aggregated results
 * param: out                   the collector used to output data.
 *
 * public void emitValueWithRetract(ACC accumulator, RetractableCollector out)
 * }
 * </pre>
 *
 * @param <T>   the type of the table aggregation result
 * @param <ACC> the type of the table aggregation accumulator. The accumulator is used to keep the
 *              aggregated values which are needed to compute an aggregation result.
 *              TableAggregateFunction represents its state using accumulator, thereby the state of
 *              the TableAggregateFunction must be put into the accumulator.
 */
@PublicEvolving
public abstract class TableAggregateFunction<T, ACC> extends UserDefinedAggregateFunction<T, ACC> {

}
