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
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.util.Collector;

/**
 * Base class for user-defined table aggregates.
 *
 * <p>The behavior of a {@link TableAggregateFunction} can be defined by implementing a series of
 * custom methods. A {@link TableAggregateFunction} needs at least three methods:
 * <ul>
 *     <li>createAccumulator</li>
 *     <li>accumulate</li>
 *     <li>emitValue or emitUpdateWithRetract</li>
 * </ul>
 *
 * <p>There is another method that can be optional to have:
 * <ul>
 *     <li>retract</li>
 * </ul>
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
 * Called every time when an aggregation result should be materialized. The returned value could
 * be either an early and incomplete result (periodically emitted as data arrive) or the final
 * result of the aggregation.
 *
 * param: accumulator           the accumulator which contains the current aggregated results
 * param: out                   the collector used to output data.
 *
 * public void emitValue(ACC accumulator, Collector<T> out)
 * }
 * </pre>
 *
 * <pre>
 * {@code
 * Called every time when an aggregation result should be materialized. The returned value could
 * be either an early and incomplete result (periodically emitted as data arrive) or the final
 * result of the aggregation.
 *
 * Different from emitValue, emitUpdateWithRetract is used to emit values that have been updated.
 * This method outputs data incrementally in retract mode, i.e., once there is an update, we have
 * to retract old records before sending new updated ones. The emitUpdateWithRetract method will be
 * used in preference to the emitValue method if both methods are defined in the table aggregate
 * function, because the method is treated to be more efficient than emitValue as it can output
 * values incrementally.
 *
 * param: accumulator           the accumulator which contains the current aggregated results
 * param: out                   the retractable collector used to output data. Use collect method
 *                              to output(add) records and use retract method to retract(delete)
 *                              records.
 *
 * public void emitUpdateWithRetract(ACC accumulator, RetractableCollector<T> out)
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

	/**
	 * Collects a record and forwards it. The collector can output retract messages with the retract
	 * method. Note: only use it in {@code emitUpdateWithRetract}.
	 */
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
	public TypeInference getTypeInference(DataTypeFactory typeFactory) {
		throw new TableException("Table aggregate functions are not updated to the new type system yet.");
	}
}
