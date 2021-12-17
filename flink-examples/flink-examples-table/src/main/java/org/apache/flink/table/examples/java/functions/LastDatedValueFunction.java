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

package org.apache.flink.table.examples.java.functions;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.types.Row;

import java.time.LocalDate;
import java.util.Optional;

/**
 * Implementation of an {@link AggregateFunction} that returns a row containing the latest non-null
 * value with its corresponding date.
 *
 * <p>The function uses a custom {@link TypeInference} and thus disables any of the default
 * reflection-based logic. It has a generic parameter {@code T} which will result in {@link Object}
 * (due to type erasure) during runtime. The {@link TypeInference} will provide the necessary
 * information how to call {@code accumulate(...)} for the given call in the query.
 *
 * <p>For code readability, we might use some internal utility methods that should rarely change.
 * Implementers can copy those if they don't want to rely on non-official API.
 *
 * @param <T> input value
 */
public final class LastDatedValueFunction<T>
        extends AggregateFunction<Row, LastDatedValueFunction.Accumulator<T>> {

    // --------------------------------------------------------------------------------------------
    // Planning
    // --------------------------------------------------------------------------------------------

    /**
     * Declares the {@link TypeInference} of this function. It specifies:
     *
     * <ul>
     *   <li>which argument types are supported when calling this function,
     *   <li>which {@link DataType#getConversionClass()} should be used when calling the JVM method
     *       {@link #accumulate(Accumulator, Object, LocalDate)} during runtime,
     *   <li>a similar strategy how to derive an accumulator type,
     *   <li>and a similar strategy how to derive the output type.
     * </ul>
     */
    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInference.newBuilder()
                // accept a signature (ANY, DATE) both with default conversion classes,
                // the input type strategy is mostly used to produce nicer validation exceptions
                // during planning, implementers can decide to skip it if they are fine with failing
                // at a later stage during code generation when the runtime method is checked
                .inputTypeStrategy(
                        InputTypeStrategies.sequence(
                                InputTypeStrategies.ANY,
                                InputTypeStrategies.explicit(DataTypes.DATE())))
                // let the accumulator data type depend on the first input argument
                .accumulatorTypeStrategy(
                        callContext -> {
                            final DataType argDataType = callContext.getArgumentDataTypes().get(0);
                            final DataType accDataType =
                                    DataTypes.STRUCTURED(
                                            Accumulator.class,
                                            DataTypes.FIELD("value", argDataType),
                                            DataTypes.FIELD("date", DataTypes.DATE()));
                            return Optional.of(accDataType);
                        })
                // let the output data type depend on the first input argument
                .outputTypeStrategy(
                        callContext -> {
                            final DataType argDataType = callContext.getArgumentDataTypes().get(0);
                            final DataType outputDataType =
                                    DataTypes.ROW(
                                            DataTypes.FIELD("value", argDataType),
                                            DataTypes.FIELD("date", DataTypes.DATE()));
                            return Optional.of(outputDataType);
                        })
                .build();
    }

    // --------------------------------------------------------------------------------------------
    // Runtime
    // --------------------------------------------------------------------------------------------

    /**
     * Generic accumulator for representing state. It will contain different kind of instances for
     * {@code value} depending on actual call in the query.
     */
    public static class Accumulator<T> {
        public T value;
        public LocalDate date;
    }

    @Override
    public Accumulator<T> createAccumulator() {
        return new Accumulator<>();
    }

    /**
     * Generic runtime function that will be called with different kind of instances for {@code
     * input} depending on actual call in the query.
     */
    public void accumulate(Accumulator<T> acc, T input, LocalDate date) {
        if (input != null && (acc.date == null || date.isAfter(acc.date))) {
            acc.value = input;
            acc.date = date;
        }
    }

    @Override
    public Row getValue(Accumulator<T> acc) {
        return Row.of(acc.value, acc.date);
    }
}
