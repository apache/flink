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

package org.apache.flink.table.planner.functions.sql.ml;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentCount;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Aggregation function for evaluating models based on task type. */
@Internal
public class MLEvaluationAggregationFunction extends AggregateFunction<Row, Object> {

    private final String task;

    public MLEvaluationAggregationFunction(String task) {
        this.task = task;
    }

    private TypeInference typeInference(DataTypeFactory typeFactory) {
        return TypeInference.newBuilder()
                .inputTypeStrategy(
                        new InputTypeStrategy() {
                            @Override
                            public ArgumentCount getArgumentCount() {
                                return new ArgumentCount() {
                                    @Override
                                    public boolean isValidCount(int count) {
                                        return count == 2;
                                    }

                                    @Override
                                    public Optional<Integer> getMinCount() {
                                        return Optional.of(2);
                                    }

                                    @Override
                                    public Optional<Integer> getMaxCount() {
                                        return Optional.of(2);
                                    }
                                };
                            }

                            @Override
                            public Optional<List<DataType>> inferInputTypes(
                                    CallContext callContext, boolean throwOnFailure) {
                                final List<DataType> args = new ArrayList<>();
                                if (task.equalsIgnoreCase("text_generation")) {
                                    args.add(DataTypes.STRING());
                                    args.add(DataTypes.STRING());
                                } else if (task.equalsIgnoreCase("embedding")) {
                                    args.add(DataTypes.ARRAY(DataTypes.DOUBLE()));
                                    args.add(DataTypes.ARRAY(DataTypes.DOUBLE()));
                                } else {
                                    args.add(DataTypes.DOUBLE());
                                    args.add(DataTypes.DOUBLE());
                                }
                                return Optional.of(args);
                            }

                            @Override
                            public List<Signature> getExpectedSignatures(
                                    FunctionDefinition definition) {
                                final List<Signature.Argument> arguments = new ArrayList<>();
                                arguments.add(Signature.Argument.of("label"));
                                arguments.add(Signature.Argument.of("prediction"));
                                return Collections.singletonList(Signature.of(arguments));
                            }
                        })
                .outputTypeStrategy(
                        callContext ->
                                Optional.of(
                                        DataTypes.MAP(
                                                        DataTypes.STRING().notNull(),
                                                        DataTypes.DOUBLE().notNull())
                                                .notNull()
                                                .bridgedTo(Map.class)))
                .accumulatorTypeStrategy(callContext -> Optional.of(DataTypes.DOUBLE()))
                .build();
    }

    /** Creates a new accumulator based on the model task type. */
    @Override
    public Object createAccumulator() {
        return null;
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return typeInference(typeFactory);
    }

    /**
     * Accumulates input values for evaluation. The first argument is the model path, followed by
     * input features, and the last argument is the actual value (ground truth).
     */
    public void accumulate(Object acc, Object... values) {}

    /** Retracts the input values from evaluation. */
    public void retract(Object acc, Object... values) {}

    public void merge(Object acc, Iterable<Object> its) {}

    public void resetAccumulator(Object acc) {}

    @Override
    public Row getValue(Object accumulator) {
        return null;
    }

    @Override
    public String toString() {
        return "MLEvaluationAggregationFunction{" + "task='" + task + "}";
    }
}
