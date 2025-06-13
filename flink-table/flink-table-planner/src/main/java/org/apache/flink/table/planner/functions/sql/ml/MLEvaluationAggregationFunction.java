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
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.ml.TaskType;
import org.apache.flink.table.planner.functions.sql.ml.evaluate.ClassificationEvaluatorAccumulator;
import org.apache.flink.table.planner.functions.sql.ml.evaluate.EmbeddingEvaluatorAccumulator;
import org.apache.flink.table.planner.functions.sql.ml.evaluate.ModelEvaluatorAccumulator;
import org.apache.flink.table.planner.functions.sql.ml.evaluate.RegressionEvaluatorAccumulator;
import org.apache.flink.table.planner.functions.sql.ml.evaluate.TextGenerationEvaluatorAccumulator;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentCount;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.inference.TypeInference;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Base class for model evaluation aggregation functions. */
@Internal
public abstract class MLEvaluationAggregationFunction
        extends AggregateFunction<Map<String, Double>, ModelEvaluatorAccumulator> {

    protected MLEvaluationAggregationFunction() {}

    protected TypeInference typeInference(DataTypeFactory typeFactory) {
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
                                return Optional.of(getInputTypes());
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
                        callContext -> Optional.of(ModelEvaluatorAccumulator.getOutputType()))
                .accumulatorTypeStrategy(
                        callContext ->
                                Optional.of(
                                        DataTypes.RAW(getAccumulatorClass())
                                                .toDataType(typeFactory)))
                .build();
    }

    /** Returns the input types for this task type. */
    protected abstract List<DataType> getInputTypes();

    protected abstract Class<? extends ModelEvaluatorAccumulator> getAccumulatorClass();

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return typeInference(typeFactory);
    }

    /**
     * Accumulates input values for evaluation. The first argument is the model path, followed by
     * input features, and the last argument is the actual value (ground truth).
     */
    public void accumulate(ModelEvaluatorAccumulator acc, Object... values) {
        acc.accumulate(values);
    }

    /** Retracts the input values from evaluation. */
    public void retract(ModelEvaluatorAccumulator acc, Object... values) {
        acc.retract(values);
    }

    public void merge(ModelEvaluatorAccumulator acc, Iterable<ModelEvaluatorAccumulator> its) {
        for (ModelEvaluatorAccumulator other : its) {
            acc.merge(other);
        }
    }

    public void resetAccumulator(ModelEvaluatorAccumulator acc) {
        acc.reset();
    }

    @Override
    public Map<String, Double> getValue(ModelEvaluatorAccumulator acc) {
        return acc.getValue();
    }

    /** Factory method to create the appropriate evaluation function for a task type. */
    public static MLEvaluationAggregationFunction create(String task) {
        TaskType.throwOrReturnInvalidTaskType(task, true);
        TaskType taskType = TaskType.fromName(task);
        if (!taskType.supportsEvaluation()) {
            throw new ValidationException("Task '" + task + "' is not supported for evaluation.");
        }
        switch (taskType) {
            case TEXT_GENERATION:
                return new TextGenerationEvaluationFunction();
            case EMBEDDING:
                return new EmbeddingEvaluationFunction();
            case CLASSIFICATION:
                return new ClassificationEvaluationFunction();
            case REGRESSION:
                return new RegressionEvaluationFunction();
            default:
                throw new ValidationException(
                        "Task '" + task + "' is not supported for evaluation.");
        }
    }

    /** Evaluation function for text generation tasks. */
    public static class TextGenerationEvaluationFunction extends MLEvaluationAggregationFunction {
        public TextGenerationEvaluationFunction() {
            super();
        }

        @Override
        protected List<DataType> getInputTypes() {
            return List.of(DataTypes.STRING(), DataTypes.STRING());
        }

        @Override
        protected Class<? extends ModelEvaluatorAccumulator> getAccumulatorClass() {
            return TextGenerationEvaluatorAccumulator.class;
        }

        @Override
        public ModelEvaluatorAccumulator createAccumulator() {
            return new TextGenerationEvaluatorAccumulator();
        }
    }

    /** Evaluation function for embedding tasks. */
    public static class EmbeddingEvaluationFunction extends MLEvaluationAggregationFunction {
        public EmbeddingEvaluationFunction() {
            super();
        }

        @Override
        protected List<DataType> getInputTypes() {
            return List.of(DataTypes.ARRAY(DataTypes.FLOAT()), DataTypes.ARRAY(DataTypes.FLOAT()));
        }

        @Override
        protected Class<? extends ModelEvaluatorAccumulator> getAccumulatorClass() {
            return EmbeddingEvaluatorAccumulator.class;
        }

        @Override
        public ModelEvaluatorAccumulator createAccumulator() {
            return new EmbeddingEvaluatorAccumulator();
        }
    }

    /** Evaluation function for classification tasks. */
    public static class ClassificationEvaluationFunction extends MLEvaluationAggregationFunction {
        public ClassificationEvaluationFunction() {
            super();
        }

        @Override
        protected List<DataType> getInputTypes() {
            return List.of(DataTypes.STRING(), DataTypes.STRING());
        }

        @Override
        protected Class<? extends ModelEvaluatorAccumulator> getAccumulatorClass() {
            return ClassificationEvaluatorAccumulator.class;
        }

        @Override
        public ModelEvaluatorAccumulator createAccumulator() {
            return new ClassificationEvaluatorAccumulator();
        }
    }

    /** Evaluation function for regression tasks. */
    public static class RegressionEvaluationFunction extends MLEvaluationAggregationFunction {
        public RegressionEvaluationFunction() {
            super();
        }

        @Override
        protected List<DataType> getInputTypes() {
            return List.of(DataTypes.DOUBLE(), DataTypes.DOUBLE());
        }

        @Override
        protected Class<? extends ModelEvaluatorAccumulator> getAccumulatorClass() {
            return RegressionEvaluatorAccumulator.class;
        }

        @Override
        public ModelEvaluatorAccumulator createAccumulator() {
            return new RegressionEvaluatorAccumulator();
        }
    }
}
