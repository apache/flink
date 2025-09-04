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

package org.apache.flink.table.types.inference.strategies;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeStrategiesTestBase;
import org.apache.flink.table.types.inference.utils.ModelSemanticsMock;
import org.apache.flink.table.types.inference.utils.TableSemanticsMock;

import java.util.stream.Stream;

import static org.apache.flink.table.types.inference.strategies.SpecificTypeStrategies.ML_PREDICT_OUTPUT_TYPE_STRATEGY;

/** Tests for {@link SpecificTypeStrategies#ML_PREDICT_OUTPUT_TYPE_STRATEGY}. */
class MLPredictOutputTypeStrategyTest extends TypeStrategiesTestBase {

    private static final DataType TABLE_TYPE =
            DataTypes.ROW(
                    DataTypes.FIELD("id", DataTypes.INT()),
                    DataTypes.FIELD("feature1", DataTypes.DOUBLE()),
                    DataTypes.FIELD("feature2", DataTypes.DOUBLE()));

    private static final DataType MODEL_INPUT_TYPE =
            DataTypes.ROW(
                    DataTypes.FIELD("feature1", DataTypes.DOUBLE()),
                    DataTypes.FIELD("feature2", DataTypes.DOUBLE()));

    private static final DataType MODEL_OUTPUT_TYPE =
            DataTypes.ROW(DataTypes.FIELD("prediction", DataTypes.DOUBLE()));

    // Model output type with field name that conflicts with table
    private static final DataType MODEL_OUTPUT_TYPE_CONFLICTING =
            DataTypes.ROW(DataTypes.FIELD("id", DataTypes.STRING()));

    // Model output type with multiple fields
    private static final DataType MODEL_OUTPUT_TYPE_MULTIPLE =
            DataTypes.ROW(
                    DataTypes.FIELD("prediction", DataTypes.DOUBLE()),
                    DataTypes.FIELD("confidence", DataTypes.DOUBLE()));

    @Override
    protected Stream<TestSpec> testData() {
        return Stream.of(
                // Basic case: table + model output without conflicts
                TestSpec.forStrategy(
                                "Basic output type inference with no field conflicts",
                                ML_PREDICT_OUTPUT_TYPE_STRATEGY)
                        .inputTypes(TABLE_TYPE, MODEL_INPUT_TYPE)
                        .calledWithTableSemanticsAt(0, new TableSemanticsMock(TABLE_TYPE))
                        .calledWithModelSemanticsAt(
                                1, new ModelSemanticsMock(MODEL_INPUT_TYPE, MODEL_OUTPUT_TYPE))
                        .expectDataType(
                                DataTypes.ROW(
                                        DataTypes.FIELD("id", DataTypes.INT()),
                                        DataTypes.FIELD("feature1", DataTypes.DOUBLE()),
                                        DataTypes.FIELD("feature2", DataTypes.DOUBLE()),
                                        DataTypes.FIELD("prediction", DataTypes.DOUBLE()))),

                // Case with field name conflict - model field should be renamed
                TestSpec.forStrategy(
                                "Output type inference with field name conflict",
                                ML_PREDICT_OUTPUT_TYPE_STRATEGY)
                        .inputTypes(TABLE_TYPE, MODEL_INPUT_TYPE)
                        .calledWithTableSemanticsAt(0, new TableSemanticsMock(TABLE_TYPE))
                        .calledWithModelSemanticsAt(
                                1,
                                new ModelSemanticsMock(
                                        MODEL_INPUT_TYPE, MODEL_OUTPUT_TYPE_CONFLICTING))
                        .expectDataType(
                                DataTypes.ROW(
                                        DataTypes.FIELD("id", DataTypes.INT()),
                                        DataTypes.FIELD("feature1", DataTypes.DOUBLE()),
                                        DataTypes.FIELD("feature2", DataTypes.DOUBLE()),
                                        DataTypes.FIELD(
                                                "id0",
                                                DataTypes.STRING()))), // Renamed to avoid conflict

                // Case with multiple model output fields
                TestSpec.forStrategy(
                                "Output type inference with multiple model fields",
                                ML_PREDICT_OUTPUT_TYPE_STRATEGY)
                        .inputTypes(TABLE_TYPE, MODEL_INPUT_TYPE)
                        .calledWithTableSemanticsAt(0, new TableSemanticsMock(TABLE_TYPE))
                        .calledWithModelSemanticsAt(
                                1,
                                new ModelSemanticsMock(
                                        MODEL_INPUT_TYPE, MODEL_OUTPUT_TYPE_MULTIPLE))
                        .expectDataType(
                                DataTypes.ROW(
                                        DataTypes.FIELD("id", DataTypes.INT()),
                                        DataTypes.FIELD("feature1", DataTypes.DOUBLE()),
                                        DataTypes.FIELD("feature2", DataTypes.DOUBLE()),
                                        DataTypes.FIELD("prediction", DataTypes.DOUBLE()),
                                        DataTypes.FIELD("confidence", DataTypes.DOUBLE()))),

                // Error case: no table semantics
                TestSpec.forStrategy(
                                "Error when table semantics missing",
                                ML_PREDICT_OUTPUT_TYPE_STRATEGY)
                        .inputTypes(TABLE_TYPE, MODEL_INPUT_TYPE)
                        .calledWithModelSemanticsAt(
                                1, new ModelSemanticsMock(MODEL_INPUT_TYPE, MODEL_OUTPUT_TYPE))
                        .expectErrorMessage(
                                "First argument must be a table for ML_PREDICT function."),

                // Error case: no model semantics
                TestSpec.forStrategy(
                                "Error when model semantics missing",
                                ML_PREDICT_OUTPUT_TYPE_STRATEGY)
                        .inputTypes(TABLE_TYPE, MODEL_INPUT_TYPE)
                        .calledWithTableSemanticsAt(0, new TableSemanticsMock(TABLE_TYPE))
                        .expectErrorMessage(
                                "Second argument must be a model for ML_PREDICT function."),

                // Error case: table type is not a row
                TestSpec.forStrategy(
                                "Error when table type is not a row",
                                ML_PREDICT_OUTPUT_TYPE_STRATEGY)
                        .inputTypes(DataTypes.STRING(), MODEL_INPUT_TYPE)
                        .calledWithTableSemanticsAt(0, new TableSemanticsMock(DataTypes.STRING()))
                        .calledWithModelSemanticsAt(
                                1, new ModelSemanticsMock(MODEL_INPUT_TYPE, MODEL_OUTPUT_TYPE))
                        .expectErrorMessage(
                                "Both table and model output types must be row types for ML_PREDICT function."),

                // Error case: model output type is not a row
                TestSpec.forStrategy(
                                "Error when model output type is not a row",
                                ML_PREDICT_OUTPUT_TYPE_STRATEGY)
                        .inputTypes(TABLE_TYPE, MODEL_INPUT_TYPE)
                        .calledWithTableSemanticsAt(0, new TableSemanticsMock(TABLE_TYPE))
                        .calledWithModelSemanticsAt(
                                1, new ModelSemanticsMock(MODEL_INPUT_TYPE, DataTypes.STRING()))
                        .expectErrorMessage(
                                "Both table and model output types must be row types for ML_PREDICT function."));
    }
}
