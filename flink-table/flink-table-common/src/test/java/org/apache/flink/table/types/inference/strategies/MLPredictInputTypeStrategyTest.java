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
import org.apache.flink.table.types.inference.InputTypeStrategiesTestBase;
import org.apache.flink.table.types.inference.utils.ModelSemanticsMock;
import org.apache.flink.table.types.inference.utils.TableSemanticsMock;
import org.apache.flink.types.ColumnList;

import java.util.List;
import java.util.stream.Stream;

import static org.apache.flink.table.types.inference.strategies.SpecificInputTypeStrategies.ML_PREDICT_INPUT_TYPE_STRATEGY;

/** Tests for {@link SpecificInputTypeStrategies#ML_PREDICT_INPUT_TYPE_STRATEGY}. */
class MLPredictInputTypeStrategyTest extends InputTypeStrategiesTestBase {

    private static final DataType TABLE_TYPE =
            DataTypes.ROW(
                    DataTypes.FIELD("id", DataTypes.STRING()),
                    DataTypes.FIELD("feature1", DataTypes.DOUBLE()),
                    DataTypes.FIELD("feature2", DataTypes.DOUBLE()));

    private static final DataType MODEL_INPUT_TYPE =
            DataTypes.ROW(
                    DataTypes.FIELD("feature1", DataTypes.DOUBLE()),
                    DataTypes.FIELD("feature2", DataTypes.DOUBLE()));
    private static final DataType MODEL_OUTPUT_TYPE =
            DataTypes.ROW(DataTypes.FIELD("prediction", DataTypes.DOUBLE()));

    private static final DataType DESCRIPTOR_TYPE = DataTypes.DESCRIPTOR();

    private static final DataType MAP_TYPE = DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING());

    @Override
    protected Stream<TestSpec> testData() {
        return Stream.of(
                // Valid case with 3 arguments (table, model, descriptor)
                TestSpec.forStrategy(
                                "Valid ML_PREDICT with table, model, and descriptor",
                                ML_PREDICT_INPUT_TYPE_STRATEGY)
                        .calledWithArgumentTypes(TABLE_TYPE, MODEL_INPUT_TYPE, DESCRIPTOR_TYPE)
                        .calledWithTableSemanticsAt(0, new TableSemanticsMock(TABLE_TYPE))
                        .calledWithModelSemanticsAt(
                                1, new ModelSemanticsMock(MODEL_INPUT_TYPE, MODEL_OUTPUT_TYPE))
                        .calledWithLiteralAt(2, ColumnList.of(List.of("feature1", "feature2")))
                        .expectSignature(
                                "f(TABLE ROW, MODEL MODEL, ARGS DESCRIPTOR)\n"
                                        + "f(TABLE ROW, MODEL MODEL, ARGS DESCRIPTOR, CONFIG MAP)")
                        .expectArgumentTypes(TABLE_TYPE, MODEL_INPUT_TYPE, DESCRIPTOR_TYPE),

                // Valid case with 4 arguments (table, model, descriptor, config)
                TestSpec.forStrategy(
                                "Valid ML_PREDICT with table, model, descriptor, and config",
                                ML_PREDICT_INPUT_TYPE_STRATEGY)
                        .calledWithArgumentTypes(
                                TABLE_TYPE, MODEL_INPUT_TYPE, DESCRIPTOR_TYPE, MAP_TYPE)
                        .calledWithTableSemanticsAt(0, new TableSemanticsMock(TABLE_TYPE))
                        .calledWithModelSemanticsAt(
                                1, new ModelSemanticsMock(MODEL_INPUT_TYPE, MODEL_OUTPUT_TYPE))
                        .calledWithLiteralAt(2, ColumnList.of(List.of("feature1", "feature2")))
                        .expectArgumentTypes(
                                TABLE_TYPE, MODEL_INPUT_TYPE, DESCRIPTOR_TYPE, MAP_TYPE),

                // Error case: descriptor column not found in table
                TestSpec.forStrategy(
                                "Descriptor column not found in table",
                                ML_PREDICT_INPUT_TYPE_STRATEGY)
                        .calledWithArgumentTypes(TABLE_TYPE, MODEL_INPUT_TYPE, DESCRIPTOR_TYPE)
                        .calledWithTableSemanticsAt(0, new TableSemanticsMock(TABLE_TYPE))
                        .calledWithModelSemanticsAt(
                                1, new ModelSemanticsMock(MODEL_INPUT_TYPE, MODEL_OUTPUT_TYPE))
                        .calledWithLiteralAt(2, ColumnList.of(List.of("nonexistent_column")))
                        .expectErrorMessage(
                                "Descriptor column 'nonexistent_column' not found in table columns. Available columns: id, feature1, feature2."),

                // Error case: descriptor column count doesn't match model input size
                TestSpec.forStrategy(
                                "Descriptor column count mismatch with model input",
                                ML_PREDICT_INPUT_TYPE_STRATEGY)
                        .calledWithArgumentTypes(TABLE_TYPE, MODEL_INPUT_TYPE, DESCRIPTOR_TYPE)
                        .calledWithTableSemanticsAt(0, new TableSemanticsMock(TABLE_TYPE))
                        .calledWithModelSemanticsAt(
                                1, new ModelSemanticsMock(MODEL_INPUT_TYPE, MODEL_OUTPUT_TYPE))
                        .calledWithLiteralAt(
                                2, ColumnList.of(List.of("feature1"))) // Only one column
                        .expectErrorMessage(
                                "Number of descriptor columns (1) does not match model input size (2)."),

                // Error case: descriptor column type incompatible with model input type
                TestSpec.forStrategy(
                                "Descriptor column type incompatible with model input",
                                ML_PREDICT_INPUT_TYPE_STRATEGY)
                        .calledWithArgumentTypes(TABLE_TYPE, MODEL_INPUT_TYPE, DESCRIPTOR_TYPE)
                        .calledWithTableSemanticsAt(0, new TableSemanticsMock(TABLE_TYPE))
                        .calledWithModelSemanticsAt(
                                1, new ModelSemanticsMock(MODEL_INPUT_TYPE, MODEL_OUTPUT_TYPE))
                        .calledWithLiteralAt(
                                2,
                                ColumnList.of(
                                        List.of(
                                                "id",
                                                "feature1"))) // id is INT, model expects DOUBLE
                        .expectErrorMessage(
                                "Descriptor column 'id' type STRING cannot be assigned to model input type DOUBLE at position 0."),

                // Error case: too few arguments
                TestSpec.forStrategy("Too few arguments", ML_PREDICT_INPUT_TYPE_STRATEGY)
                        .calledWithArgumentTypes(TABLE_TYPE, MODEL_INPUT_TYPE)
                        .expectErrorMessage(
                                "Invalid number of arguments. At least 3 arguments expected but 2 passed."),

                // Error case: too many arguments
                TestSpec.forStrategy("Too many arguments", ML_PREDICT_INPUT_TYPE_STRATEGY)
                        .calledWithArgumentTypes(
                                TABLE_TYPE,
                                MODEL_INPUT_TYPE,
                                DESCRIPTOR_TYPE,
                                MAP_TYPE,
                                DataTypes.STRING())
                        .expectErrorMessage(
                                "Invalid number of arguments. At most 4 arguments expected but 5 passed."),

                // Error case: first argument not a table
                TestSpec.forStrategy("First argument not a table", ML_PREDICT_INPUT_TYPE_STRATEGY)
                        .calledWithArgumentTypes(
                                DataTypes.STRING(), MODEL_INPUT_TYPE, DESCRIPTOR_TYPE)
                        .expectErrorMessage(
                                "First argument must be a table for ML_PREDICT function."),

                // Error case: second argument not a model
                TestSpec.forStrategy("Second argument not a model", ML_PREDICT_INPUT_TYPE_STRATEGY)
                        .calledWithTableSemanticsAt(0, new TableSemanticsMock(TABLE_TYPE))
                        .calledWithArgumentTypes(TABLE_TYPE, DataTypes.STRING(), DESCRIPTOR_TYPE)
                        .expectErrorMessage(
                                "Second argument must be a model for ML_PREDICT function."),

                // Error case: third argument not a descriptor
                TestSpec.forStrategy(
                                "Third argument not a descriptor", ML_PREDICT_INPUT_TYPE_STRATEGY)
                        .calledWithArgumentTypes(TABLE_TYPE, MODEL_INPUT_TYPE, DataTypes.STRING())
                        .calledWithTableSemanticsAt(0, new TableSemanticsMock(TABLE_TYPE))
                        .calledWithModelSemanticsAt(
                                1, new ModelSemanticsMock(MODEL_INPUT_TYPE, MODEL_OUTPUT_TYPE))
                        .expectErrorMessage(
                                "Third argument must be a descriptor with simple column names for ML_PREDICT function."));
    }
}
