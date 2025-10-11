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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.DataTypes.Field;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.ModelSemantics;
import org.apache.flink.table.functions.TableSemantics;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentCount;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.ConstantArgumentCount;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.inference.Signature.Argument;
import org.apache.flink.table.types.inference.TypeStrategy;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeCasts;
import org.apache.flink.types.ColumnList;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** Type strategies for ML_PREDICT function. */
@Internal
public class MLPredictTypeStrategy {
    public static final InputTypeStrategy ML_PREDICT_INPUT_TYPE_STRATEGY =
            new InputTypeStrategy() {
                @Override
                public ArgumentCount getArgumentCount() {
                    return ConstantArgumentCount.between(3, 4);
                }

                @Override
                public Optional<List<DataType>> inferInputTypes(
                        CallContext callContext, boolean throwOnFailure) {
                    return inferMLPredictInputTypes(callContext, throwOnFailure);
                }

                @Override
                public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
                    return List.of(
                            Signature.of(
                                    Argument.of("TABLE", "ROW"),
                                    Argument.of("MODEL", "MODEL"),
                                    Argument.of("ARGS", "DESCRIPTOR")),
                            Signature.of(
                                    Argument.of("TABLE", "ROW"),
                                    Argument.of("MODEL", "MODEL"),
                                    Argument.of("ARGS", "DESCRIPTOR"),
                                    Argument.of("CONFIG", "MAP")));
                }
            };

    public static final TypeStrategy ML_PREDICT_OUTPUT_TYPE_STRATEGY =
            callContext -> {
                // The output type of the model prediction is always a row type with a single field
                // named "prediction" of type DOUBLE.
                TableSemantics tableSemantics = callContext.getTableSemantics(0).orElse(null);
                if (tableSemantics == null) {
                    throw new ValidationException(
                            "First argument must be a table for ML_PREDICT function.");
                }
                final ModelSemantics modelSemantics = callContext.getModelSemantics(1).orElse(null);
                if (modelSemantics == null) {
                    throw new ValidationException(
                            "Second argument must be a model for ML_PREDICT function.");
                }

                LogicalType tableType = tableSemantics.dataType().getLogicalType();
                LogicalType modelOutputType = modelSemantics.outputDataType().getLogicalType();
                if (!tableType.is(LogicalTypeRoot.ROW)
                        || !modelOutputType.is(LogicalTypeRoot.ROW)) {
                    throw new ValidationException(
                            "Both table and model output types must be row types for ML_PREDICT function.");
                }
                List<Field> tableFields = DataType.getFields(tableSemantics.dataType());
                List<Field> modelFields = DataType.getFields(modelSemantics.outputDataType());
                List<Field> outputFields = new ArrayList<>(tableFields);
                Set<String> tableFieldNames =
                        tableFields.stream().map(Field::getName).collect(Collectors.toSet());
                for (Field modelField : modelFields) {
                    String fieldName = modelField.getName();
                    if (tableFieldNames.contains(modelField.getName())) {
                        fieldName = fieldName + "0";
                    }
                    outputFields.add(DataTypes.FIELD(fieldName, modelField.getDataType()));
                }
                return Optional.of(DataTypes.ROW(outputFields));
            };

    private static Optional<List<DataType>> inferMLPredictInputTypes(
            CallContext callContext, boolean throwOnFailure) {
        // Check that first argument is a table
        TableSemantics tableSemantics = callContext.getTableSemantics(0).orElse(null);
        if (tableSemantics == null) {
            if (throwOnFailure) {
                throw new ValidationException(
                        "First argument must be a table for ML_PREDICT function.");
            } else {
                return Optional.empty();
            }
        }

        // Check that second argument is a model
        ModelSemantics modelSemantics = callContext.getModelSemantics(1).orElse(null);
        if (modelSemantics == null) {
            if (throwOnFailure) {
                throw new ValidationException(
                        "Second argument must be a model for ML_PREDICT function.");
            } else {
                return Optional.empty();
            }
        }

        // Check that third argument is a descriptor with column names
        Optional<ColumnList> descriptorColumns = callContext.getArgumentValue(2, ColumnList.class);
        if (descriptorColumns.isEmpty()) {
            if (throwOnFailure) {
                throw new ValidationException(
                        "Third argument must be a descriptor with simple column names for ML_PREDICT function.");
            } else {
                return Optional.empty();
            }
        }

        if (!validateTableAndDescriptorArguments(
                tableSemantics, descriptorColumns.get(), throwOnFailure)) {
            return Optional.empty();
        }

        if (!validateModelDescriptorCompatibility(
                tableSemantics, modelSemantics, descriptorColumns.get(), throwOnFailure)) {
            return Optional.empty();
        }

        // Config map validation is done in StreamPhysicalMLPredictTableFunctionRule since
        // we are not able to get map literal here.
        return Optional.of(callContext.getArgumentDataTypes());
    }

    private static boolean validateTableAndDescriptorArguments(
            TableSemantics tableSemantics, ColumnList descriptorColumns, boolean throwOnFailure) {

        // Check that descriptor column names exist in table columns
        List<String> tableFieldNames = DataType.getFieldNames(tableSemantics.dataType());
        List<String> descriptorColumnNames = descriptorColumns.getNames();

        for (String descriptorColumnName : descriptorColumnNames) {
            if (!tableFieldNames.contains(descriptorColumnName)) {
                if (throwOnFailure) {
                    throw new ValidationException(
                            String.format(
                                    "Descriptor column '%s' not found in table columns. "
                                            + "Available columns: %s.",
                                    descriptorColumnName, String.join(", ", tableFieldNames)));
                } else {
                    return false;
                }
            }
        }

        return true;
    }

    private static boolean validateModelDescriptorCompatibility(
            TableSemantics tableSemantics,
            ModelSemantics modelSemantics,
            ColumnList descriptorColumns,
            boolean throwOnFailure) {

        // Check descriptor columns match model input size and types
        DataType modelInputDataType = modelSemantics.inputDataType();
        List<Field> modelInputFields = DataType.getFields(modelInputDataType);
        List<String> descriptorColumnNames = descriptorColumns.getNames();

        // Check size compatibility
        if (descriptorColumnNames.size() != modelInputFields.size()) {
            if (throwOnFailure) {
                throw new ValidationException(
                        String.format(
                                "Number of descriptor columns (%d) does not match model input size (%d).",
                                descriptorColumnNames.size(), modelInputFields.size()));
            } else {
                return false;
            }
        }

        // Check type compatibility for each descriptor column
        List<Field> tableFields = DataType.getFields(tableSemantics.dataType());
        for (int i = 0; i < descriptorColumnNames.size(); i++) {
            String descriptorColumnName = descriptorColumnNames.get(i);

            // Find the descriptor column's type in the table
            Field tableField =
                    tableFields.stream()
                            .filter(field -> field.getName().equals(descriptorColumnName))
                            .findFirst()
                            .orElseThrow(
                                    () ->
                                            new IllegalStateException(
                                                    "Column should exist")); // Should not happen
            // due to earlier check

            LogicalType tableColumnType = tableField.getDataType().getLogicalType();
            LogicalType modelInputColumnType =
                    modelInputFields.get(i).getDataType().getLogicalType();

            if (!LogicalTypeCasts.supportsImplicitCast(tableColumnType, modelInputColumnType)) {
                if (throwOnFailure) {
                    throw new ValidationException(
                            String.format(
                                    "Descriptor column '%s' type %s cannot be assigned to model input type %s at position %d.",
                                    descriptorColumnName,
                                    tableColumnType,
                                    modelInputColumnType,
                                    i));
                } else {
                    return false;
                }
            }
        }

        return true;
    }
}
