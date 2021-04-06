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

package org.apache.flink.table.planner.functions.inference;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeInferenceUtil;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.type.SqlOperandTypeInference;

import java.util.List;

import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTypeFactory;
import static org.apache.flink.table.types.inference.TypeInferenceUtil.createUnexpectedException;

/**
 * A {@link SqlOperandTypeInference} backed by {@link TypeInference}.
 *
 * <p>Note: This class must be kept in sync with {@link TypeInferenceUtil}.
 */
@Internal
public final class TypeInferenceOperandInference implements SqlOperandTypeInference {

    private final DataTypeFactory dataTypeFactory;

    private final FunctionDefinition definition;

    private final TypeInference typeInference;

    public TypeInferenceOperandInference(
            DataTypeFactory dataTypeFactory,
            FunctionDefinition definition,
            TypeInference typeInference) {
        this.dataTypeFactory = dataTypeFactory;
        this.definition = definition;
        this.typeInference = typeInference;
    }

    @Override
    public void inferOperandTypes(
            SqlCallBinding callBinding, RelDataType returnType, RelDataType[] operandTypes) {
        final CallContext callContext =
                new CallBindingCallContext(dataTypeFactory, definition, callBinding, returnType);
        try {
            inferOperandTypesOrError(unwrapTypeFactory(callBinding), callContext, operandTypes);
        } catch (ValidationException e) {
            // let operand checker fail
        } catch (Throwable t) {
            throw createUnexpectedException(callContext, t);
        }
    }

    // --------------------------------------------------------------------------------------------

    private void inferOperandTypesOrError(
            FlinkTypeFactory typeFactory, CallContext callContext, RelDataType[] operandTypes) {
        final List<DataType> expectedDataTypes;
        // typed arguments have highest priority
        if (typeInference.getTypedArguments().isPresent()) {
            expectedDataTypes = typeInference.getTypedArguments().get();
        } else {
            expectedDataTypes =
                    typeInference
                            .getInputTypeStrategy()
                            .inferInputTypes(callContext, false)
                            .orElse(null);
        }

        // early out for invalid input
        if (expectedDataTypes == null || expectedDataTypes.size() != operandTypes.length) {
            return;
        }

        for (int i = 0; i < operandTypes.length; i++) {
            final LogicalType inferredType = expectedDataTypes.get(i).getLogicalType();
            operandTypes[i] = typeFactory.createFieldTypeFromLogicalType(inferredType);
        }
    }
}
