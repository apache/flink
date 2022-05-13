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
import org.apache.flink.table.planner.calcite.FlinkCalciteSqlValidator;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeInferenceUtil;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.SqlReturnTypeInference;

import javax.annotation.Nullable;

import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTypeFactory;
import static org.apache.flink.table.types.inference.TypeInferenceUtil.createInvalidCallException;
import static org.apache.flink.table.types.inference.TypeInferenceUtil.createUnexpectedException;
import static org.apache.flink.table.types.inference.TypeInferenceUtil.inferOutputType;

/**
 * A {@link SqlReturnTypeInference} backed by {@link TypeInference}.
 *
 * <p>Note: This class must be kept in sync with {@link TypeInferenceUtil}.
 */
@Internal
public final class TypeInferenceReturnInference implements SqlReturnTypeInference {

    private final DataTypeFactory dataTypeFactory;

    private final FunctionDefinition definition;

    private final TypeInference typeInference;

    public TypeInferenceReturnInference(
            DataTypeFactory dataTypeFactory,
            FunctionDefinition definition,
            TypeInference typeInference) {
        this.dataTypeFactory = dataTypeFactory;
        this.definition = definition;
        this.typeInference = typeInference;
    }

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
        final CallContext callContext =
                new OperatorBindingCallContext(
                        dataTypeFactory,
                        definition,
                        opBinding,
                        extractExpectedOutputType(opBinding));
        try {
            return inferReturnTypeOrError(unwrapTypeFactory(opBinding), callContext);
        } catch (ValidationException e) {
            throw createInvalidCallException(callContext, e);
        } catch (Throwable t) {
            throw createUnexpectedException(callContext, t);
        }
    }

    // --------------------------------------------------------------------------------------------

    private @Nullable RelDataType extractExpectedOutputType(SqlOperatorBinding opBinding) {
        if (opBinding instanceof SqlCallBinding) {
            final SqlCallBinding binding = (SqlCallBinding) opBinding;
            final FlinkCalciteSqlValidator validator =
                    (FlinkCalciteSqlValidator) binding.getValidator();
            return validator.getExpectedOutputType(binding.getCall()).orElse(null);
        }
        return null;
    }

    private RelDataType inferReturnTypeOrError(
            FlinkTypeFactory typeFactory, CallContext callContext) {
        final LogicalType inferredType =
                inferOutputType(callContext, typeInference.getOutputTypeStrategy())
                        .getLogicalType();
        return typeFactory.createFieldTypeFromLogicalType(inferredType);
    }
}
