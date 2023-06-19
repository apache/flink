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

package org.apache.flink.table.planner.functions.bridging;

import org.apache.flink.table.catalog.ContextResolvedProcedure;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.procedures.Procedure;
import org.apache.flink.table.procedures.ProcedureDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.extraction.TypeInferenceExtractor;
import org.apache.flink.table.types.inference.TypeInference;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.checkerframework.checker.nullness.qual.Nullable;

import static org.apache.flink.table.planner.functions.bridging.BridgingUtils.createName;
import static org.apache.flink.table.planner.functions.bridging.BridgingUtils.createSqlIdentifier;
import static org.apache.flink.table.planner.functions.bridging.BridgingUtils.createSqlOperandTypeChecker;
import static org.apache.flink.table.planner.functions.bridging.BridgingUtils.createSqlOperandTypeInference;
import static org.apache.flink.table.planner.functions.bridging.BridgingUtils.createSqlReturnTypeInference;

/** Bridges {@link Procedure} to Calcite's representation of a function. */
public class BridgingSqlProcedure extends SqlFunction {

    private final ContextResolvedProcedure contextResolvedProcedure;

    private BridgingSqlProcedure(
            String name,
            SqlIdentifier sqlIdentifier,
            @Nullable SqlReturnTypeInference returnTypeInference,
            @Nullable SqlOperandTypeInference operandTypeInference,
            @Nullable SqlOperandTypeChecker operandTypeChecker,
            SqlFunctionCategory category,
            ContextResolvedProcedure contextResolvedProcedure) {
        super(
                name,
                sqlIdentifier,
                SqlKind.OTHER_FUNCTION,
                returnTypeInference,
                operandTypeInference,
                operandTypeChecker,
                category);
        this.contextResolvedProcedure = contextResolvedProcedure;
    }

    public ContextResolvedProcedure getContextResolveProcedure() {
        return contextResolvedProcedure;
    }

    /**
     * Creates an instance of a procedure.
     *
     * @param dataTypeFactory used for creating {@link DataType}
     * @param resolvedProcedure {@link Procedure} with context
     */
    public static BridgingSqlProcedure of(
            DataTypeFactory dataTypeFactory, ContextResolvedProcedure resolvedProcedure) {
        final Procedure procedure = resolvedProcedure.getProcedure();
        final ProcedureDefinition procedureDefinition = new ProcedureDefinition(procedure);
        final TypeInference typeInference =
                TypeInferenceExtractor.forProcedure(dataTypeFactory, procedure.getClass());
        return new BridgingSqlProcedure(
                createName(resolvedProcedure),
                createSqlIdentifier(resolvedProcedure),
                createSqlReturnTypeInference(dataTypeFactory, procedureDefinition, typeInference),
                createSqlOperandTypeInference(dataTypeFactory, procedureDefinition, typeInference),
                createSqlOperandTypeChecker(dataTypeFactory, procedureDefinition, typeInference),
                SqlFunctionCategory.USER_DEFINED_PROCEDURE,
                resolvedProcedure);
    }
}
