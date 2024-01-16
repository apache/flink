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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.ContextResolvedFunction;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.functions.FunctionRequirement;
import org.apache.flink.table.planner.calcite.FlinkContext;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeInference;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.Optionality;

import java.util.List;

import static org.apache.flink.table.planner.functions.bridging.BridgingUtils.createName;
import static org.apache.flink.table.planner.functions.bridging.BridgingUtils.createParamTypes;
import static org.apache.flink.table.planner.functions.bridging.BridgingUtils.createSqlFunctionCategory;
import static org.apache.flink.table.planner.functions.bridging.BridgingUtils.createSqlIdentifier;
import static org.apache.flink.table.planner.functions.bridging.BridgingUtils.createSqlOperandTypeChecker;
import static org.apache.flink.table.planner.functions.bridging.BridgingUtils.createSqlOperandTypeInference;
import static org.apache.flink.table.planner.functions.bridging.BridgingUtils.createSqlReturnTypeInference;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Bridges {@link FunctionDefinition} to Calcite's representation of an aggregating function (either
 * a system or user-defined function).
 */
@Internal
public final class BridgingSqlAggFunction extends SqlAggFunction {

    private final DataTypeFactory dataTypeFactory;

    private final FlinkTypeFactory typeFactory;

    private final ContextResolvedFunction resolvedFunction;

    private final TypeInference typeInference;

    private final List<RelDataType> paramTypes;

    private BridgingSqlAggFunction(
            DataTypeFactory dataTypeFactory,
            FlinkTypeFactory typeFactory,
            SqlKind kind,
            ContextResolvedFunction resolvedFunction,
            TypeInference typeInference) {
        super(
                createName(resolvedFunction),
                createSqlIdentifier(resolvedFunction),
                kind,
                createSqlReturnTypeInference(
                        dataTypeFactory, resolvedFunction.getDefinition(), typeInference),
                createSqlOperandTypeInference(
                        dataTypeFactory, resolvedFunction.getDefinition(), typeInference),
                createSqlOperandTypeChecker(
                        dataTypeFactory, resolvedFunction.getDefinition(), typeInference),
                createSqlFunctionCategory(resolvedFunction),
                createOrderRequirement(),
                createOverWindowRequirement(resolvedFunction.getDefinition()),
                createGroupOrderRequirement());

        this.dataTypeFactory = dataTypeFactory;
        this.typeFactory = typeFactory;
        this.resolvedFunction = resolvedFunction;
        this.typeInference = typeInference;
        this.paramTypes = createParamTypes(typeFactory, typeInference);
    }

    /**
     * Creates an instance of a aggregating function (either a system or user-defined function).
     *
     * @param dataTypeFactory used for creating {@link DataType}
     * @param typeFactory used for bridging to {@link RelDataType}
     * @param kind commonly used SQL standard function; use {@link SqlKind#OTHER_FUNCTION} if this
     *     function cannot be mapped to a common function kind.
     * @param resolvedFunction system or user-defined {@link FunctionDefinition} with context
     * @param typeInference type inference logic
     */
    public static BridgingSqlAggFunction of(
            DataTypeFactory dataTypeFactory,
            FlinkTypeFactory typeFactory,
            SqlKind kind,
            ContextResolvedFunction resolvedFunction,
            TypeInference typeInference) {
        final FunctionKind functionKind = resolvedFunction.getDefinition().getKind();
        checkState(
                functionKind == FunctionKind.AGGREGATE
                        || functionKind == FunctionKind.TABLE_AGGREGATE,
                "Aggregating function kind expected.");

        return new BridgingSqlAggFunction(
                dataTypeFactory, typeFactory, kind, resolvedFunction, typeInference);
    }

    /** Creates an instance of a aggregate function during translation. */
    public static BridgingSqlAggFunction of(
            FlinkContext context,
            FlinkTypeFactory typeFactory,
            ContextResolvedFunction resolvedFunction) {
        final DataTypeFactory dataTypeFactory = context.getCatalogManager().getDataTypeFactory();
        final TypeInference typeInference =
                resolvedFunction.getDefinition().getTypeInference(dataTypeFactory);
        return of(
                dataTypeFactory,
                typeFactory,
                SqlKind.OTHER_FUNCTION,
                resolvedFunction,
                typeInference);
    }

    public DataTypeFactory getDataTypeFactory() {
        return dataTypeFactory;
    }

    public FlinkTypeFactory getTypeFactory() {
        return typeFactory;
    }

    public ContextResolvedFunction getResolvedFunction() {
        return resolvedFunction;
    }

    public FunctionDefinition getDefinition() {
        return resolvedFunction.getDefinition();
    }

    public TypeInference getTypeInference() {
        return typeInference;
    }

    @Override
    public List<RelDataType> getParamTypes() {
        return paramTypes;
    }

    @Override
    public List<String> getParamNames() {
        if (typeInference.getNamedArguments().isPresent()) {
            return typeInference.getNamedArguments().get();
        }
        return super.getParamNames();
    }

    @Override
    public boolean isDeterministic() {
        return resolvedFunction.getDefinition().isDeterministic();
    }

    // --------------------------------------------------------------------------------------------

    private static boolean createOrderRequirement() {
        return false;
    }

    private static boolean createOverWindowRequirement(FunctionDefinition definition) {
        return definition.getRequirements().contains(FunctionRequirement.OVER_WINDOW_ONLY);
    }

    private static Optionality createGroupOrderRequirement() {
        return Optionality.FORBIDDEN;
    }
}
