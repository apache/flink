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
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.planner.calcite.FlinkContext;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeInference;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static org.apache.flink.table.planner.functions.bridging.BridgingUtils.createName;
import static org.apache.flink.table.planner.functions.bridging.BridgingUtils.createParamTypes;
import static org.apache.flink.table.planner.functions.bridging.BridgingUtils.createSqlFunctionCategory;
import static org.apache.flink.table.planner.functions.bridging.BridgingUtils.createSqlIdentifier;
import static org.apache.flink.table.planner.functions.bridging.BridgingUtils.createSqlOperandTypeChecker;
import static org.apache.flink.table.planner.functions.bridging.BridgingUtils.createSqlOperandTypeInference;
import static org.apache.flink.table.planner.functions.bridging.BridgingUtils.createSqlReturnTypeInference;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Bridges {@link FunctionDefinition} to Calcite's representation of a scalar or table function
 * (either a system or user-defined function).
 */
@Internal
public final class BridgingSqlFunction extends SqlFunction {

    private final DataTypeFactory dataTypeFactory;

    private final FlinkTypeFactory typeFactory;

    private final @Nullable FunctionIdentifier identifier;

    private final FunctionDefinition definition;

    private final TypeInference typeInference;

    private BridgingSqlFunction(
            DataTypeFactory dataTypeFactory,
            FlinkTypeFactory typeFactory,
            SqlKind kind,
            @Nullable FunctionIdentifier identifier,
            FunctionDefinition definition,
            TypeInference typeInference) {
        super(
                createName(identifier, definition),
                createSqlIdentifier(identifier),
                kind,
                createSqlReturnTypeInference(dataTypeFactory, definition, typeInference),
                createSqlOperandTypeInference(dataTypeFactory, definition, typeInference),
                createSqlOperandTypeChecker(dataTypeFactory, definition, typeInference),
                createParamTypes(typeFactory, typeInference),
                createSqlFunctionCategory(identifier));

        this.dataTypeFactory = dataTypeFactory;
        this.typeFactory = typeFactory;
        this.identifier = identifier;
        this.definition = definition;
        this.typeInference = typeInference;
    }

    /**
     * Creates an instance of a scalar or table function (either a system or user-defined function).
     *
     * @param dataTypeFactory used for creating {@link DataType}
     * @param typeFactory used for bridging to {@link RelDataType}
     * @param kind commonly used SQL standard function; use {@link SqlKind#OTHER_FUNCTION} if this
     *     function cannot be mapped to a common function kind.
     * @param identifier catalog identifier
     * @param definition system or user-defined {@link FunctionDefinition}
     * @param typeInference type inference logic
     */
    public static BridgingSqlFunction of(
            DataTypeFactory dataTypeFactory,
            FlinkTypeFactory typeFactory,
            SqlKind kind,
            @Nullable FunctionIdentifier identifier,
            FunctionDefinition definition,
            TypeInference typeInference) {

        checkState(
                definition.getKind() == FunctionKind.SCALAR
                        || definition.getKind() == FunctionKind.TABLE,
                "Scalar or table function kind expected.");

        return new BridgingSqlFunction(
                dataTypeFactory, typeFactory, kind, identifier, definition, typeInference);
    }

    /** Creates an instance of a scalar or table function during translation. */
    public static BridgingSqlFunction of(
            FlinkContext context,
            FlinkTypeFactory typeFactory,
            @Nullable FunctionIdentifier identifier,
            FunctionDefinition definition) {
        final DataTypeFactory dataTypeFactory = context.getCatalogManager().getDataTypeFactory();
        final TypeInference typeInference = definition.getTypeInference(dataTypeFactory);
        return of(
                dataTypeFactory,
                typeFactory,
                SqlKind.OTHER_FUNCTION,
                identifier,
                definition,
                typeInference);
    }

    /** Creates an instance of a scalar or table function during translation. */
    public static BridgingSqlFunction of(
            RelOptCluster cluster,
            @Nullable FunctionIdentifier identifier,
            FunctionDefinition definition) {
        final FlinkContext context = ShortcutUtils.unwrapContext(cluster);
        final FlinkTypeFactory typeFactory = ShortcutUtils.unwrapTypeFactory(cluster);
        return of(context, typeFactory, identifier, definition);
    }

    public DataTypeFactory getDataTypeFactory() {
        return dataTypeFactory;
    }

    public FlinkTypeFactory getTypeFactory() {
        return typeFactory;
    }

    public Optional<FunctionIdentifier> getIdentifier() {
        return Optional.ofNullable(identifier);
    }

    public FunctionDefinition getDefinition() {
        return definition;
    }

    public TypeInference getTypeInference() {
        return typeInference;
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
        return definition.isDeterministic();
    }
}
