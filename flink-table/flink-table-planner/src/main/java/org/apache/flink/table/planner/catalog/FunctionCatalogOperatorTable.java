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

package org.apache.flink.table.planner.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ContextResolvedFunction;
import org.apache.flink.table.catalog.ContextResolvedProcedure;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.functions.AggregateFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.functions.ScalarFunctionDefinition;
import org.apache.flink.table.functions.TableFunctionDefinition;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.RexFactory;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlAggFunction;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlProcedure;
import org.apache.flink.table.planner.functions.utils.UserDefinedFunctionUtils;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.table.types.utils.TypeConversions;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.validate.SqlNameMatcher;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

/** Thin adapter between {@link SqlOperatorTable} and {@link FunctionCatalog}. */
@Internal
public class FunctionCatalogOperatorTable implements SqlOperatorTable {

    private final FunctionCatalog functionCatalog;
    private final DataTypeFactory dataTypeFactory;
    private final FlinkTypeFactory typeFactory;
    private final RexFactory rexFactory;

    public FunctionCatalogOperatorTable(
            FunctionCatalog functionCatalog,
            DataTypeFactory dataTypeFactory,
            FlinkTypeFactory typeFactory,
            RexFactory rexFactory) {
        this.functionCatalog = functionCatalog;
        this.dataTypeFactory = dataTypeFactory;
        this.typeFactory = typeFactory;
        this.rexFactory = rexFactory;
    }

    @Override
    public void lookupOperatorOverloads(
            SqlIdentifier opName,
            SqlFunctionCategory category,
            SqlSyntax syntax,
            List<SqlOperator> operatorList,
            SqlNameMatcher nameMatcher) {
        if (opName.isStar()) {
            return;
        }

        final UnresolvedIdentifier identifier = UnresolvedIdentifier.of(opName.names);

        if (category == SqlFunctionCategory.USER_DEFINED_PROCEDURE) {
            functionCatalog
                    .lookupProcedure(identifier)
                    .flatMap(this::convertToSqlProcedure)
                    .ifPresent(operatorList::add);
        } else {
            functionCatalog
                    .lookupFunction(identifier)
                    .flatMap(resolvedFunction -> convertToSqlFunction(category, resolvedFunction))
                    .ifPresent(operatorList::add);
        }
    }

    private Optional<SqlFunction> convertToSqlProcedure(
            ContextResolvedProcedure resolvedProcedure) {
        return Optional.of(BridgingSqlProcedure.of(dataTypeFactory, resolvedProcedure));
    }

    private Optional<SqlFunction> convertToSqlFunction(
            @Nullable SqlFunctionCategory category, ContextResolvedFunction resolvedFunction) {
        final FunctionDefinition definition = resolvedFunction.getDefinition();
        final FunctionIdentifier identifier = resolvedFunction.getIdentifier().orElse(null);
        // legacy
        if (definition instanceof AggregateFunctionDefinition) {
            return convertAggregateFunction(identifier, (AggregateFunctionDefinition) definition);
        } else if (definition instanceof ScalarFunctionDefinition) {
            ScalarFunctionDefinition def = (ScalarFunctionDefinition) definition;
            return convertScalarFunction(identifier, def);
        } else if (definition instanceof TableFunctionDefinition
                && category != null
                && category.isTableFunction()) {
            return convertTableFunction(identifier, (TableFunctionDefinition) definition);
        }
        // new stack
        return convertToBridgingSqlFunction(category, resolvedFunction);
    }

    private Optional<SqlFunction> convertToBridgingSqlFunction(
            @Nullable SqlFunctionCategory category, ContextResolvedFunction resolvedFunction) {
        final FunctionDefinition definition = resolvedFunction.getDefinition();

        if (!verifyFunctionKind(category, resolvedFunction)) {
            return Optional.empty();
        }

        final TypeInference typeInference;
        try {
            typeInference = definition.getTypeInference(dataTypeFactory);
        } catch (Throwable t) {
            throw new ValidationException(
                    String.format(
                            "An error occurred in the type inference logic of function '%s'.",
                            resolvedFunction),
                    t);
        }
        if (typeInference.getOutputTypeStrategy() == TypeStrategies.MISSING) {
            return Optional.empty();
        }

        final SqlFunction function;
        if (definition.getKind() == FunctionKind.AGGREGATE
                || definition.getKind() == FunctionKind.TABLE_AGGREGATE) {
            function =
                    BridgingSqlAggFunction.of(
                            dataTypeFactory,
                            typeFactory,
                            SqlKind.OTHER_FUNCTION,
                            resolvedFunction,
                            typeInference);
        } else {
            function =
                    BridgingSqlFunction.of(
                            dataTypeFactory,
                            typeFactory,
                            rexFactory,
                            SqlKind.OTHER_FUNCTION,
                            resolvedFunction,
                            typeInference);
        }
        return Optional.of(function);
    }

    /**
     * Verifies which kinds of functions are allowed to be returned from the catalog given the
     * context information.
     */
    private boolean verifyFunctionKind(
            @Nullable SqlFunctionCategory category, ContextResolvedFunction resolvedFunction) {
        final FunctionDefinition definition = resolvedFunction.getDefinition();

        // built-in functions without implementation are handled separately
        if (definition instanceof BuiltInFunctionDefinition) {
            final BuiltInFunctionDefinition builtInFunction =
                    (BuiltInFunctionDefinition) definition;
            if (!builtInFunction.hasRuntimeImplementation()) {
                return false;
            }
        }

        final FunctionKind kind = definition.getKind();

        if (kind == FunctionKind.TABLE) {
            return true;
        } else if (kind == FunctionKind.SCALAR
                || kind == FunctionKind.AGGREGATE
                || kind == FunctionKind.TABLE_AGGREGATE) {
            if (category != null && category.isTableFunction()) {
                throw new ValidationException(
                        String.format(
                                "Function '%s' cannot be used as a table function.",
                                resolvedFunction));
            }
            return true;
        }
        return false;
    }

    private Optional<SqlFunction> convertAggregateFunction(
            FunctionIdentifier identifier, AggregateFunctionDefinition functionDefinition) {
        SqlFunction aggregateFunction =
                UserDefinedFunctionUtils.createAggregateSqlFunction(
                        identifier,
                        identifier.toString(),
                        functionDefinition.getAggregateFunction(),
                        TypeConversions.fromLegacyInfoToDataType(
                                functionDefinition.getResultTypeInfo()),
                        TypeConversions.fromLegacyInfoToDataType(
                                functionDefinition.getAccumulatorTypeInfo()),
                        typeFactory);
        return Optional.of(aggregateFunction);
    }

    private Optional<SqlFunction> convertScalarFunction(
            FunctionIdentifier identifier, ScalarFunctionDefinition functionDefinition) {
        SqlFunction scalarFunction =
                UserDefinedFunctionUtils.createScalarSqlFunction(
                        identifier,
                        identifier.toString(),
                        functionDefinition.getScalarFunction(),
                        typeFactory);
        return Optional.of(scalarFunction);
    }

    private Optional<SqlFunction> convertTableFunction(
            FunctionIdentifier identifier, TableFunctionDefinition functionDefinition) {
        SqlFunction tableFunction =
                UserDefinedFunctionUtils.createTableSqlFunction(
                        identifier,
                        identifier.toString(),
                        functionDefinition.getTableFunction(),
                        TypeConversions.fromLegacyInfoToDataType(
                                functionDefinition.getResultType()),
                        typeFactory);
        return Optional.of(tableFunction);
    }

    @Override
    public List<SqlOperator> getOperatorList() {
        throw new UnsupportedOperationException("This should never be called");
    }
}
