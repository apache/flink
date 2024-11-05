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

package org.apache.flink.table.planner.expressions;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.ContextResolvedFunction;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionDefaultVisitor;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.AggregateFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.functions.FunctionRequirement;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.table.functions.TableAggregateFunctionDefinition;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlAggFunction;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.planner.functions.utils.AggSqlFunction;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.types.inference.TypeInference;

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;

import javax.annotation.Nullable;

import java.util.IdentityHashMap;
import java.util.Map;

import static org.apache.flink.table.expressions.ApiExpressionUtils.isFunctionOfKind;
import static org.apache.flink.table.functions.FunctionKind.AGGREGATE;
import static org.apache.flink.table.functions.FunctionKind.TABLE_AGGREGATE;
import static org.apache.flink.table.functions.UserDefinedFunctionHelper.generateInlineFunctionName;
import static org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType;

/** The class to get {@link SqlAggFunctionVisitor} of CallExpression. */
public class SqlAggFunctionVisitor extends ExpressionDefaultVisitor<SqlAggFunction> {

    private static final Map<FunctionDefinition, SqlAggFunction> AGG_DEF_SQL_OPERATOR_MAPPING =
            new IdentityHashMap<>();

    static {
        AGG_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.AVG, FlinkSqlOperatorTable.AVG);
        AGG_DEF_SQL_OPERATOR_MAPPING.put(
                BuiltInFunctionDefinitions.COUNT, FlinkSqlOperatorTable.COUNT);
        AGG_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.MAX, FlinkSqlOperatorTable.MAX);
        AGG_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.MIN, FlinkSqlOperatorTable.MIN);
        AGG_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.SUM, FlinkSqlOperatorTable.SUM);
        AGG_DEF_SQL_OPERATOR_MAPPING.put(
                BuiltInFunctionDefinitions.FIRST_VALUE, FlinkSqlOperatorTable.FIRST_VALUE);
        AGG_DEF_SQL_OPERATOR_MAPPING.put(
                BuiltInFunctionDefinitions.LAST_VALUE, FlinkSqlOperatorTable.LAST_VALUE);
        AGG_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.LAG, FlinkSqlOperatorTable.LAG);
        AGG_DEF_SQL_OPERATOR_MAPPING.put(
                BuiltInFunctionDefinitions.LEAD, FlinkSqlOperatorTable.LEAD);
        AGG_DEF_SQL_OPERATOR_MAPPING.put(
                BuiltInFunctionDefinitions.LISTAGG, FlinkSqlOperatorTable.LISTAGG);
        AGG_DEF_SQL_OPERATOR_MAPPING.put(
                BuiltInFunctionDefinitions.SUM0, FlinkSqlOperatorTable.SUM0);
        AGG_DEF_SQL_OPERATOR_MAPPING.put(
                BuiltInFunctionDefinitions.STDDEV_POP, FlinkSqlOperatorTable.STDDEV_POP);
        AGG_DEF_SQL_OPERATOR_MAPPING.put(
                BuiltInFunctionDefinitions.STDDEV_SAMP, FlinkSqlOperatorTable.STDDEV_SAMP);
        AGG_DEF_SQL_OPERATOR_MAPPING.put(
                BuiltInFunctionDefinitions.VAR_POP, FlinkSqlOperatorTable.VAR_POP);
        AGG_DEF_SQL_OPERATOR_MAPPING.put(
                BuiltInFunctionDefinitions.VAR_SAMP, FlinkSqlOperatorTable.VAR_SAMP);
        AGG_DEF_SQL_OPERATOR_MAPPING.put(
                BuiltInFunctionDefinitions.COLLECT, FlinkSqlOperatorTable.COLLECT);
        AGG_DEF_SQL_OPERATOR_MAPPING.put(
                BuiltInFunctionDefinitions.ARRAY_AGG, FlinkSqlOperatorTable.ARRAY_AGG);
        AGG_DEF_SQL_OPERATOR_MAPPING.put(
                BuiltInFunctionDefinitions.JSON_OBJECTAGG_NULL_ON_NULL,
                FlinkSqlOperatorTable.JSON_OBJECTAGG_NULL_ON_NULL);
        AGG_DEF_SQL_OPERATOR_MAPPING.put(
                BuiltInFunctionDefinitions.JSON_OBJECTAGG_ABSENT_ON_NULL,
                FlinkSqlOperatorTable.JSON_OBJECTAGG_ABSENT_ON_NULL);
        AGG_DEF_SQL_OPERATOR_MAPPING.put(
                BuiltInFunctionDefinitions.JSON_ARRAYAGG_NULL_ON_NULL,
                FlinkSqlOperatorTable.JSON_ARRAYAGG_NULL_ON_NULL);
        AGG_DEF_SQL_OPERATOR_MAPPING.put(
                BuiltInFunctionDefinitions.JSON_ARRAYAGG_ABSENT_ON_NULL,
                FlinkSqlOperatorTable.JSON_ARRAYAGG_ABSENT_ON_NULL);
    }

    private final RelBuilder relBuilder;

    public SqlAggFunctionVisitor(RelBuilder relBuilder) {
        this.relBuilder = relBuilder;
    }

    @Override
    public SqlAggFunction visit(CallExpression call) {
        if (!isFunctionOfKind(call, AGGREGATE) && !isFunctionOfKind(call, TABLE_AGGREGATE)) {
            defaultMethod(call);
        }

        final FunctionDefinition definition = call.getFunctionDefinition();
        if (AGG_DEF_SQL_OPERATOR_MAPPING.containsKey(definition)) {
            return AGG_DEF_SQL_OPERATOR_MAPPING.get(definition);
        }
        if (BuiltInFunctionDefinitions.DISTINCT == definition) {
            Expression innerAgg = call.getChildren().get(0);
            return innerAgg.accept(this);
        }

        return createSqlAggFunction(call);
    }

    private SqlAggFunction createSqlAggFunction(CallExpression call) {
        final FunctionDefinition definition = call.getFunctionDefinition();
        // legacy
        if (definition instanceof AggregateFunctionDefinition) {
            return createLegacySqlAggregateFunction(
                    call.getFunctionIdentifier().orElse(null),
                    (AggregateFunctionDefinition) definition);
        } else if (definition instanceof TableAggregateFunctionDefinition) {
            return createLegacySqlTableAggregateFunction(
                    call.getFunctionIdentifier().orElse(null),
                    (TableAggregateFunctionDefinition) definition);
        }

        // new stack
        final DataTypeFactory dataTypeFactory =
                ShortcutUtils.unwrapContext(relBuilder).getCatalogManager().getDataTypeFactory();
        final TypeInference typeInference = definition.getTypeInference(dataTypeFactory);
        return BridgingSqlAggFunction.of(
                dataTypeFactory,
                ShortcutUtils.unwrapTypeFactory(relBuilder),
                SqlKind.OTHER_FUNCTION,
                ContextResolvedFunction.fromCallExpression(call),
                typeInference);
    }

    private SqlAggFunction createLegacySqlAggregateFunction(
            @Nullable FunctionIdentifier identifier, AggregateFunctionDefinition definition) {
        final AggregateFunction<?, ?> aggFunc = definition.getAggregateFunction();
        final FunctionIdentifier adjustedIdentifier;
        if (identifier != null) {
            adjustedIdentifier = identifier;
        } else {
            adjustedIdentifier = FunctionIdentifier.of(generateInlineFunctionName(aggFunc));
        }
        return new AggSqlFunction(
                adjustedIdentifier,
                aggFunc.toString(),
                aggFunc,
                fromLegacyInfoToDataType(definition.getResultTypeInfo()),
                fromLegacyInfoToDataType(definition.getAccumulatorTypeInfo()),
                ShortcutUtils.unwrapTypeFactory(relBuilder),
                aggFunc.getRequirements().contains(FunctionRequirement.OVER_WINDOW_ONLY),
                scala.Option.empty());
    }

    private SqlAggFunction createLegacySqlTableAggregateFunction(
            @Nullable FunctionIdentifier identifier, TableAggregateFunctionDefinition definition) {
        final TableAggregateFunction<?, ?> aggFunc = definition.getTableAggregateFunction();
        final FunctionIdentifier adjustedIdentifier;
        if (identifier != null) {
            adjustedIdentifier = identifier;
        } else {
            adjustedIdentifier = FunctionIdentifier.of(generateInlineFunctionName(aggFunc));
        }
        return new AggSqlFunction(
                adjustedIdentifier,
                aggFunc.toString(),
                aggFunc,
                fromLegacyInfoToDataType(definition.getResultTypeInfo()),
                fromLegacyInfoToDataType(definition.getAccumulatorTypeInfo()),
                ShortcutUtils.unwrapTypeFactory(relBuilder),
                false,
                scala.Option.empty());
    }

    @Override
    protected SqlAggFunction defaultMethod(Expression expression) {
        throw new TableException("Unexpected expression: " + expression);
    }
}
