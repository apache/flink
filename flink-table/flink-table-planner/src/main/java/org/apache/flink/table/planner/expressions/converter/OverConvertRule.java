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

package org.apache.flink.table.planner.expressions.converter;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.expressions.SqlAggFunctionVisitor;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlPostfixOperator;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OrdinalReturnTypeInference;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.table.planner.expressions.converter.ExpressionConverter.extractValue;
import static org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromDataTypeToLogicalType;

/** A {@link CallExpressionConvertRule} that converts {@link BuiltInFunctionDefinitions#OVER}. */
public class OverConvertRule implements CallExpressionConvertRule {

    @Override
    public Optional<RexNode> convert(CallExpression call, ConvertContext context) {
        List<Expression> children = call.getChildren();
        if (call.getFunctionDefinition() == BuiltInFunctionDefinitions.OVER) {
            FlinkTypeFactory typeFactory = context.getTypeFactory();
            Expression agg = children.get(0);
            FunctionDefinition def = ((CallExpression) agg).getFunctionDefinition();
            boolean isDistinct = BuiltInFunctionDefinitions.DISTINCT == def;

            SqlAggFunction aggFunc = agg.accept(new SqlAggFunctionVisitor(context.getRelBuilder()));
            RelDataType aggResultType =
                    typeFactory.createFieldTypeFromLogicalType(
                            fromDataTypeToLogicalType(
                                    ((ResolvedExpression) agg).getOutputDataType()));

            // assemble exprs by agg children
            List<RexNode> aggExprs =
                    agg.getChildren().stream()
                            .map(
                                    child -> {
                                        if (isDistinct) {
                                            return context.toRexNode(child.getChildren().get(0));
                                        } else {
                                            return context.toRexNode(child);
                                        }
                                    })
                            .collect(Collectors.toList());

            // assemble order by key
            Expression orderKeyExpr = children.get(1);
            Set<SqlKind> kinds = new HashSet<>();
            RexNode collationRexNode =
                    createCollation(
                            context.toRexNode(orderKeyExpr),
                            RelFieldCollation.Direction.ASCENDING,
                            null,
                            kinds);
            ImmutableList<RexFieldCollation> orderKey =
                    ImmutableList.of(new RexFieldCollation(collationRexNode, kinds));

            // assemble partition by keys
            List<RexNode> partitionKeys =
                    children.subList(4, children.size()).stream()
                            .map(context::toRexNode)
                            .collect(Collectors.toList());
            // assemble bounds
            Expression preceding = children.get(2);
            boolean isPhysical =
                    fromDataTypeToLogicalType(((ResolvedExpression) preceding).getOutputDataType())
                            .is(LogicalTypeRoot.BIGINT);
            Expression following = children.get(3);
            RexWindowBound lowerBound = createBound(context, preceding, SqlKind.PRECEDING);
            RexWindowBound upperBound = createBound(context, following, SqlKind.FOLLOWING);

            // build RexOver
            return Optional.of(
                    context.getRelBuilder()
                            .getRexBuilder()
                            .makeOver(
                                    aggResultType,
                                    aggFunc,
                                    aggExprs,
                                    partitionKeys,
                                    orderKey,
                                    lowerBound,
                                    upperBound,
                                    isPhysical,
                                    true,
                                    false,
                                    isDistinct,
                                    false));
        }
        return Optional.empty();
    }

    private RexNode createCollation(
            RexNode node,
            RelFieldCollation.Direction direction,
            RelFieldCollation.NullDirection nullDirection,
            Set<SqlKind> kinds) {
        switch (node.getKind()) {
            case DESCENDING:
                kinds.add(node.getKind());
                return createCollation(
                        ((RexCall) node).getOperands().get(0),
                        RelFieldCollation.Direction.DESCENDING,
                        nullDirection,
                        kinds);
            case NULLS_FIRST:
                kinds.add(node.getKind());
                return createCollation(
                        ((RexCall) node).getOperands().get(0),
                        direction,
                        RelFieldCollation.NullDirection.FIRST,
                        kinds);
            case NULLS_LAST:
                kinds.add(node.getKind());
                return createCollation(
                        ((RexCall) node).getOperands().get(0),
                        direction,
                        RelFieldCollation.NullDirection.LAST,
                        kinds);
            default:
                if (nullDirection == null) {
                    // Set the null direction if not specified.
                    // Consistent with HIVE/SPARK/MYSQL
                    if (FlinkPlannerImpl.defaultNullCollation()
                            .last(direction.equals(RelFieldCollation.Direction.DESCENDING))) {
                        kinds.add(SqlKind.NULLS_LAST);
                    } else {
                        kinds.add(SqlKind.NULLS_FIRST);
                    }
                }
                return node;
        }
    }

    private RexWindowBound createBound(ConvertContext context, Expression bound, SqlKind sqlKind) {
        if (bound instanceof CallExpression) {
            CallExpression callExpr = (CallExpression) bound;
            FunctionDefinition func = callExpr.getFunctionDefinition();
            if (BuiltInFunctionDefinitions.UNBOUNDED_ROW.equals(func)
                    || BuiltInFunctionDefinitions.UNBOUNDED_RANGE.equals(func)) {
                SqlNode unbounded =
                        sqlKind.equals(SqlKind.PRECEDING)
                                ? SqlWindow.createUnboundedPreceding(SqlParserPos.ZERO)
                                : SqlWindow.createUnboundedFollowing(SqlParserPos.ZERO);
                return RexWindowBounds.create(unbounded, null);
            } else if (BuiltInFunctionDefinitions.CURRENT_ROW.equals(func)
                    || BuiltInFunctionDefinitions.CURRENT_RANGE.equals(func)) {
                SqlNode currentRow = SqlWindow.createCurrentRow(SqlParserPos.ZERO);
                return RexWindowBounds.create(currentRow, null);
            } else {
                throw new IllegalArgumentException("Unexpected expression: " + bound);
            }
        } else if (bound instanceof ValueLiteralExpression) {
            RelDataType returnType =
                    context.getTypeFactory()
                            .createFieldTypeFromLogicalType(new DecimalType(true, 19, 0));
            SqlOperator sqlOperator =
                    new SqlPostfixOperator(
                            sqlKind.name(),
                            sqlKind,
                            2,
                            new OrdinalReturnTypeInference(0),
                            null,
                            null);
            SqlNode[] operands =
                    new SqlNode[] {SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO)};
            SqlNode node = new SqlBasicCall(sqlOperator, operands, SqlParserPos.ZERO);

            ValueLiteralExpression literalExpr = (ValueLiteralExpression) bound;
            RexNode literalRexNode =
                    literalExpr
                            .getValueAs(BigDecimal.class)
                            .map(v -> context.getRelBuilder().literal(v))
                            .orElse(
                                    context.getRelBuilder()
                                            .literal(extractValue(literalExpr, Object.class)));

            List<RexNode> expressions = new ArrayList<>();
            expressions.add(literalRexNode);
            RexNode rexNode =
                    context.getRelBuilder()
                            .getRexBuilder()
                            .makeCall(returnType, sqlOperator, expressions);
            return RexWindowBounds.create(node, rexNode);
        } else {
            throw new TableException("Unexpected expression: " + bound);
        }
    }
}
