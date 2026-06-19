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

import org.apache.flink.table.expressions.ApiExpressionUtils;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionDefaultVisitor;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.UnresolvedCallExpression;
import org.apache.flink.table.expressions.UnresolvedReferenceExpression;
import org.apache.flink.table.functions.DeclarativeAggregateFunction;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.RexDistinctKeyVariable;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.tools.RelBuilder;
import org.apache.commons.lang3.ArrayUtils;

import java.util.stream.Collectors;

import static org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromLogicalTypeToDataType;

/** Abstract class to resolve the expressions in {@link DeclarativeAggregateFunction}. */
public abstract class DeclarativeExpressionResolver
        extends ExpressionDefaultVisitor<ResolvedExpression> {

    private final DeclarativeAggregateFunction function;
    private final boolean isMerge;
    private final CallExpressionResolver resolver;

    public DeclarativeExpressionResolver(
            RelBuilder relBuilder, DeclarativeAggregateFunction function, boolean isMerge) {
        this.function = function;
        this.isMerge = isMerge;
        this.resolver = new CallExpressionResolver(relBuilder);
    }

    @Override
    protected ResolvedExpression defaultMethod(Expression expression) {
        if (expression instanceof UnresolvedReferenceExpression) {
            UnresolvedReferenceExpression expr = (UnresolvedReferenceExpression) expression;
            String name = expr.getName();
            int localIndex = ArrayUtils.indexOf(function.aggBufferAttributes(), expr);
            if (localIndex == -1) {
                // We always use UnresolvedFieldReference to represent reference of input field.
                // In non-merge case, the input is operand of the aggregate function. But in merge
                // case, the input is aggregate buffers which sent by local aggregate.
                if (isMerge) {
                    return toMergeInputExpr(
                            name, ArrayUtils.indexOf(function.mergeOperands(), expr));
                } else {
                    return toAccInputExpr(name, ArrayUtils.indexOf(function.operands(), expr));
                }
            } else {
                return toAggBufferExpr(name, localIndex);
            }
        } else if (expression instanceof UnresolvedCallExpression) {
            UnresolvedCallExpression unresolvedCall = (UnresolvedCallExpression) expression;
            return resolver.resolve(
                    ApiExpressionUtils.unresolvedCall(
                            unresolvedCall.getFunctionDefinition(),
                            unresolvedCall.getChildren().stream()
                                    .map(c -> c.accept(DeclarativeExpressionResolver.this))
                                    .collect(Collectors.toList())));
        } else if (expression instanceof ResolvedExpression) {
            return (ResolvedExpression) expression;
        } else {
            return resolver.resolve(expression);
        }
    }

    /** When merge phase, for inputs. */
    public abstract ResolvedExpression toMergeInputExpr(String name, int localIndex);

    /** When accumulate phase, for inputs. */
    public abstract ResolvedExpression toAccInputExpr(String name, int localIndex);

    /** For aggregate buffer. */
    public abstract ResolvedExpression toAggBufferExpr(String name, int localIndex);

    public static ResolvedExpression toRexInputRef(RelBuilder builder, int i, LogicalType t) {
        RelDataType tp =
                ((FlinkTypeFactory) builder.getTypeFactory()).createFieldTypeFromLogicalType(t);
        return new RexNodeExpression(
                new RexInputRef(i, tp), fromLogicalTypeToDataType(t), null, null);
    }

    public static ResolvedExpression toRexDistinctKey(
            RelBuilder builder, String name, LogicalType t) {
        return new RexNodeExpression(
                new RexDistinctKeyVariable(
                        name,
                        ((FlinkTypeFactory) builder.getTypeFactory())
                                .createFieldTypeFromLogicalType(t),
                        t),
                fromLogicalTypeToDataType(t),
                null,
                null);
    }
}
