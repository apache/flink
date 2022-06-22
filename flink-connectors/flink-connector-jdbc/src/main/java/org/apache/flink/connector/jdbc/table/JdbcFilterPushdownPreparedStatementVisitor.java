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

package org.apache.flink.connector.jdbc.table;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionDefaultVisitor;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Visitor that convert Expression to ParameterizedPredicate. Return Optional.empty() if we cannot
 * push down the filter.
 */
@Experimental
public class JdbcFilterPushdownPreparedStatementVisitor
        extends ExpressionDefaultVisitor<Optional<ParameterizedPredicate>> {

    private Function<String, String> quoteIdentifierFunction;

    private static final Set<Class<?>> SUPPORTED_DATA_TYPES;

    static {
        SUPPORTED_DATA_TYPES = new HashSet<>();
        SUPPORTED_DATA_TYPES.add(IntType.class);
        SUPPORTED_DATA_TYPES.add(BigIntType.class);
        SUPPORTED_DATA_TYPES.add(BooleanType.class);
        SUPPORTED_DATA_TYPES.add(DecimalType.class);
        SUPPORTED_DATA_TYPES.add(DoubleType.class);
        SUPPORTED_DATA_TYPES.add(FloatType.class);
        SUPPORTED_DATA_TYPES.add(SmallIntType.class);
        SUPPORTED_DATA_TYPES.add(VarCharType.class);
        SUPPORTED_DATA_TYPES.add(TimestampType.class);
    }

    public JdbcFilterPushdownPreparedStatementVisitor(
            Function<String, String> quoteIdentifierFunction) {
        this.quoteIdentifierFunction = quoteIdentifierFunction;
    }

    @Override
    public Optional<ParameterizedPredicate> visit(CallExpression call) {
        if (BuiltInFunctionDefinitions.EQUALS.equals(call.getFunctionDefinition())) {
            return renderBinaryOperator("=", call.getResolvedChildren());
        }
        if (BuiltInFunctionDefinitions.LESS_THAN.equals(call.getFunctionDefinition())) {
            return renderBinaryOperator("<", call.getResolvedChildren());
        }
        if (BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL.equals(call.getFunctionDefinition())) {
            return renderBinaryOperator("<=", call.getResolvedChildren());
        }
        if (BuiltInFunctionDefinitions.GREATER_THAN.equals(call.getFunctionDefinition())) {
            return renderBinaryOperator(">", call.getResolvedChildren());
        }
        if (BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL.equals(call.getFunctionDefinition())) {
            return renderBinaryOperator(">=", call.getResolvedChildren());
        }
        if (BuiltInFunctionDefinitions.NOT_EQUALS.equals(call.getFunctionDefinition())) {
            return renderBinaryOperator("<>", call.getResolvedChildren());
        }
        if (BuiltInFunctionDefinitions.OR.equals(call.getFunctionDefinition())) {
            return renderBinaryOperator("OR", call.getResolvedChildren());
        }
        if (BuiltInFunctionDefinitions.AND.equals(call.getFunctionDefinition())) {
            return renderBinaryOperator("AND", call.getResolvedChildren());
        }

        return Optional.empty();
    }

    private Optional<ParameterizedPredicate> renderBinaryOperator(
            String operator, List<ResolvedExpression> allOperands) {
        Optional<ParameterizedPredicate> leftOperandString = allOperands.get(0).accept(this);

        Supplier<Optional<ParameterizedPredicate>> rightOperandString =
                () -> allOperands.get(1).accept(this);

        return leftOperandString.flatMap(
                left -> rightOperandString.get().map(right -> left.combine(operator, right)));
    }

    @Override
    public Optional<ParameterizedPredicate> visit(ValueLiteralExpression litExp) {
        LogicalType tpe = litExp.getOutputDataType().getLogicalType();
        Class<?> typeCs = tpe.getClass();

        if (SUPPORTED_DATA_TYPES.contains(typeCs)) {
            ParameterizedPredicate predicate = new ParameterizedPredicate("?");

            Serializable[] params = new Serializable[1];

            if (typeCs.equals(VarCharType.class)) {
                params[0] = litExp.getValueAs(String.class).orElse(null);
            }
            if (typeCs.equals(BigIntType.class)) {
                params[0] = litExp.getValueAs(Long.class).orElse(null);
            }
            if (typeCs.equals(IntType.class) || typeCs.equals(SmallIntType.class)) {
                params[0] = litExp.getValueAs(Integer.class).orElse(null);
            }
            if (typeCs.equals(DoubleType.class)) {
                params[0] = litExp.getValueAs(Double.class).orElse(null);
            }
            if (typeCs.equals(BooleanType.class)) {
                params[0] = litExp.getValueAs(Boolean.class).orElse(null);
            }
            if (typeCs.equals(FloatType.class)) {
                params[0] = litExp.getValueAs(Float.class).orElse(null);
            }
            if (typeCs.equals(DecimalType.class)) {
                params[0] = litExp.getValueAs(BigDecimal.class).orElse(null);
            }
            if (typeCs.equals(DateType.class)) {
                params[0] = litExp.getValueAs(LocalDate.class).map(Date::valueOf).orElse(null);
            }
            if (typeCs.equals(TimestampType.class)) {
                params[0] =
                        litExp.getValueAs(LocalDateTime.class).map(Timestamp::valueOf).orElse(null);
            }
            predicate.setParameters(params);
            return Optional.of(predicate);
        } else {
            return Optional.empty();
        }
    }

    @Override
    public Optional<ParameterizedPredicate> visit(FieldReferenceExpression fieldReference) {
        String predicateStr = (this.quoteIdentifierFunction.apply(fieldReference.toString()));
        ParameterizedPredicate predicate = new ParameterizedPredicate(predicateStr);
        return Optional.of(predicate);
    }

    @Override
    protected Optional<ParameterizedPredicate> defaultMethod(Expression expression) {
        return Optional.empty();
    }
}
