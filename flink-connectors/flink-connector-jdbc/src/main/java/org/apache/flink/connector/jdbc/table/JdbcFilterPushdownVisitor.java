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

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Visitor to walk a Expression AST. Produces a String that can be used to pushdown the filter,
 * return Optional.empty() if we cannot pushdown the filter.
 */
public class JdbcFilterPushdownVisitor extends ExpressionDefaultVisitor<Optional<String>> {
    /**
     * This Map should not be modifed by user code. Semantically it is equivalent to a {@code
     * Function<ValueLiteralExpression, Optional<String>>}.
     *
     * <p>We expose this low-level construct instead of just a function to facilitate customization
     * of the behavior. User can make a copy of this Map, manipulate the mapping functions to
     * support different dialects.
     *
     * <pre>{@code
     * Map<Class<? extends LogicalType>, Function<ValueLiteralExpression, Optional<String>>> customMapping =
     *   new HashMap<>(JdbcFilterPushdownVisitor.defaultLiteralStringifyFunctions);
     *
     * customMapping.remove(DateType.class);
     * customMapping.remove(TimestampType.class);
     * customMapping.put(ArrayType.class, arrLiteral -> ....);
     *
     * new JdbcFilterPushdownVisitor(quoteIdentifier, customMapping);
     * }</pre>
     *
     * <p>Note that the key has to be a class of a type that extends {@link LogicalType}, which
     * represents the type of Flink SQL literal expression at runtime.
     */
    public static final Map<
                    Class<? extends LogicalType>,
                    Function<ValueLiteralExpression, Optional<String>>>
            DEFAULT_LITERAL_TO_STRING_FUNCTIONS;

    private static final Function<ValueLiteralExpression, Optional<String>> toStringRender =
            litExp -> Optional.of(litExp.toString());

    static {
        Map<Class<? extends LogicalType>, Function<ValueLiteralExpression, Optional<String>>>
                localMap = new HashMap<>();
        localMap.put(BigIntType.class, toStringRender);
        localMap.put(IntType.class, toStringRender);
        localMap.put(BooleanType.class, toStringRender);
        localMap.put(DecimalType.class, toStringRender);
        localMap.put(DoubleType.class, toStringRender);
        localMap.put(FloatType.class, toStringRender);
        localMap.put(SmallIntType.class, toStringRender);
        localMap.put(VarCharType.class, toStringRender);
        localMap.put(
                DateType.class,
                litExp -> {
                    DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd");
                    return litExp.getValueAs(LocalDate.class)
                            .map(d -> String.format("'%s'", d.format(dateFormat)));
                });
        localMap.put(
                TimestampType.class,
                litExp -> {
                    LogicalType tpe = litExp.getOutputDataType().getLogicalType();
                    int secondsPrecision = ((TimestampType) tpe).getPrecision();
                    String fractionSecPart =
                            String.join("", Collections.nCopies(secondsPrecision, "S"));
                    DateTimeFormatter dateTomeFormat =
                            secondsPrecision > 0
                                    ? DateTimeFormatter.ofPattern(
                                            "yyyy-MM-dd HH:mm:ss." + fractionSecPart)
                                    : DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

                    return litExp.getValueAs(LocalDateTime.class)
                            .map(d -> String.format("'%s'", d.format(dateTomeFormat)));
                });
        DEFAULT_LITERAL_TO_STRING_FUNCTIONS = Collections.unmodifiableMap(localMap);
    }

    Function<String, String> quoteIdentifierFunction;

    Map<Class<? extends LogicalType>, Function<ValueLiteralExpression, Optional<String>>>
            literalToStringFunctions;

    /**
     * Create a JdbcFilterPushdownVisitor.
     *
     * @param quoteIdentifierFunction this typically comes from {@link
     *     org.apache.flink.connector.jdbc.dialect.JdbcDialect#quoteIdentifier}, we use this
     *     function to wrap column name to make sure it complies to corresponding RDBMS.
     * @param literalToStringFunctions This is a mapping to convert literal expression to SQL
     *     string, if not provided, {@value #DEFAULT_LITERAL_TO_STRING_FUNCTIONS} will be used
     */
    public JdbcFilterPushdownVisitor(
            Function<String, String> quoteIdentifierFunction,
            Map<Class<? extends LogicalType>, Function<ValueLiteralExpression, Optional<String>>>
                    literalToStringFunctions) {
        this.quoteIdentifierFunction = quoteIdentifierFunction;
        this.literalToStringFunctions = literalToStringFunctions;
    }

    public JdbcFilterPushdownVisitor(Function<String, String> quoteIdentifierFunction) {
        this(
                quoteIdentifierFunction,
                JdbcFilterPushdownVisitor.DEFAULT_LITERAL_TO_STRING_FUNCTIONS);
    }

    @Override
    public Optional<String> visit(CallExpression call) {
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
        if (BuiltInFunctionDefinitions.OR.equals(call.getFunctionDefinition())) {
            return renderBinaryOperator("OR", call.getResolvedChildren());
        }
        if (BuiltInFunctionDefinitions.AND.equals(call.getFunctionDefinition())) {
            return renderBinaryOperator("AND", call.getResolvedChildren());
        }

        return Optional.empty();
    }

    private Optional<String> renderBinaryOperator(
            String operator, List<ResolvedExpression> allOperands) {
        Optional<String> leftOperandString = allOperands.get(0).accept(this);

        Supplier<Optional<String>> rightOperandString = () -> allOperands.get(1).accept(this);

        return leftOperandString.flatMap(
                left ->
                        rightOperandString
                                .get()
                                .map(right -> String.format("(%s %s %s)", left, operator, right)));
    }

    @Override
    public Optional<String> visit(ValueLiteralExpression litExp) {
        LogicalType tpe = litExp.getOutputDataType().getLogicalType();
        Class<?> typeCs = tpe.getClass();
        if (this.literalToStringFunctions.containsKey(typeCs)) {
            return this.literalToStringFunctions.get(typeCs).apply(litExp);
        } else {
            return Optional.empty();
        }
    }

    @Override
    public Optional<String> visit(FieldReferenceExpression fieldReference) {
        return Optional.of(this.quoteIdentifierFunction.apply(fieldReference.toString()));
    }

    @Override
    protected Optional<String> defaultMethod(Expression expression) {
        return Optional.empty();
    }
}
