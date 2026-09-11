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

package org.apache.flink.table.operations.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.LocalReferenceExpression;
import org.apache.flink.table.expressions.LookupCallExpression;
import org.apache.flink.table.expressions.ModelReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.TableReferenceExpression;
import org.apache.flink.table.expressions.UnresolvedCallExpression;
import org.apache.flink.table.expressions.utils.ApiExpressionDefaultVisitor;
import org.apache.flink.table.expressions.utils.ResolvedExpressionDefaultVisitor;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.operations.QueryOperation;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.table.expressions.ApiExpressionUtils.isFunctionOfKind;
import static org.apache.flink.table.expressions.ApiExpressionUtils.unresolvedCall;
import static org.apache.flink.table.expressions.ApiExpressionUtils.unresolvedRef;
import static org.apache.flink.table.expressions.ApiExpressionUtils.valueLiteral;
import static org.apache.flink.table.expressions.ExpressionUtils.extractValue;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.AS;
import static org.apache.flink.table.functions.FunctionKind.AGGREGATE;

/**
 * Utility methods for transforming {@link Expression} to use them in {@link QueryOperation}s.
 *
 * <p>Note: Some of these utilities are intended to be used before expressions are fully resolved
 * and some afterwards.
 */
@Internal
public class OperationExpressionsUtils {

    /**
     * Functions that denote a property of the {@link org.apache.flink.table.api.GroupWindow} they
     * are called on.
     */
    public static final Set<FunctionDefinition> WINDOW_PROPERTIES =
            new HashSet<>(
                    Arrays.asList(
                            BuiltInFunctionDefinitions.WINDOW_START,
                            BuiltInFunctionDefinitions.WINDOW_END,
                            BuiltInFunctionDefinitions.PROCTIME,
                            BuiltInFunctionDefinitions.ROWTIME));

    /** Functions that declare the sort order of an expression in a {@code orderBy}. */
    public static final Set<FunctionDefinition> ORDERING =
            new HashSet<>(
                    Arrays.asList(
                            BuiltInFunctionDefinitions.ORDER_ASC,
                            BuiltInFunctionDefinitions.ORDER_DESC));

    // --------------------------------------------------------------------------------------------
    // Pre-expression resolution utils
    // --------------------------------------------------------------------------------------------

    /** Container for extracted expressions of the same family. */
    @Internal
    public static class CategorizedExpressions {
        private final List<Expression> projections;
        private final List<Expression> aggregations;
        private final List<Expression> windowProperties;

        CategorizedExpressions(
                List<Expression> projections,
                List<Expression> aggregations,
                List<Expression> windowProperties) {
            this.projections = projections;
            this.aggregations = aggregations;
            this.windowProperties = windowProperties;
        }

        public List<Expression> getProjections() {
            return projections;
        }

        public List<Expression> getAggregations() {
            return aggregations;
        }

        public List<Expression> getWindowProperties() {
            return windowProperties;
        }
    }

    /**
     * Extracts and deduplicates all aggregation and window property expressions (zero, one, or
     * more) from the given expressions.
     *
     * @param expressions a list of expressions to extract
     * @return a Tuple2, the first field contains the extracted and deduplicated aggregations, and
     *     the second field contains the extracted and deduplicated window properties.
     */
    public static CategorizedExpressions extractAggregationsAndProperties(
            List<Expression> expressions) {
        AggregationAndPropertiesSplitter splitter = new AggregationAndPropertiesSplitter();
        expressions.forEach(expr -> expr.accept(splitter));

        Map<Expression, String> extractedExpressionToFieldName =
                new LinkedHashMap<>(splitter.aggregates);
        extractedExpressionToFieldName.putAll(splitter.properties);

        List<Expression> projections =
                expressions.stream()
                        .map(
                                expr ->
                                        expr.accept(
                                                new ExpressionReplacer(
                                                        extractedExpressionToFieldName)))
                        .collect(Collectors.toList());

        List<Expression> aggregates = nameExpressions(splitter.aggregates);
        List<Expression> properties = nameExpressions(splitter.properties);

        return new CategorizedExpressions(projections, aggregates, properties);
    }

    /**
     * Replaces grouping expressions in projections with references to the corresponding aggregate
     * output fields.
     *
     * <p>Aggregate outputs place grouping fields before aggregate fields. Therefore, grouping
     * expressions and output field names are matched by position.
     */
    public static List<Expression> replaceGroupingExpressions(
            List<Expression> projections,
            List<Expression> groupingExpressions,
            List<String> aggregateOutputFieldNames) {
        if (groupingExpressions.size() > aggregateOutputFieldNames.size()) {
            throw new IllegalArgumentException(
                    "The aggregate output does not contain all grouping expressions.");
        }

        Map<Expression, String> groupingExpressionToFieldName = new LinkedHashMap<>();
        for (int i = 0; i < groupingExpressions.size(); i++) {
            groupingExpressionToFieldName.put(
                    groupingExpressions.get(i), aggregateOutputFieldNames.get(i));
        }

        return projections.stream()
                .map(
                        projection ->
                                projection.accept(
                                        new ExpressionReplacer(groupingExpressionToFieldName)))
                .collect(Collectors.toList());
    }

    private static List<Expression> nameExpressions(Map<Expression, String> expressions) {
        return expressions.entrySet().stream()
                .map(entry -> unresolvedCall(AS, entry.getKey(), valueLiteral(entry.getValue())))
                .collect(Collectors.toList());
    }

    private static class AggregationAndPropertiesSplitter
            extends ApiExpressionDefaultVisitor<Void> {

        private int uniqueId = 0;
        private final Map<Expression, String> aggregates = new LinkedHashMap<>();
        private final Map<Expression, String> properties = new LinkedHashMap<>();

        @Override
        public Void visit(LookupCallExpression unresolvedCall) {
            throw new IllegalStateException(
                    "All lookup calls should be resolved by now. Got: " + unresolvedCall);
        }

        @Override
        public Void visit(UnresolvedCallExpression unresolvedCall) {
            FunctionDefinition functionDefinition = unresolvedCall.getFunctionDefinition();
            if (isFunctionOfKind(unresolvedCall, AGGREGATE)) {
                aggregates.computeIfAbsent(unresolvedCall, expr -> "EXPR$" + uniqueId++);
            } else if (WINDOW_PROPERTIES.contains(functionDefinition)) {
                properties.computeIfAbsent(unresolvedCall, expr -> "EXPR$" + uniqueId++);
            } else {
                unresolvedCall.getChildren().forEach(c -> c.accept(this));
            }
            return null;
        }

        @Override
        protected Void defaultMethod(Expression expression) {
            return null;
        }
    }

    private static class ExpressionReplacer extends ApiExpressionDefaultVisitor<Expression> {

        private final Map<Expression, String> expressionToFieldName;

        private ExpressionReplacer(Map<Expression, String> expressionToFieldName) {
            this.expressionToFieldName = expressionToFieldName;
        }

        @Override
        public Expression visit(LookupCallExpression unresolvedCall) {
            throw new IllegalStateException(
                    "All lookup calls should be resolved by now. Got: " + unresolvedCall);
        }

        @Override
        public Expression visit(CallExpression call) {
            throw new IllegalStateException("All calls should still be unresolved by now.");
        }

        @Override
        public Expression visit(UnresolvedCallExpression unresolvedCall) {
            String fieldName = expressionToFieldName.get(unresolvedCall);
            if (fieldName != null) {
                return unresolvedRef(fieldName);
            }

            final List<Expression> args =
                    unresolvedCall.getChildren().stream()
                            .map(c -> c.accept(this))
                            .collect(Collectors.toList());
            return unresolvedCall.replaceArgs(args);
        }

        @Override
        protected Expression defaultMethod(Expression expression) {
            String fieldName = expressionToFieldName.get(expression);
            return fieldName == null ? expression : unresolvedRef(fieldName);
        }
    }

    // --------------------------------------------------------------------------------------------
    // utils that can be used both before and after resolution
    // --------------------------------------------------------------------------------------------

    private static final ExtractNameVisitor extractNameVisitor = new ExtractNameVisitor();

    /**
     * Extracts names from given expressions if they have one. Expressions that have names are:
     *
     * <ul>
     *   <li>{@link FieldReferenceExpression}
     *   <li>{@link TableReferenceExpression}
     *   <li>{@link LocalReferenceExpression}
     *   <li>{@link BuiltInFunctionDefinitions#AS}
     * </ul>
     *
     * @param expressions list of expressions to extract names from
     * @return corresponding list of optional names
     */
    public static List<Optional<String>> extractNames(List<ResolvedExpression> expressions) {
        return expressions.stream()
                .map(OperationExpressionsUtils::extractName)
                .collect(Collectors.toList());
    }

    /**
     * Extracts name from given expression if it has one. Expressions that have names are:
     *
     * <ul>
     *   <li>{@link FieldReferenceExpression}
     *   <li>{@link TableReferenceExpression}
     *   <li>{@link LocalReferenceExpression}
     *   <li>{@link BuiltInFunctionDefinitions#AS}
     * </ul>
     *
     * @param expression expression to extract name from
     * @return optional name of given expression
     */
    public static Optional<String> extractName(Expression expression) {
        return expression.accept(extractNameVisitor);
    }

    /** Returns the underlying expression if the given expression declares an alias. */
    public static ResolvedExpression unwrapAlias(ResolvedExpression expression) {
        if (expression instanceof CallExpression) {
            CallExpression call = (CallExpression) expression;
            if (call.getFunctionDefinition() == AS) {
                return call.getResolvedChildren().get(0);
            }
        }
        return expression;
    }

    private static class ExtractNameVisitor extends ApiExpressionDefaultVisitor<Optional<String>> {

        @Override
        public Optional<String> visit(LookupCallExpression lookupCall) {
            throw new IllegalStateException("All lookup calls should be resolved by now.");
        }

        @Override
        public Optional<String> visit(UnresolvedCallExpression unresolvedCall) {
            if (unresolvedCall.getFunctionDefinition() == AS) {
                return extractValue(unresolvedCall.getChildren().get(1), String.class);
            } else {
                return Optional.empty();
            }
        }

        @Override
        public Optional<String> visit(CallExpression call) {
            if (call.getFunctionDefinition() == AS) {
                return extractValue(call.getChildren().get(1), String.class);
            } else {
                return Optional.empty();
            }
        }

        @Override
        public Optional<String> visit(LocalReferenceExpression localReference) {
            return Optional.of(localReference.getName());
        }

        @Override
        public Optional<String> visit(TableReferenceExpression tableReference) {
            return Optional.of(tableReference.getName());
        }

        @Override
        public Optional<String> visit(ModelReferenceExpression modelReference) {
            return Optional.of(modelReference.getName());
        }

        @Override
        public Optional<String> visit(FieldReferenceExpression fieldReference) {
            return Optional.of(fieldReference.getName());
        }

        @Override
        protected Optional<String> defaultMethod(Expression expression) {
            return Optional.empty();
        }
    }

    /**
     * Adds an input alias to all {@link FieldReferenceExpression} in the given {@code expression}.
     */
    public static ResolvedExpression scopeReferencesWithAlias(
            final String aliasName, final ResolvedExpression expression) {
        return expression.accept(
                new TableReferenceScopingVisitor(Collections.singletonMap(0, aliasName)));
    }

    /**
     * Adds an input alias to all {@link FieldReferenceExpression} in the given {@code expression}.
     * This method accepts multiple aliases for given input indices.
     */
    public static ResolvedExpression scopeReferencesWithAlias(
            final Map<Integer, String> inputAliases, final ResolvedExpression expression) {
        return expression.accept(new TableReferenceScopingVisitor(inputAliases));
    }

    private static class TableReferenceScopingVisitor
            extends ResolvedExpressionDefaultVisitor<ResolvedExpression> {

        private final Map<Integer, String> inputAliases;

        private TableReferenceScopingVisitor(Map<Integer, String> inputAliases) {
            this.inputAliases = inputAliases;
        }

        @Override
        public ResolvedExpression visit(CallExpression call) {
            List<ResolvedExpression> scopedChildren =
                    call.getChildren().stream()
                            .map(c -> c.accept(this))
                            .collect(Collectors.toList());
            return call.replaceArgs(scopedChildren, call.getOutputDataType());
        }

        @Override
        public ResolvedExpression visit(FieldReferenceExpression fieldReference) {
            return new FieldReferenceExpression(
                    fieldReference.getName(),
                    fieldReference.getOutputDataType(),
                    fieldReference.getInputIndex(),
                    fieldReference.getFieldIndex(),
                    inputAliases.get(fieldReference.getInputIndex()));
        }

        @Override
        protected ResolvedExpression defaultMethod(ResolvedExpression expression) {
            return expression;
        }
    }

    private OperationExpressionsUtils() {}
}
