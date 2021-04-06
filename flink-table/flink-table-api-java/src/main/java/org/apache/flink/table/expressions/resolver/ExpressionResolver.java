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

package org.apache.flink.table.expressions.resolver;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.GroupWindow;
import org.apache.flink.table.api.OverWindow;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.catalog.FunctionLookup;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.LocalReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.UnresolvedReferenceExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.expressions.resolver.lookups.FieldReferenceLookup;
import org.apache.flink.table.expressions.resolver.lookups.TableReferenceLookup;
import org.apache.flink.table.expressions.resolver.rules.ResolverRule;
import org.apache.flink.table.expressions.resolver.rules.ResolverRules;
import org.apache.flink.table.expressions.utils.ApiExpressionDefaultVisitor;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.table.expressions.ApiExpressionUtils.typeLiteral;
import static org.apache.flink.table.expressions.ApiExpressionUtils.valueLiteral;

/**
 * Tries to resolve all unresolved expressions such as {@link UnresolvedReferenceExpression} or
 * calls such as {@link BuiltInFunctionDefinitions#OVER}.
 *
 * <p>The default set of rules ({@link ExpressionResolver#getAllResolverRules()}) will resolve
 * following references:
 *
 * <ul>
 *   <li>flatten '*' and column functions to all fields of underlying inputs
 *   <li>join over aggregates with corresponding over windows into a single resolved call
 *   <li>resolve remaining unresolved references to fields, tables or local references
 *   <li>replace calls to {@link BuiltInFunctionDefinitions#FLATTEN}, {@link
 *       BuiltInFunctionDefinitions#WITH_COLUMNS}, etc.
 *   <li>performs call arguments types validation and inserts additional casts if possible
 * </ul>
 */
@Internal
public class ExpressionResolver {

    /** List of rules for (possibly) expanding the list of unresolved expressions. */
    public static List<ResolverRule> getExpandingResolverRules() {
        return Arrays.asList(
                ResolverRules.UNWRAP_API_EXPRESSION,
                ResolverRules.LOOKUP_CALL_BY_NAME,
                ResolverRules.FLATTEN_STAR_REFERENCE,
                ResolverRules.EXPAND_COLUMN_FUNCTIONS);
    }

    /** List of rules that will be applied during expression resolution. */
    public static List<ResolverRule> getAllResolverRules() {
        return Arrays.asList(
                ResolverRules.UNWRAP_API_EXPRESSION,
                ResolverRules.LOOKUP_CALL_BY_NAME,
                ResolverRules.FLATTEN_STAR_REFERENCE,
                ResolverRules.EXPAND_COLUMN_FUNCTIONS,
                ResolverRules.OVER_WINDOWS,
                ResolverRules.FIELD_RESOLVE,
                ResolverRules.QUALIFY_BUILT_IN_FUNCTIONS,
                ResolverRules.RESOLVE_SQL_CALL,
                ResolverRules.RESOLVE_CALL_BY_ARGUMENTS);
    }

    private static final VerifyResolutionVisitor VERIFY_RESOLUTION_VISITOR =
            new VerifyResolutionVisitor();

    private final ReadableConfig config;

    private final FieldReferenceLookup fieldLookup;

    private final TableReferenceLookup tableLookup;

    private final FunctionLookup functionLookup;

    private final DataTypeFactory typeFactory;

    private final SqlExpressionResolver sqlExpressionResolver;

    private final PostResolverFactory postResolverFactory = new PostResolverFactory();

    private final Map<String, LocalReferenceExpression> localReferences;

    private final Map<Expression, LocalOverWindow> localOverWindows;

    private ExpressionResolver(
            TableConfig config,
            TableReferenceLookup tableLookup,
            FunctionLookup functionLookup,
            DataTypeFactory typeFactory,
            SqlExpressionResolver sqlExpressionResolver,
            FieldReferenceLookup fieldLookup,
            List<OverWindow> localOverWindows,
            List<LocalReferenceExpression> localReferences) {
        this.config = Preconditions.checkNotNull(config).getConfiguration();
        this.tableLookup = Preconditions.checkNotNull(tableLookup);
        this.fieldLookup = Preconditions.checkNotNull(fieldLookup);
        this.functionLookup = Preconditions.checkNotNull(functionLookup);
        this.typeFactory = Preconditions.checkNotNull(typeFactory);
        this.sqlExpressionResolver = Preconditions.checkNotNull(sqlExpressionResolver);

        this.localReferences =
                localReferences.stream()
                        .collect(
                                Collectors.toMap(
                                        LocalReferenceExpression::getName,
                                        Function.identity(),
                                        (u, v) -> {
                                            throw new IllegalStateException(
                                                    "Duplicate local reference: " + u);
                                        },
                                        LinkedHashMap::new));
        this.localOverWindows = prepareOverWindows(localOverWindows);
    }

    /**
     * Creates a builder for {@link ExpressionResolver}. One can add additional properties to the
     * resolver like e.g. {@link GroupWindow} or {@link OverWindow}. You can also add additional
     * {@link ResolverRule}.
     *
     * @param config general configuration
     * @param tableCatalog a way to lookup a table reference by name
     * @param functionLookup a way to lookup call by name
     * @param typeFactory a way to lookup and create data types
     * @param inputs inputs to use for field resolution
     * @return builder for resolver
     */
    public static ExpressionResolverBuilder resolverFor(
            TableConfig config,
            TableReferenceLookup tableCatalog,
            FunctionLookup functionLookup,
            DataTypeFactory typeFactory,
            SqlExpressionResolver sqlExpressionResolver,
            QueryOperation... inputs) {
        return new ExpressionResolverBuilder(
                inputs, config, tableCatalog, functionLookup, typeFactory, sqlExpressionResolver);
    }

    /**
     * Resolves given expressions with configured set of rules. All expressions of an operation
     * should be given at once as some rules might assume the order of expressions.
     *
     * <p>After this method is applied the returned expressions should be ready to be converted to
     * planner specific expressions.
     *
     * @param expressions list of expressions to resolve.
     * @return resolved list of expression
     */
    public List<ResolvedExpression> resolve(List<Expression> expressions) {
        final Function<List<Expression>, List<Expression>> resolveFunction =
                concatenateRules(getAllResolverRules());
        final List<Expression> resolvedExpressions = resolveFunction.apply(expressions);
        return resolvedExpressions.stream()
                .map(e -> e.accept(VERIFY_RESOLUTION_VISITOR))
                .collect(Collectors.toList());
    }

    /**
     * Resolves given expressions with configured set of rules. All expressions of an operation
     * should be given at once as some rules might assume the order of expressions.
     *
     * <p>After this method is applied the returned expressions might contain unresolved expression
     * that can be used for further API transformations.
     *
     * @param expressions list of expressions to resolve.
     * @return resolved list of expression
     */
    public List<Expression> resolveExpanding(List<Expression> expressions) {
        final Function<List<Expression>, List<Expression>> resolveFunction =
                concatenateRules(getExpandingResolverRules());
        return resolveFunction.apply(expressions);
    }

    /**
     * Enables the creation of resolved expressions for transformations after the actual resolution.
     */
    public PostResolverFactory postResolverFactory() {
        return postResolverFactory;
    }

    private Function<List<Expression>, List<Expression>> concatenateRules(
            List<ResolverRule> rules) {
        return rules.stream()
                .reduce(
                        Function.identity(),
                        (function, resolverRule) ->
                                function.andThen(
                                        exprs ->
                                                resolverRule.apply(
                                                        exprs, new ExpressionResolverContext())),
                        Function::andThen);
    }

    private Map<Expression, LocalOverWindow> prepareOverWindows(List<OverWindow> overWindows) {
        return overWindows.stream()
                .map(this::resolveOverWindow)
                .collect(Collectors.toMap(LocalOverWindow::getAlias, Function.identity()));
    }

    private List<Expression> prepareExpressions(List<Expression> expressions) {
        return expressions.stream()
                .flatMap(e -> resolveExpanding(Collections.singletonList(e)).stream())
                .map(this::resolveFieldsInSingleExpression)
                .collect(Collectors.toList());
    }

    private Expression resolveFieldsInSingleExpression(Expression expression) {
        List<Expression> expressions =
                ResolverRules.FIELD_RESOLVE.apply(
                        Collections.singletonList(expression), new ExpressionResolverContext());

        if (expressions.size() != 1) {
            throw new TableException(
                    "Expected a single expression as a result. Got: " + expressions);
        }

        return expressions.get(0);
    }

    private static class VerifyResolutionVisitor
            extends ApiExpressionDefaultVisitor<ResolvedExpression> {

        @Override
        public ResolvedExpression visit(CallExpression call) {
            call.getChildren().forEach(c -> c.accept(this));
            return call;
        }

        @Override
        protected ResolvedExpression defaultMethod(Expression expression) {
            if (expression instanceof ResolvedExpression) {
                return (ResolvedExpression) expression;
            }
            throw new TableException(
                    "All expressions should have been resolved at this stage. Unexpected expression: "
                            + expression);
        }
    }

    private class ExpressionResolverContext implements ResolverRule.ResolutionContext {

        @Override
        public ReadableConfig configuration() {
            return config;
        }

        @Override
        public FieldReferenceLookup referenceLookup() {
            return fieldLookup;
        }

        @Override
        public TableReferenceLookup tableLookup() {
            return tableLookup;
        }

        @Override
        public FunctionLookup functionLookup() {
            return functionLookup;
        }

        @Override
        public DataTypeFactory typeFactory() {
            return typeFactory;
        }

        public SqlExpressionResolver sqlExpressionResolver() {
            return sqlExpressionResolver;
        }

        @Override
        public PostResolverFactory postResolutionFactory() {
            return postResolverFactory;
        }

        @Override
        public Optional<LocalReferenceExpression> getLocalReference(String alias) {
            return Optional.ofNullable(localReferences.get(alias));
        }

        @Override
        public List<LocalReferenceExpression> getLocalReferences() {
            return new ArrayList<>(localReferences.values());
        }

        @Override
        public Optional<LocalOverWindow> getOverWindow(Expression alias) {
            return Optional.ofNullable(localOverWindows.get(alias));
        }
    }

    private LocalOverWindow resolveOverWindow(OverWindow overWindow) {
        return new LocalOverWindow(
                overWindow.getAlias(),
                prepareExpressions(overWindow.getPartitioning()),
                resolveFieldsInSingleExpression(overWindow.getOrder()),
                resolveFieldsInSingleExpression(overWindow.getPreceding()),
                overWindow.getFollowing().map(this::resolveFieldsInSingleExpression).orElse(null));
    }

    /**
     * Factory for creating resolved expressions after the actual resolution has happened. This is
     * required when a resolved expression stack needs to be modified in later transformations.
     *
     * <p>Note: Further resolution or validation will not happen anymore, therefore the created
     * expressions must be valid.
     */
    public class PostResolverFactory {

        public CallExpression as(ResolvedExpression expression, String alias) {
            final FunctionLookup.Result lookupOfAs =
                    functionLookup.lookupBuiltInFunction(BuiltInFunctionDefinitions.AS);

            return new CallExpression(
                    lookupOfAs.getFunctionIdentifier(),
                    lookupOfAs.getFunctionDefinition(),
                    Arrays.asList(expression, valueLiteral(alias)),
                    expression.getOutputDataType());
        }

        public CallExpression cast(ResolvedExpression expression, DataType dataType) {
            final FunctionLookup.Result lookupOfCast =
                    functionLookup.lookupBuiltInFunction(BuiltInFunctionDefinitions.CAST);

            return new CallExpression(
                    lookupOfCast.getFunctionIdentifier(),
                    lookupOfCast.getFunctionDefinition(),
                    Arrays.asList(expression, typeLiteral(dataType)),
                    dataType);
        }

        public CallExpression row(DataType dataType, ResolvedExpression... expression) {
            final FunctionLookup.Result lookupOfRow =
                    functionLookup.lookupBuiltInFunction(BuiltInFunctionDefinitions.ROW);

            return new CallExpression(
                    lookupOfRow.getFunctionIdentifier(),
                    lookupOfRow.getFunctionDefinition(),
                    Arrays.asList(expression),
                    dataType);
        }

        public CallExpression array(DataType dataType, ResolvedExpression... expression) {
            final FunctionLookup.Result lookupOfArray =
                    functionLookup.lookupBuiltInFunction(BuiltInFunctionDefinitions.ARRAY);

            return new CallExpression(
                    lookupOfArray.getFunctionIdentifier(),
                    lookupOfArray.getFunctionDefinition(),
                    Arrays.asList(expression),
                    dataType);
        }

        public CallExpression map(DataType dataType, ResolvedExpression... expression) {
            final FunctionLookup.Result lookupOfArray =
                    functionLookup.lookupBuiltInFunction(BuiltInFunctionDefinitions.MAP);

            return new CallExpression(
                    lookupOfArray.getFunctionIdentifier(),
                    lookupOfArray.getFunctionDefinition(),
                    Arrays.asList(expression),
                    dataType);
        }

        public CallExpression wrappingCall(
                BuiltInFunctionDefinition definition, ResolvedExpression expression) {
            final FunctionLookup.Result lookupOfDefinition =
                    functionLookup.lookupBuiltInFunction(definition);

            return new CallExpression(
                    lookupOfDefinition.getFunctionIdentifier(),
                    lookupOfDefinition.getFunctionDefinition(),
                    Collections.singletonList(expression),
                    expression.getOutputDataType()); // the output type is equal to the input type
        }

        public CallExpression get(
                ResolvedExpression composite, ValueLiteralExpression key, DataType dataType) {
            final FunctionLookup.Result lookupOfGet =
                    functionLookup.lookupBuiltInFunction(BuiltInFunctionDefinitions.GET);

            return new CallExpression(
                    lookupOfGet.getFunctionIdentifier(),
                    lookupOfGet.getFunctionDefinition(),
                    Arrays.asList(composite, key),
                    dataType);
        }
    }

    /** Builder for creating {@link ExpressionResolver}. */
    public static class ExpressionResolverBuilder {

        private final TableConfig config;
        private final List<QueryOperation> queryOperations;
        private final TableReferenceLookup tableCatalog;
        private final FunctionLookup functionLookup;
        private final DataTypeFactory typeFactory;
        private final SqlExpressionResolver sqlExpressionResolver;
        private List<OverWindow> logicalOverWindows = new ArrayList<>();
        private List<LocalReferenceExpression> localReferences = new ArrayList<>();

        private ExpressionResolverBuilder(
                QueryOperation[] queryOperations,
                TableConfig config,
                TableReferenceLookup tableCatalog,
                FunctionLookup functionLookup,
                DataTypeFactory typeFactory,
                SqlExpressionResolver sqlExpressionResolver) {
            this.config = config;
            this.queryOperations = Arrays.asList(queryOperations);
            this.tableCatalog = tableCatalog;
            this.functionLookup = functionLookup;
            this.typeFactory = typeFactory;
            this.sqlExpressionResolver = sqlExpressionResolver;
        }

        public ExpressionResolverBuilder withOverWindows(List<OverWindow> windows) {
            this.logicalOverWindows = Preconditions.checkNotNull(windows);
            return this;
        }

        public ExpressionResolverBuilder withLocalReferences(
                LocalReferenceExpression... localReferences) {
            this.localReferences = Arrays.asList(localReferences);
            return this;
        }

        public ExpressionResolver build() {
            return new ExpressionResolver(
                    config,
                    tableCatalog,
                    functionLookup,
                    typeFactory,
                    sqlExpressionResolver,
                    new FieldReferenceLookup(queryOperations),
                    logicalOverWindows,
                    localReferences);
        }
    }
}
