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

package org.apache.flink.table.expressions.resolver.rules;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.delegation.PlannerTypeInferenceUtil;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.TypeLiteralExpression;
import org.apache.flink.table.expressions.UnresolvedCallExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.AggregateFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.functions.ScalarFunctionDefinition;
import org.apache.flink.table.functions.TableAggregateFunctionDefinition;
import org.apache.flink.table.functions.TableFunctionDefinition;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.functions.UserDefinedFunctionHelper;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeInferenceUtil;
import org.apache.flink.table.types.inference.TypeInferenceUtil.Result;
import org.apache.flink.table.types.inference.TypeInferenceUtil.SurroundingInfo;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.singletonList;
import static org.apache.flink.table.expressions.ApiExpressionUtils.valueLiteral;
import static org.apache.flink.table.types.logical.utils.LogicalTypeCasts.supportsAvoidingCast;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasLegacyTypes;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.isCompositeType;
import static org.apache.flink.table.types.utils.TypeConversions.fromDataTypeToLegacyInfo;
import static org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType;

/**
 * This rule checks if a {@link UnresolvedCallExpression} can work with the given arguments and
 * infers the output data type. All function calls are resolved {@link CallExpression} after
 * applying this rule.
 *
 * <p>This rule also resolves {@code flatten()} calls on composite types.
 *
 * <p>If the call expects different types of arguments, but the given arguments have types that can
 * be casted, a {@link BuiltInFunctionDefinitions#CAST} expression is inserted.
 *
 * <p>It validates and prepares inline, unregistered {@link UserDefinedFunction}s.
 */
@Internal
final class ResolveCallByArgumentsRule implements ResolverRule {

    @Override
    public List<Expression> apply(List<Expression> expression, ResolutionContext context) {
        // only the top-level expressions may access the output data type
        final SurroundingInfo surroundingInfo =
                context.getOutputDataType().map(SurroundingInfo::of).orElse(null);
        return expression.stream()
                .flatMap(e -> e.accept(new ResolvingCallVisitor(context, surroundingInfo)).stream())
                .collect(Collectors.toList());
    }

    // --------------------------------------------------------------------------------------------

    private static class ResolvingCallVisitor
            extends RuleExpressionVisitor<List<ResolvedExpression>> {

        private final @Nullable SurroundingInfo surroundingInfo;

        ResolvingCallVisitor(ResolutionContext context, @Nullable SurroundingInfo surroundingInfo) {
            super(context);
            this.surroundingInfo = surroundingInfo;
        }

        @Override
        public List<ResolvedExpression> visit(UnresolvedCallExpression unresolvedCall) {
            final FunctionDefinition definition;
            // clean functions that were not registered in a catalog
            if (!unresolvedCall.getFunctionIdentifier().isPresent()) {
                definition =
                        prepareInlineUserDefinedFunction(unresolvedCall.getFunctionDefinition());
            } else {
                definition = unresolvedCall.getFunctionDefinition();
            }

            final String name =
                    unresolvedCall
                            .getFunctionIdentifier()
                            .map(FunctionIdentifier::toString)
                            .orElseGet(definition::toString);

            final Optional<TypeInference> typeInference = getOptionalTypeInference(definition);

            // resolve the children with information from the current call
            final List<ResolvedExpression> resolvedArgs = new ArrayList<>();
            final int argCount = unresolvedCall.getChildren().size();

            for (int i = 0; i < argCount; i++) {
                final int currentPos = i;
                final SurroundingInfo surroundingInfo =
                        typeInference
                                .map(
                                        inference ->
                                                SurroundingInfo.of(
                                                        name,
                                                        definition,
                                                        inference,
                                                        argCount,
                                                        currentPos,
                                                        resolutionContext.isGroupedAggregation()))
                                .orElse(null);
                final ResolvingCallVisitor childResolver =
                        new ResolvingCallVisitor(resolutionContext, surroundingInfo);
                resolvedArgs.addAll(unresolvedCall.getChildren().get(i).accept(childResolver));
            }

            if (definition == BuiltInFunctionDefinitions.FLATTEN) {
                return executeFlatten(resolvedArgs);
            }

            return Collections.singletonList(
                    typeInference
                            .map(
                                    newInference ->
                                            runTypeInference(
                                                    name,
                                                    unresolvedCall,
                                                    newInference,
                                                    resolvedArgs,
                                                    surroundingInfo))
                            .orElseGet(() -> runLegacyTypeInference(unresolvedCall, resolvedArgs)));
        }

        @Override
        protected List<ResolvedExpression> defaultMethod(Expression expression) {
            if (expression instanceof ResolvedExpression) {
                return Collections.singletonList((ResolvedExpression) expression);
            }
            throw new TableException("Unexpected unresolved expression: " + expression);
        }

        private List<ResolvedExpression> executeFlatten(List<ResolvedExpression> args) {
            if (args.size() != 1) {
                throw new ValidationException("Invalid number of arguments for flattening.");
            }
            final ResolvedExpression composite = args.get(0);
            final LogicalType compositeType = composite.getOutputDataType().getLogicalType();
            if (hasLegacyTypes(compositeType)) {
                return flattenLegacyCompositeType(composite);
            }
            return flattenCompositeType(composite);
        }

        private List<ResolvedExpression> flattenCompositeType(ResolvedExpression composite) {
            final DataType dataType = composite.getOutputDataType();
            final LogicalType type = dataType.getLogicalType();
            if (!isCompositeType(type)) {
                return singletonList(composite);
            }
            final List<DataType> fieldDataTypes = DataTypeUtils.flattenToDataTypes(dataType);
            final List<String> fieldNames = DataTypeUtils.flattenToNames(dataType);
            return IntStream.range(0, fieldDataTypes.size())
                    .mapToObj(
                            idx -> {
                                final DataType fieldDataType = fieldDataTypes.get(idx);
                                final DataType nullableFieldDataType;
                                if (type.isNullable()) {
                                    nullableFieldDataType = fieldDataType.nullable();
                                } else {
                                    nullableFieldDataType = fieldDataType;
                                }
                                return resolutionContext
                                        .postResolutionFactory()
                                        .get(
                                                composite,
                                                valueLiteral(fieldNames.get(idx)),
                                                nullableFieldDataType);
                            })
                    .collect(Collectors.toList());
        }

        private List<ResolvedExpression> flattenLegacyCompositeType(ResolvedExpression composite) {
            final TypeInformation<?> resultType =
                    fromDataTypeToLegacyInfo(composite.getOutputDataType());
            if (!(resultType instanceof CompositeType)) {
                return singletonList(composite);
            }
            final CompositeType<?> compositeType = (CompositeType<?>) resultType;
            return IntStream.range(0, resultType.getArity())
                    .mapToObj(
                            idx ->
                                    resolutionContext
                                            .postResolutionFactory()
                                            .get(
                                                    composite,
                                                    valueLiteral(
                                                            compositeType.getFieldNames()[idx]),
                                                    fromLegacyInfoToDataType(
                                                            compositeType.getTypeAt(idx))))
                    .collect(Collectors.toList());
        }

        /** Temporary method until all calls define a type inference. */
        private Optional<TypeInference> getOptionalTypeInference(FunctionDefinition definition) {
            if (definition instanceof ScalarFunctionDefinition
                    || definition instanceof TableFunctionDefinition
                    || definition instanceof AggregateFunctionDefinition
                    || definition instanceof TableAggregateFunctionDefinition) {
                return Optional.empty();
            }

            final TypeInference inference =
                    definition.getTypeInference(resolutionContext.typeFactory());
            if (inference.getOutputTypeStrategy() != TypeStrategies.MISSING) {
                return Optional.of(inference);
            } else {
                return Optional.empty();
            }
        }

        private ResolvedExpression runTypeInference(
                String name,
                UnresolvedCallExpression unresolvedCall,
                TypeInference inference,
                List<ResolvedExpression> resolvedArgs,
                @Nullable SurroundingInfo surroundingInfo) {

            final Result inferenceResult =
                    TypeInferenceUtil.runTypeInference(
                            inference,
                            new TableApiCallContext(
                                    resolutionContext.typeFactory(),
                                    name,
                                    unresolvedCall.getFunctionDefinition(),
                                    resolvedArgs,
                                    resolutionContext.isGroupedAggregation()),
                            surroundingInfo);

            final List<ResolvedExpression> adaptedArguments =
                    adaptArguments(inferenceResult, resolvedArgs);

            return unresolvedCall.resolve(adaptedArguments, inferenceResult.getOutputDataType());
        }

        private ResolvedExpression runLegacyTypeInference(
                UnresolvedCallExpression unresolvedCall, List<ResolvedExpression> resolvedArgs) {

            final PlannerTypeInferenceUtil util =
                    resolutionContext.functionLookup().getPlannerTypeInferenceUtil();

            final Result inferenceResult = util.runTypeInference(unresolvedCall, resolvedArgs);

            final List<ResolvedExpression> adaptedArguments =
                    adaptArguments(inferenceResult, resolvedArgs);

            return unresolvedCall.resolve(adaptedArguments, inferenceResult.getOutputDataType());
        }

        /** Adapts the arguments according to the properties of the {@link Result}. */
        private List<ResolvedExpression> adaptArguments(
                Result inferenceResult, List<ResolvedExpression> resolvedArgs) {

            return IntStream.range(0, resolvedArgs.size())
                    .mapToObj(
                            pos -> {
                                final ResolvedExpression argument = resolvedArgs.get(pos);
                                final DataType argumentType = argument.getOutputDataType();
                                final DataType expectedType =
                                        inferenceResult.getExpectedArgumentTypes().get(pos);

                                if (!supportsAvoidingCast(
                                        argumentType.getLogicalType(),
                                        expectedType.getLogicalType())) {
                                    return resolutionContext
                                            .postResolutionFactory()
                                            .cast(argument, expectedType);
                                }
                                return argument;
                            })
                    .collect(Collectors.toList());
        }

        /** Validates and cleans an inline, unregistered {@link UserDefinedFunction}. */
        private FunctionDefinition prepareInlineUserDefinedFunction(FunctionDefinition definition) {
            if (definition instanceof ScalarFunctionDefinition) {
                final ScalarFunctionDefinition sf = (ScalarFunctionDefinition) definition;
                UserDefinedFunctionHelper.prepareInstance(
                        resolutionContext.configuration(), sf.getScalarFunction());
                return new ScalarFunctionDefinition(sf.getName(), sf.getScalarFunction());
            } else if (definition instanceof TableFunctionDefinition) {
                final TableFunctionDefinition tf = (TableFunctionDefinition) definition;
                UserDefinedFunctionHelper.prepareInstance(
                        resolutionContext.configuration(), tf.getTableFunction());
                return new TableFunctionDefinition(
                        tf.getName(), tf.getTableFunction(), tf.getResultType());
            } else if (definition instanceof AggregateFunctionDefinition) {
                final AggregateFunctionDefinition af = (AggregateFunctionDefinition) definition;
                UserDefinedFunctionHelper.prepareInstance(
                        resolutionContext.configuration(), af.getAggregateFunction());
                return new AggregateFunctionDefinition(
                        af.getName(),
                        af.getAggregateFunction(),
                        af.getResultTypeInfo(),
                        af.getAccumulatorTypeInfo());
            } else if (definition instanceof TableAggregateFunctionDefinition) {
                final TableAggregateFunctionDefinition taf =
                        (TableAggregateFunctionDefinition) definition;
                UserDefinedFunctionHelper.prepareInstance(
                        resolutionContext.configuration(), taf.getTableAggregateFunction());
                return new TableAggregateFunctionDefinition(
                        taf.getName(),
                        taf.getTableAggregateFunction(),
                        taf.getResultTypeInfo(),
                        taf.getAccumulatorTypeInfo());
            } else if (definition instanceof UserDefinedFunction) {
                UserDefinedFunctionHelper.prepareInstance(
                        resolutionContext.configuration(), (UserDefinedFunction) definition);
            }
            return definition;
        }
    }

    // --------------------------------------------------------------------------------------------

    private static class TableApiCallContext implements CallContext {

        private final DataTypeFactory typeFactory;

        private final String name;

        private final FunctionDefinition definition;

        private final List<ResolvedExpression> resolvedArgs;

        private final boolean isGroupedAggregation;

        public TableApiCallContext(
                DataTypeFactory typeFactory,
                String name,
                FunctionDefinition definition,
                List<ResolvedExpression> resolvedArgs,
                boolean isGroupedAggregation) {
            this.typeFactory = typeFactory;
            this.name = name;
            this.definition = definition;
            this.resolvedArgs = resolvedArgs;
            this.isGroupedAggregation = isGroupedAggregation;
        }

        @Override
        public DataTypeFactory getDataTypeFactory() {
            return typeFactory;
        }

        @Override
        public FunctionDefinition getFunctionDefinition() {
            return definition;
        }

        @Override
        public boolean isArgumentLiteral(int pos) {
            final ResolvedExpression arg = getArgument(pos);
            return arg instanceof ValueLiteralExpression || arg instanceof TypeLiteralExpression;
        }

        @Override
        public boolean isArgumentNull(int pos) {
            Preconditions.checkArgument(
                    isArgumentLiteral(pos), "Argument at position %s is not a literal.", pos);
            final ResolvedExpression arg = getArgument(pos);
            // special case for type literals in Table API only
            if (arg instanceof TypeLiteralExpression) {
                return false;
            }
            final ValueLiteralExpression literal = (ValueLiteralExpression) getArgument(pos);
            return literal.isNull();
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> Optional<T> getArgumentValue(int pos, Class<T> clazz) {
            Preconditions.checkArgument(
                    isArgumentLiteral(pos), "Argument at position %s is not a literal.", pos);
            final ResolvedExpression arg = getArgument(pos);
            // special case for type literals in Table API only
            if (arg instanceof TypeLiteralExpression) {
                if (!DataType.class.isAssignableFrom(clazz)) {
                    return Optional.empty();
                }
                return Optional.of((T) arg.getOutputDataType());
            }
            final ValueLiteralExpression literal = (ValueLiteralExpression) getArgument(pos);
            return literal.getValueAs(clazz);
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public List<DataType> getArgumentDataTypes() {
            return resolvedArgs.stream()
                    .map(ResolvedExpression::getOutputDataType)
                    .collect(Collectors.toList());
        }

        @Override
        public Optional<DataType> getOutputDataType() {
            return Optional.empty();
        }

        @Override
        public boolean isGroupedAggregation() {
            return isGroupedAggregation;
        }

        private ResolvedExpression getArgument(int pos) {
            if (pos >= resolvedArgs.size()) {
                throw new IndexOutOfBoundsException(
                        String.format(
                                "Not enough arguments to access literal at position %d for function '%s'.",
                                pos, name));
            }
            return resolvedArgs.get(pos);
        }
    }
}
