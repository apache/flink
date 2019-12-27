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
import org.apache.flink.table.catalog.DataTypeLookup;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.delegation.PlannerTypeInferenceUtil;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.UnresolvedCallExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.AggregateFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
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
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.singletonList;
import static org.apache.flink.table.expressions.utils.ApiExpressionUtils.valueLiteral;
import static org.apache.flink.table.types.utils.TypeConversions.fromDataTypeToLegacyInfo;
import static org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType;

/**
 * This rule checks if a {@link UnresolvedCallExpression} can work with the given arguments and infers
 * the output data type. All function calls are resolved {@link CallExpression} after applying this rule.
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
		return expression.stream()
			.flatMap(expr -> expr.accept(new ResolvingCallVisitor(context, null)).stream())
			.collect(Collectors.toList());
	}

	// --------------------------------------------------------------------------------------------

	private static class ResolvingCallVisitor extends RuleExpressionVisitor<List<ResolvedExpression>> {

		private @Nullable SurroundingInfo surroundingInfo;

		ResolvingCallVisitor(ResolutionContext context, @Nullable SurroundingInfo surroundingInfo) {
			super(context);
			this.surroundingInfo = surroundingInfo;
		}

		@Override
		public List<ResolvedExpression> visit(UnresolvedCallExpression unresolvedCall) {
			final FunctionDefinition definition = prepareUserDefinedFunction(unresolvedCall.getFunctionDefinition());

			final String name = unresolvedCall.getFunctionIdentifier()
				.map(FunctionIdentifier::toString)
				.orElseGet(definition::toString);

			final Optional<TypeInference> typeInference = getOptionalTypeInference(definition);

			// resolve the children with information from the current call
			final List<ResolvedExpression> resolvedArgs = new ArrayList<>();
			final int argCount = unresolvedCall.getChildren().size();
			for (int i = 0; i < argCount; i++) {
				final int currentPos = i;
				final ResolvingCallVisitor childResolver = new ResolvingCallVisitor(
					resolutionContext,
					typeInference
						.map(inference -> new SurroundingInfo(name, definition, inference, argCount, currentPos))
						.orElse(null));
				resolvedArgs.addAll(unresolvedCall.getChildren().get(i).accept(childResolver));
			}

			if (definition == BuiltInFunctionDefinitions.FLATTEN) {
				return executeFlatten(resolvedArgs);
			}

			return Collections.singletonList(
				typeInference
					.map(newInference -> runTypeInference(name, unresolvedCall, newInference, resolvedArgs, surroundingInfo))
					.orElseGet(() -> runLegacyTypeInference(unresolvedCall, resolvedArgs))
			);
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
			// TODO support the new type system with ROW and STRUCTURED_TYPE
			final TypeInformation<?> resultType = fromDataTypeToLegacyInfo(composite.getOutputDataType());
			if (resultType instanceof CompositeType) {
				return flattenCompositeType(composite, (CompositeType<?>) resultType);
			} else {
				return singletonList(composite);
			}
		}

		private List<ResolvedExpression> flattenCompositeType(ResolvedExpression composite, CompositeType<?> resultType) {
			return IntStream.range(0, resultType.getArity())
				.mapToObj(idx ->
					resolutionContext.postResolutionFactory()
						.get(
							composite,
							valueLiteral(resultType.getFieldNames()[idx]),
							fromLegacyInfoToDataType(resultType.getTypeAt(idx)))
				)
				.collect(Collectors.toList());
		}

		/**
		 * Temporary method until all calls define a type inference.
		 */
		private Optional<TypeInference> getOptionalTypeInference(FunctionDefinition definition) {
			if (definition instanceof BuiltInFunctionDefinition) {
				final BuiltInFunctionDefinition builtInDefinition = (BuiltInFunctionDefinition) definition;
				if (builtInDefinition.getTypeInference().getOutputTypeStrategy() != TypeStrategies.MISSING) {
					return Optional.of(builtInDefinition.getTypeInference());
				}
			}
			return Optional.empty();
		}

		private ResolvedExpression runTypeInference(
				String name,
				UnresolvedCallExpression unresolvedCall,
				TypeInference inference,
				List<ResolvedExpression> resolvedArgs,
				@Nullable SurroundingInfo surroundingInfo) {

			final Result inferenceResult = TypeInferenceUtil.runTypeInference(
				inference,
				new TableApiCallContext(
					new UnsupportedDataTypeLookup(),
					name,
					unresolvedCall.getFunctionDefinition(),
					resolvedArgs),
				surroundingInfo);

			final List<ResolvedExpression> adaptedArguments = adaptArguments(inferenceResult, resolvedArgs);

			return unresolvedCall.resolve(adaptedArguments, inferenceResult.getOutputDataType());
		}

		private ResolvedExpression runLegacyTypeInference(
				UnresolvedCallExpression unresolvedCall,
				List<ResolvedExpression> resolvedArgs) {

			final PlannerTypeInferenceUtil util = resolutionContext.functionLookup().getPlannerTypeInferenceUtil();

			final Result inferenceResult = util.runTypeInference(
				unresolvedCall,
				resolvedArgs);

			final List<ResolvedExpression> adaptedArguments = adaptArguments(inferenceResult, resolvedArgs);

			return unresolvedCall.resolve(adaptedArguments, inferenceResult.getOutputDataType());
		}

		/**
		 * Adapts the arguments according to the properties of the {@link Result}.
		 */
		private List<ResolvedExpression> adaptArguments(
				Result inferenceResult,
				List<ResolvedExpression> resolvedArgs) {

			return IntStream.range(0, resolvedArgs.size())
				.mapToObj(pos -> {
					final ResolvedExpression argument = resolvedArgs.get(pos);
					final DataType argumentType = argument.getOutputDataType();
					final DataType expectedType = inferenceResult.getExpectedArgumentTypes().get(pos);
					if (!argumentType.equals(expectedType)) {
						return resolutionContext
							.postResolutionFactory()
							.cast(argument, expectedType);
					}
					return argument;
				})
				.collect(Collectors.toList());
		}

		/**
		 * Validates and cleans an inline, unregistered {@link UserDefinedFunction}.
		 */
		private FunctionDefinition prepareUserDefinedFunction(FunctionDefinition definition) {
			if (definition instanceof ScalarFunctionDefinition) {
				final ScalarFunctionDefinition sf = (ScalarFunctionDefinition) definition;
				UserDefinedFunctionHelper.prepareFunction(resolutionContext.configuration(), sf.getScalarFunction());
				return new ScalarFunctionDefinition(
					sf.getName(),
					sf.getScalarFunction());
			} else if (definition instanceof TableFunctionDefinition) {
				final TableFunctionDefinition tf = (TableFunctionDefinition) definition;
				UserDefinedFunctionHelper.prepareFunction(resolutionContext.configuration(), tf.getTableFunction());
				return new TableFunctionDefinition(
					tf.getName(),
					tf.getTableFunction(),
					tf.getResultType());
			} else if (definition instanceof AggregateFunctionDefinition) {
				final AggregateFunctionDefinition af = (AggregateFunctionDefinition) definition;
				UserDefinedFunctionHelper.prepareFunction(resolutionContext.configuration(), af.getAggregateFunction());
				return new AggregateFunctionDefinition(
					af.getName(),
					af.getAggregateFunction(),
					af.getResultTypeInfo(),
					af.getAccumulatorTypeInfo());
			} else if (definition instanceof TableAggregateFunctionDefinition) {
				final TableAggregateFunctionDefinition taf = (TableAggregateFunctionDefinition) definition;
				UserDefinedFunctionHelper.prepareFunction(resolutionContext.configuration(), taf.getTableAggregateFunction());
				return new TableAggregateFunctionDefinition(
					taf.getName(),
					taf.getTableAggregateFunction(),
					taf.getResultTypeInfo(),
					taf.getAccumulatorTypeInfo());
			}
			return definition;
		}
	}

	// --------------------------------------------------------------------------------------------

	private static class UnsupportedDataTypeLookup implements DataTypeLookup {

		@Override
		public Optional<DataType> lookupDataType(String name) {
			throw new TableException("Data type lookup is not supported yet.");
		}

		@Override
		public Optional<DataType> lookupDataType(UnresolvedIdentifier identifier) {
			throw new TableException("Data type lookup is not supported yet.");
		}

		@Override
		public DataType resolveRawDataType(Class<?> clazz) {
			throw new TableException("Data type lookup is not supported yet.");
		}
	}

	private static class TableApiCallContext implements CallContext {

		private final DataTypeLookup lookup;

		private final String name;

		private final FunctionDefinition definition;

		private final List<ResolvedExpression> resolvedArgs;

		public TableApiCallContext(
				DataTypeLookup lookup,
				String name,
				FunctionDefinition definition,
				List<ResolvedExpression> resolvedArgs) {
			this.lookup = lookup;
			this.name = name;
			this.definition = definition;
			this.resolvedArgs = resolvedArgs;
		}

		@Override
		public DataTypeLookup getDataTypeLookup() {
			return lookup;
		}

		@Override
		public FunctionDefinition getFunctionDefinition() {
			return definition;
		}

		@Override
		public boolean isArgumentLiteral(int pos) {
			return getArgument(pos) instanceof ValueLiteralExpression;
		}

		@Override
		public boolean isArgumentNull(int pos) {
			Preconditions.checkArgument(isArgumentLiteral(pos), "Argument at position %s is not a literal.", pos);
			final ValueLiteralExpression literal = (ValueLiteralExpression) getArgument(pos);
			return literal.isNull();
		}

		@Override
		public <T> Optional<T> getArgumentValue(int pos, Class<T> clazz) {
			Preconditions.checkArgument(isArgumentLiteral(pos), "Argument at position %s is not a literal.", pos);
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

		private ResolvedExpression getArgument(int pos) {
			if (pos >= resolvedArgs.size()) {
				throw new IndexOutOfBoundsException(
					String.format(
						"Not enough arguments to access literal at position %d for function '%s'.",
						pos,
						name));
			}
			return resolvedArgs.get(pos);
		}
	}
}
