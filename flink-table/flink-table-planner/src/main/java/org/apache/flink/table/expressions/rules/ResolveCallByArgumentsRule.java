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

package org.apache.flink.table.expressions.rules;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.delegation.PlannerTypeInferenceUtil;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.UnresolvedCallExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeInferenceUtil;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.util.Preconditions;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * This rule checks if a {@link UnresolvedCallExpression} can work with the given arguments and infers
 * the output data type. All function calls are resolved {@link CallExpression} after applying this rule.
 *
 * <p>If the call expects different types of arguments, but the given arguments
 * have types that can be casted, a {@link BuiltInFunctionDefinitions#CAST}
 * expression is inserted.
 */
@Internal
final class ResolveCallByArgumentsRule implements ResolverRule {

	@Override
	public List<Expression> apply(List<Expression> expression, ResolutionContext context) {
		return expression.stream()
			.map(expr -> expr.accept(new CallArgumentsCastingVisitor(context)))
			.collect(Collectors.toList());
	}

	private class CallArgumentsCastingVisitor extends RuleExpressionVisitor<Expression> {

		CallArgumentsCastingVisitor(ResolutionContext context) {
			super(context);
		}

		@Override
		public ResolvedExpression visit(UnresolvedCallExpression unresolvedCall) {

			final List<ResolvedExpression> resolvedArgs = unresolvedCall.getChildren().stream()
				.map(c -> c.accept(this))
				.map(e -> {
					if (e instanceof ResolvedExpression) {
						return (ResolvedExpression) e;
					}
					throw new TableException("Unexpected unresolved expression: " + e);
				})
				.collect(Collectors.toList());

			if (unresolvedCall.getFunctionDefinition() instanceof BuiltInFunctionDefinition) {
				final BuiltInFunctionDefinition definition =
					(BuiltInFunctionDefinition) unresolvedCall.getFunctionDefinition();

				if (definition.getTypeInference().getOutputTypeStrategy() != TypeStrategies.MISSING) {
					return runTypeInference(
						unresolvedCall,
						definition.getTypeInference(),
						resolvedArgs);
				}
			}
			return runLegacyTypeInference(unresolvedCall, resolvedArgs);
		}

		private ResolvedExpression runTypeInference(
				UnresolvedCallExpression unresolvedCall,
				TypeInference inference,
				List<ResolvedExpression> resolvedArgs) {

			final String name = unresolvedCall.getObjectIdentifier()
				.map(ObjectIdentifier::toString)
				.orElseGet(() -> unresolvedCall.getFunctionDefinition().toString());

			final TypeInferenceUtil.Result inferenceResult = TypeInferenceUtil.runTypeInference(
				inference,
				new TableApiCallContext(name, unresolvedCall.getFunctionDefinition(), resolvedArgs));

			final List<ResolvedExpression> adaptedArguments = adaptArguments(inferenceResult, resolvedArgs);

			return unresolvedCall.resolve(adaptedArguments, inferenceResult.getOutputDataType());
		}

		private ResolvedExpression runLegacyTypeInference(
				UnresolvedCallExpression unresolvedCall,
				List<ResolvedExpression> resolvedArgs) {

			final PlannerTypeInferenceUtil util = PlannerTypeInferenceUtil.create();

			final TypeInferenceUtil.Result inferenceResult = util.runTypeInference(
				unresolvedCall,
				resolvedArgs);

			final List<ResolvedExpression> adaptedArguments = adaptArguments(inferenceResult, resolvedArgs);

			return unresolvedCall.resolve(adaptedArguments, inferenceResult.getOutputDataType());
		}

		/**
		 * Adapts the arguments according to the properties of the {@link TypeInferenceUtil.Result}.
		 */
		private List<ResolvedExpression> adaptArguments(
				TypeInferenceUtil.Result inferenceResult,
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

		@Override
		protected Expression defaultMethod(Expression expression) {
			return expression;
		}
	}

	// --------------------------------------------------------------------------------------------

	private class TableApiCallContext implements CallContext {

		private final String name;

		private final FunctionDefinition definition;

		private final List<ResolvedExpression> resolvedArgs;

		public TableApiCallContext(
				String name,
				FunctionDefinition definition,
				List<ResolvedExpression> resolvedArgs) {
			this.name = name;
			this.definition = definition;
			this.resolvedArgs = resolvedArgs;
		}

		@Override
		public List<DataType> getArgumentDataTypes() {
			return resolvedArgs.stream()
				.map(ResolvedExpression::getOutputDataType)
				.collect(Collectors.toList());
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
