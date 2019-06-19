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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.InputTypeSpec;
import org.apache.flink.table.expressions.PlannerExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.UnresolvedCallExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.typeutils.TypeCoercion;
import org.apache.flink.table.validate.ValidationFailure;
import org.apache.flink.table.validate.ValidationResult;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType;
import static org.apache.flink.table.util.JavaScalaConversionUtil.toJava;

/**
 * It checks if a {@link UnresolvedCallExpression} can work with given arguments.
 * If the call expects different types of arguments, but the given arguments
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

			final PlannerExpression plannerCall = resolutionContext.bridge(unresolvedCall);

			if (plannerCall instanceof InputTypeSpec) {
				return resolveWithCastedAssignment(
					unresolvedCall,
					resolvedArgs,
					toJava(((InputTypeSpec) plannerCall).expectedTypes()),
					plannerCall.resultType());
			} else {
				validateArguments(plannerCall);

				return unresolvedCall.resolve(
					resolvedArgs,
					fromLegacyInfoToDataType(plannerCall.resultType()));
			}
		}

		private ResolvedExpression resolveWithCastedAssignment(
				UnresolvedCallExpression unresolvedCall,
				List<ResolvedExpression> args,
				List<TypeInformation<?>> expectedTypes,
				TypeInformation<?> resultType) {

			final List<PlannerExpression> plannerArgs = unresolvedCall.getChildren()
				.stream()
				.map(resolutionContext::bridge)
				.collect(Collectors.toList());

			final List<ResolvedExpression> castedArgs = IntStream.range(0, plannerArgs.size())
				.mapToObj(idx -> castIfNeeded(
					args.get(idx),
					plannerArgs.get(idx),
					expectedTypes.get(idx)))
				.collect(Collectors.toList());

			return unresolvedCall.resolve(
				castedArgs,
				fromLegacyInfoToDataType(resultType));
		}

		private void validateArguments(PlannerExpression plannerCall) {
			if (!plannerCall.valid()) {
				throw new ValidationException(
					getValidationErrorMessage(plannerCall)
						.orElse("Unexpected behavior, validation failed but can't get error messages!"));
			}
		}

		/**
		 * Return the validation error message of this {@link PlannerExpression} or return the
		 * validation error message of it's children if it passes the validation. Return empty if
		 * all validation succeeded.
		 */
		private Optional<String> getValidationErrorMessage(PlannerExpression plannerCall) {
			ValidationResult validationResult = plannerCall.validateInput();
			if (validationResult instanceof ValidationFailure) {
				return Optional.of(((ValidationFailure) validationResult).message());
			} else {
				for (Expression plannerExpression: plannerCall.getChildren()) {
					Optional<String> errorMessage = getValidationErrorMessage((PlannerExpression) plannerExpression);
					if (errorMessage.isPresent()) {
						return errorMessage;
					}
				}
			}
			return Optional.empty();
		}

		private ResolvedExpression castIfNeeded(
				ResolvedExpression child,
				PlannerExpression plannerChild,
				TypeInformation<?> expectedType) {
			TypeInformation<?> actualType = plannerChild.resultType();
			if (actualType.equals(expectedType)) {
				return child;
			} else if (TypeCoercion.canSafelyCast(actualType, expectedType)) {
				return resolutionContext
					.postResolutionFactory()
					.cast(child, fromLegacyInfoToDataType(expectedType));
			} else {
				throw new ValidationException(String.format("Incompatible type of argument: %s Expected: %s",
					child,
					expectedType));
			}
		}

		@Override
		protected Expression defaultMethod(Expression expression) {
			return expression;
		}
	}
}
