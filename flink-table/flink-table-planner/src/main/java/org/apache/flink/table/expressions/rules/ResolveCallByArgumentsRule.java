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
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.InputTypeSpec;
import org.apache.flink.table.expressions.PlannerExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.typeutils.TypeCoercion;
import org.apache.flink.table.validate.ValidationFailure;
import org.apache.flink.table.validate.ValidationResult;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Arrays.asList;
import static org.apache.flink.table.expressions.ApiExpressionUtils.typeLiteral;
import static org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType;
import static org.apache.flink.table.util.JavaScalaConversionUtil.toJava;

/**
 * It checks if a {@link CallExpression} can work with given arguments.
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
		public Expression visitCall(CallExpression call) {
			PlannerExpression plannerCall = resolutionContext.bridge(call);
			if (plannerCall instanceof InputTypeSpec) {
				List<TypeInformation<?>> expectedTypes = toJava(((InputTypeSpec) plannerCall).expectedTypes());
				return performTypeResolution(call, expectedTypes);
			} else {
				return validateArguments(call, plannerCall);
			}
		}

		private Expression performTypeResolution(CallExpression call, List<TypeInformation<?>> expectedTypes) {
			List<PlannerExpression> args = call.getChildren()
				.stream()
				.map(resolutionContext::bridge)
				.collect(Collectors.toList());

			List<Expression> newArgs = IntStream.range(0, args.size())
				.mapToObj(idx -> castIfNeeded(args.get(idx), expectedTypes.get(idx)))
				.collect(Collectors.toList());

			return new CallExpression(call.getFunctionDefinition(), newArgs);
		}

		private Expression validateArguments(CallExpression call, PlannerExpression plannerCall) {
			if (!plannerCall.valid()) {
				throw new ValidationException(
					getValidationErrorMessage(plannerCall)
						.orElse("Unexpected behavior, validation failed but can't get error messages!"));
			}
			return call;
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

		private Expression castIfNeeded(PlannerExpression childExpression, TypeInformation<?> expectedType) {
			TypeInformation<?> actualType = childExpression.resultType();
			if (actualType.equals(expectedType)) {
				return childExpression;
			} else if (TypeCoercion.canSafelyCast(actualType, expectedType)) {
				return new CallExpression(
					BuiltInFunctionDefinitions.CAST,
					asList(
						childExpression,
						typeLiteral(fromLegacyInfoToDataType(expectedType)))
				);
			} else {
				throw new ValidationException(String.format("Incompatible type of argument: %s Expected: %s",
					childExpression,
					expectedType));
			}
		}

		@Override
		protected Expression defaultMethod(Expression expression) {
			return expression;
		}
	}
}
