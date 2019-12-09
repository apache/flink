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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.delegation.PlannerTypeInferenceUtil;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.UnresolvedCallExpression;
import org.apache.flink.table.planner.typeutils.TypeCoercion;
import org.apache.flink.table.planner.validate.ValidationFailure;
import org.apache.flink.table.planner.validate.ValidationResult;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeInferenceUtil;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.table.planner.utils.JavaScalaConversionUtil.toJava;
import static org.apache.flink.table.runtime.types.TypeInfoLogicalTypeConverter.fromTypeInfoToLogicalType;
import static org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType;

/**
 * Implementation of {@link PlannerTypeInferenceUtil}.
 */
@Internal
public final class PlannerTypeInferenceUtilImpl implements PlannerTypeInferenceUtil {

	public static final PlannerTypeInferenceUtil INSTANCE = new PlannerTypeInferenceUtilImpl();

	private static final PlannerExpressionConverter CONVERTER = PlannerExpressionConverter.INSTANCE();

	@Override
	public TypeInferenceUtil.Result runTypeInference(
			UnresolvedCallExpression unresolvedCall,
			List<ResolvedExpression> resolvedArgs) {
		final PlannerExpression plannerCall = unresolvedCall.accept(CONVERTER);

		if (plannerCall instanceof InputTypeSpec) {
			return resolveWithCastedAssignment(
				unresolvedCall,
				resolvedArgs,
				toJava(((InputTypeSpec) plannerCall).expectedTypes()),
				plannerCall.resultType());
		} else {
			validateArguments(plannerCall);

			final List<DataType> expectedArgumentTypes = resolvedArgs.stream()
				.map(ResolvedExpression::getOutputDataType)
				.collect(Collectors.toList());

			return new TypeInferenceUtil.Result(
				expectedArgumentTypes,
				null,
				fromLegacyInfoToDataType(plannerCall.resultType()));
		}
	}

	private TypeInferenceUtil.Result resolveWithCastedAssignment(
			UnresolvedCallExpression unresolvedCall,
			List<ResolvedExpression> args,
			List<TypeInformation<?>> expectedTypes,
			TypeInformation<?> resultType) {

		final List<PlannerExpression> plannerArgs = unresolvedCall.getChildren()
			.stream()
			.map(e -> e.accept(CONVERTER))
			.collect(Collectors.toList());

		final List<DataType> castedArgs = IntStream.range(0, plannerArgs.size())
			.mapToObj(idx -> castIfNeeded(
				args.get(idx),
				plannerArgs.get(idx),
				expectedTypes.get(idx)))
			.collect(Collectors.toList());

		return new TypeInferenceUtil.Result(
			castedArgs,
			null,
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

	private DataType castIfNeeded(
			ResolvedExpression child,
			PlannerExpression plannerChild,
			TypeInformation<?> expectedType) {
		TypeInformation<?> actualType = plannerChild.resultType();
		if (actualType.equals(expectedType)) {
			return child.getOutputDataType();
		} else if (TypeCoercion.canSafelyCast(
				fromTypeInfoToLogicalType(actualType), fromTypeInfoToLogicalType(expectedType))) {
			return fromLegacyInfoToDataType(expectedType);
		} else {
			throw new ValidationException(String.format("Incompatible type of argument: %s Expected: %s",
				child,
				expectedType));
		}
	}
}
