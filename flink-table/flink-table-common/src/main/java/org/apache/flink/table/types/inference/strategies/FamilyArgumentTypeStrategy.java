/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.types.inference.strategies;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentTypeStrategy;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.logical.LegacyTypeInformationType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.table.types.inference.strategies.StrategyUtils.findDataType;

/**
 * Strategy for an argument that corresponds to a given {@link LogicalTypeFamily} and nullability.
 *
 * <p>Implicit casts will be inserted if possible.
 */
@Internal
public final class FamilyArgumentTypeStrategy implements ArgumentTypeStrategy {

	private final LogicalTypeFamily expectedFamily;

	private final @Nullable Boolean expectedNullability;

	private static final Map<LogicalTypeFamily, LogicalTypeRoot> familyToRoot = new HashMap<>();
	static {
		// "fallback" root for a NULL literals,
		// they receive the smallest precision possible for having little impact when finding a common type.
		familyToRoot.put(LogicalTypeFamily.NUMERIC, LogicalTypeRoot.TINYINT);
		familyToRoot.put(LogicalTypeFamily.INTEGER_NUMERIC, LogicalTypeRoot.TINYINT);
		familyToRoot.put(LogicalTypeFamily.EXACT_NUMERIC, LogicalTypeRoot.TINYINT);
		familyToRoot.put(LogicalTypeFamily.CHARACTER_STRING, LogicalTypeRoot.VARCHAR);
		familyToRoot.put(LogicalTypeFamily.BINARY_STRING, LogicalTypeRoot.VARBINARY);
		familyToRoot.put(LogicalTypeFamily.APPROXIMATE_NUMERIC, LogicalTypeRoot.DOUBLE);
		familyToRoot.put(LogicalTypeFamily.TIMESTAMP, LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE);
		familyToRoot.put(LogicalTypeFamily.TIME, LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE);
	}

	public FamilyArgumentTypeStrategy(LogicalTypeFamily expectedFamily, @Nullable Boolean expectedNullability) {
		this.expectedFamily = Preconditions.checkNotNull(expectedFamily);
		this.expectedNullability = expectedNullability;
	}

	@Override
	public Optional<DataType> inferArgumentType(CallContext callContext, int argumentPos, boolean throwOnFailure) {
		final DataType actualDataType = callContext.getArgumentDataTypes().get(argumentPos);
		final LogicalType actualType = actualDataType.getLogicalType();

		// a hack to make legacy types possible until we drop them
		if (actualType instanceof LegacyTypeInformationType) {
			return Optional.of(actualDataType);
		}

		if (Objects.equals(expectedNullability, Boolean.FALSE) && actualType.isNullable()) {
			if (throwOnFailure) {
				throw callContext.newValidationError(
					"Unsupported argument type. Expected nullable type of family '%s' but actual type was '%s'.",
					expectedFamily,
					actualType);
			}
			return Optional.empty();
		}

		// type is part of the family
		if (actualType.getTypeRoot().getFamilies().contains(expectedFamily)) {
			return Optional.of(actualDataType);
		}

		// find a type for the family
		final LogicalTypeRoot expectedRoot = familyToRoot.get(expectedFamily);
		final Optional<DataType> inferredDataType;
		if (expectedRoot == null) {
			inferredDataType = Optional.empty();
		} else {
			inferredDataType = findDataType(
				callContext,
				false,
				actualDataType,
				expectedRoot,
				expectedNullability);
		}
		if (!inferredDataType.isPresent() && throwOnFailure) {
			throw callContext.newValidationError(
					"Unsupported argument type. Expected type of family '%s' but actual type was '%s'.",
					expectedFamily,
					actualType);
		}
		return inferredDataType;
	}

	@Override
	public Signature.Argument getExpectedArgument(FunctionDefinition functionDefinition, int argumentPos) {
		// "< ... >" to indicate that this is not a type
		if (Objects.equals(expectedNullability, Boolean.TRUE)) {
			return Signature.Argument.of("<" + expectedFamily + " NULL>");
		} else if (Objects.equals(expectedNullability, Boolean.FALSE)) {
			return Signature.Argument.of("<" + expectedFamily + " NOT NULL>");
		}
		return Signature.Argument.of("<" + expectedFamily + ">");
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		FamilyArgumentTypeStrategy that = (FamilyArgumentTypeStrategy) o;
		return expectedFamily == that.expectedFamily &&
			Objects.equals(expectedNullability, that.expectedNullability);
	}

	@Override
	public int hashCode() {
		return Objects.hash(expectedFamily, expectedNullability);
	}
}
