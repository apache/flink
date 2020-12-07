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
import org.apache.flink.table.types.inference.Signature.Argument;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.table.types.inference.strategies.StrategyUtils.findDataType;

/**
 * Strategy for an argument that corresponds to a given {@link LogicalTypeRoot} and nullability.
 *
 * <p>Implicit casts will be inserted if possible.
 */
@Internal
public final class RootArgumentTypeStrategy implements ArgumentTypeStrategy {

	private final LogicalTypeRoot expectedRoot;

	private final @Nullable Boolean expectedNullability;

	public RootArgumentTypeStrategy(LogicalTypeRoot expectedRoot, @Nullable Boolean expectedNullability) {
		this.expectedRoot = Preconditions.checkNotNull(expectedRoot);
		this.expectedNullability = expectedNullability;
	}

	@Override
	public Optional<DataType> inferArgumentType(CallContext callContext, int argumentPos, boolean throwOnFailure) {
		final DataType actualDataType = callContext.getArgumentDataTypes().get(argumentPos);
		final LogicalType actualType = actualDataType.getLogicalType();

		if (Objects.equals(expectedNullability, Boolean.FALSE) && actualType.isNullable()) {
			if (throwOnFailure) {
				throw callContext.newValidationError(
					"Unsupported argument type. Expected nullable type of root '%s' but actual type was '%s'.",
					expectedRoot,
					actualType);
			}
			return Optional.empty();
		}

		return findDataType(
			callContext,
			throwOnFailure,
			actualDataType,
			expectedRoot,
			expectedNullability);
	}

	@Override
	public Argument getExpectedArgument(FunctionDefinition functionDefinition, int argumentPos) {
		// "< ... >" to indicate that this is not a type
		if (Objects.equals(expectedNullability, Boolean.TRUE)) {
			return Argument.of("<" + expectedRoot + " NULL>");
		} else if (Objects.equals(expectedNullability, Boolean.FALSE)) {
			return Argument.of("<" + expectedRoot + " NOT NULL>");
		}
		return Argument.of("<" + expectedRoot + ">");
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		RootArgumentTypeStrategy strategy = (RootArgumentTypeStrategy) o;
		return expectedRoot == strategy.expectedRoot &&
			Objects.equals(expectedNullability, strategy.expectedNullability);
	}

	@Override
	public int hashCode() {
		return Objects.hash(expectedRoot, expectedNullability);
	}
}
