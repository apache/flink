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

package org.apache.flink.table.types.inference.strategies;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentTypeStrategy;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.Signature.Argument;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeCasts;
import org.apache.flink.util.Preconditions;

import java.util.Objects;
import java.util.Optional;

/**
 * Strategy for an argument that corresponds to an explicitly defined type. Implicit casts will be
 * inserted if possible.
 */
@Internal
public final class ExplicitArgumentTypeStrategy implements ArgumentTypeStrategy {

	private final DataType expectedDataType;

	public ExplicitArgumentTypeStrategy(DataType expectedDataType) {
		this.expectedDataType = Preconditions.checkNotNull(expectedDataType);
	}

	@Override
	public Optional<DataType> inferArgumentType(CallContext callContext, int argumentPos, boolean throwOnFailure) {
		final LogicalType expectedType = expectedDataType.getLogicalType();
		final LogicalType actualType = callContext.getArgumentDataTypes().get(argumentPos).getLogicalType();
		// if logical types match, we return the expected data type
		// for ensuring the expected conversion class
		if (expectedType.equals(actualType)) {
			return Optional.of(expectedDataType);
		}
		// type coercion
		if (!LogicalTypeCasts.supportsImplicitCast(actualType, expectedType)) {
			if (throwOnFailure) {
				throw callContext.newValidationError(
					"Unsupported argument type. Expected type '%s' but actual type was '%s'.",
					expectedType,
					actualType);
			}
			return Optional.empty();
		}
		return Optional.of(expectedDataType);
	}

	@Override
	public Argument getExpectedArgument(FunctionDefinition functionDefinition, int argumentPos) {
		return Argument.of(expectedDataType.toString());
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		ExplicitArgumentTypeStrategy that = (ExplicitArgumentTypeStrategy) o;
		return Objects.equals(expectedDataType, that.expectedDataType);
	}

	@Override
	public int hashCode() {
		return Objects.hash(expectedDataType);
	}
}
