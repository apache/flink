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

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * Strategy for an argument that must fulfill a given constraint.
 */
@Internal
public final class ConstraintArgumentTypeStrategy implements ArgumentTypeStrategy {

	private final String constraintMessage;

	private final Function<List<DataType>, Boolean> evaluator;

	public ConstraintArgumentTypeStrategy(
			String constraintMessage,
			Function<List<DataType>, Boolean> evaluator) {
		this.constraintMessage = constraintMessage;
		this.evaluator = evaluator;
	}

	@Override
	public Optional<DataType> inferArgumentType(CallContext callContext, int argumentPos, boolean throwOnFailure) {
		final List<DataType> actualDataTypes = callContext.getArgumentDataTypes();

		// type fulfills constraint
		if (evaluator.apply(actualDataTypes)) {
			return Optional.of(actualDataTypes.get(argumentPos));
		}

		if (throwOnFailure) {
			throw callContext.newValidationError(
				constraintMessage,
				actualDataTypes.toArray());
		}
		return Optional.empty();
	}

	@Override
	public Argument getExpectedArgument(FunctionDefinition functionDefinition, int argumentPos) {
		return Argument.of("<CONSTRAINT>");
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		ConstraintArgumentTypeStrategy that = (ConstraintArgumentTypeStrategy) o;
		return constraintMessage.equals(that.constraintMessage) && evaluator.equals(that.evaluator);
	}

	@Override
	public int hashCode() {
		return Objects.hash(constraintMessage, evaluator);
	}
}
