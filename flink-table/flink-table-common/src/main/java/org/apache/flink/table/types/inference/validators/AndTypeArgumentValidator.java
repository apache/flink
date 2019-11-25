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

package org.apache.flink.table.types.inference.validators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.inference.ArgumentTypeValidator;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.util.Preconditions;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Validator that checks for a conjunction of multiple {@link ArgumentTypeValidator}s into one like
 * {@code f(NUMERIC && LITERAL)}.
 */
@Internal
public final class AndTypeArgumentValidator implements ArgumentTypeValidator {

	private final List<? extends ArgumentTypeValidator> validators;

	public AndTypeArgumentValidator(List<? extends ArgumentTypeValidator> validators) {
		Preconditions.checkArgument(validators.size() > 0);
		this.validators = validators;
	}

	@Override
	public boolean validateArgument(CallContext callContext, int argumentPos, boolean throwOnFailure) {
		for (ArgumentTypeValidator validator : validators) {
			if (!validator.validateArgument(callContext, argumentPos, throwOnFailure)) {
				return false;
			}
		}
		return true;
	}

	@Override
	public Signature.Argument getExpectedArgument(FunctionDefinition functionDefinition, int argumentPos) {
		final String argument = validators.stream()
			.map(v -> v.getExpectedArgument(functionDefinition, argumentPos).getType())
			.collect(Collectors.joining(" & ", "[", "]"));
		return Signature.Argument.of(argument);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		AndTypeArgumentValidator that = (AndTypeArgumentValidator) o;
		return Objects.equals(validators, that.validators);
	}

	@Override
	public int hashCode() {
		return Objects.hash(validators);
	}
}
