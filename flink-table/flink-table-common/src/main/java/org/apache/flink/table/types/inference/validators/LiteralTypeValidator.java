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
import org.apache.flink.table.types.inference.ArgumentCount;
import org.apache.flink.table.types.inference.ArgumentTypeValidator;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.ConstantArgumentCount;
import org.apache.flink.table.types.inference.InputTypeValidator;
import org.apache.flink.table.types.inference.Signature;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Validator that checks if a single argument is a literal.
 */
@Internal
public final class LiteralTypeValidator implements ArgumentTypeValidator, InputTypeValidator {

	private final boolean allowNull;

	public LiteralTypeValidator(boolean allowNull) {
		this.allowNull = allowNull;
	}

	@Override
	public boolean validateArgument(CallContext callContext, int argumentPos, boolean throwOnFailure) {
		if (!callContext.isArgumentLiteral(argumentPos)) {
			if (throwOnFailure) {
				throw callContext.newValidationError("Literal expected.");
			}
			return false;
		}
		if (callContext.isArgumentNull(argumentPos) && !allowNull) {
			if (throwOnFailure) {
				throw callContext.newValidationError("Literal must not be NULL.");
			}
			return false;
		}
		return true;
	}

	@Override
	public Signature.Argument getExpectedArgument(FunctionDefinition functionDefinition, int argumentPos) {
		return Signature.Argument.of("<LITERAL>");
	}

	@Override
	public ArgumentCount getArgumentCount() {
		return ConstantArgumentCount.of(1);
	}

	@Override
	public boolean validate(CallContext callContext, boolean throwOnFailure) {
		return validateArgument(callContext, 0, throwOnFailure);
	}

	@Override
	public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
		return Collections.singletonList(Signature.of(getExpectedArgument(definition, 0)));
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		LiteralTypeValidator that = (LiteralTypeValidator) o;
		return allowNull == that.allowNull;
	}

	@Override
	public int hashCode() {
		return Objects.hash(allowNull);
	}
}
