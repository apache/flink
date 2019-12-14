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
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A varying sequence of {@link ArgumentTypeValidator}s for validating an entire function signature
 * like {@code f(INT, STRING, NUMERIC...)}. The first n - 1 arguments must be constant. The n-th
 * argument can occur 0, 1, or more times.
 */
@Internal
public final class VaryingSequenceTypeValidator implements InputTypeValidator {

	private final int constantArgumentCount;

	private final List<ArgumentTypeValidator> constantValidators;

	private final ArgumentTypeValidator varyingValidator;

	private final @Nullable List<String> argumentNames;

	public VaryingSequenceTypeValidator(
			List<ArgumentTypeValidator> validators,
			@Nullable List<String> argumentNames) {
		Preconditions.checkArgument(validators.size() > 0);
		Preconditions.checkArgument(argumentNames == null || argumentNames.size() == validators.size());
		constantArgumentCount = validators.size() - 1;
		constantValidators = validators.subList(0, constantArgumentCount);
		varyingValidator = validators.get(constantArgumentCount);
		this.argumentNames = argumentNames;
	}

	@Override
	public ArgumentCount getArgumentCount() {
		return ConstantArgumentCount.from(constantArgumentCount); // at least the constant part
	}

	@Override
	public boolean validate(CallContext callContext, boolean throwOnFailure) {
		for (int i = 0; i < callContext.getArgumentDataTypes().size(); i++) {
			if (i < constantArgumentCount) {
				if (!constantValidators.get(i).validateArgument(callContext, i, throwOnFailure)) {
					return false;
				}
			} else {
				if (!varyingValidator.validateArgument(callContext, i, throwOnFailure)) {
					return false;
				}
			}
		}
		return true;
	}

	@Override
	public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
		final Signature.Argument varyingArgument = varyingValidator.getExpectedArgument(
			definition,
			constantValidators.size());
		final Signature.Argument newArg;
		final String type = varyingArgument.getType();
		if (argumentNames == null) {
			newArg = Signature.Argument.of(type + "...");
		} else {
			newArg = Signature.Argument.of(argumentNames.get(argumentNames.size() - 1), type + "...");
		}

		final List<Signature.Argument> arguments = new ArrayList<>();
		for (int i = 0; i < constantValidators.size(); i++) {
			if (argumentNames == null) {
				arguments.add(constantValidators.get(i).getExpectedArgument(definition, i));
			} else {
				arguments.add(Signature.Argument.of(
					argumentNames.get(i),
					constantValidators.get(i).getExpectedArgument(definition, i).getType()));
			}
		}

		arguments.add(newArg);

		return Collections.singletonList(Signature.of(arguments));
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		VaryingSequenceTypeValidator that = (VaryingSequenceTypeValidator) o;
		return constantArgumentCount == that.constantArgumentCount &&
			Objects.equals(constantValidators, that.constantValidators) &&
			Objects.equals(varyingValidator, that.varyingValidator) &&
			Objects.equals(argumentNames, that.argumentNames);
	}

	@Override
	public int hashCode() {
		return Objects.hash(constantArgumentCount, constantValidators, varyingValidator, argumentNames);
	}
}
