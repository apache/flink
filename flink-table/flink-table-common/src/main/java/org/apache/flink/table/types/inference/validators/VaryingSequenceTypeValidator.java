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
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.ConstantArgumentCount;
import org.apache.flink.table.types.inference.InputTypeValidator;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.inference.validators.CompositeTypeValidator.Composition;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A varying sequence of {@link SingleInputTypeValidator}s for validating an entire function signature
 * like {@code f(INT, STRING, NUMERIC...)}. The first n - 1 arguments must be constant and are validated
 * according to {@link Composition#SEQUENCE}. The n-th argument can occur 0, 1, or more times.
 */
@Internal
public final class VaryingSequenceTypeValidator implements InputTypeValidator {

	private final int constantArgumentCount;

	private final SingleInputTypeValidator constantSequence;

	private final SingleInputTypeValidator varyingValidator;

	private final @Nullable String varyingArgumentName;

	public VaryingSequenceTypeValidator(
			List<SingleInputTypeValidator> validators,
			@Nullable List<String> argumentNames) {
		Preconditions.checkArgument(validators.size() > 0);
		Preconditions.checkArgument(argumentNames == null || argumentNames.size() == validators.size());
		constantArgumentCount = validators.size() - 1;
		final List<SingleInputTypeValidator> constantValidators = validators.subList(0, constantArgumentCount);
		if (argumentNames == null) {
			constantSequence = new CompositeTypeValidator(
				Composition.SEQUENCE,
				constantValidators,
				null);
			varyingArgumentName = null;
		} else {
			constantSequence =  new CompositeTypeValidator(
				Composition.SEQUENCE,
				constantValidators,
				argumentNames.subList(0, constantArgumentCount));
			varyingArgumentName = argumentNames.get(constantArgumentCount);
		}
		varyingValidator = validators.get(constantArgumentCount);
	}

	@Override
	public ArgumentCount getArgumentCount() {
		return ConstantArgumentCount.from(constantArgumentCount); // at least the constant part
	}

	@Override
	public boolean validate(CallContext callContext, boolean throwOnFailure) {
		for (int i = 0; i < callContext.getArgumentDataTypes().size(); i++) {
			if (i < constantArgumentCount) {
				if (!constantSequence.validateArgument(callContext, i, i, throwOnFailure)) {
					return false;
				}
			} else {
				if (!varyingValidator.validateArgument(callContext, i, 0, throwOnFailure)) {
					return false;
				}
			}
		}
		return true;
	}

	@Override
	public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
		final List<Signature> signatures = varyingValidator.getExpectedSignatures(definition);
		final String type;
		if (signatures.size() > 1) {
			type = signatures.stream()
				.map(s -> s.getArguments().get(0).getType())
				.collect(Collectors.joining(" | ", "[", "]"));
		} else {
			type = signatures.get(0).getArguments().get(0).getType();
		}
		final Signature.Argument newArg;
		if (varyingArgumentName == null) {
			newArg = Signature.Argument.of(type + "...");
		} else {
			newArg = Signature.Argument.of(varyingArgumentName, type + "...");
		}

		final List<Signature> constantSignature = constantSequence.getExpectedSignatures(definition);
		return constantSignature.stream()
			.map(s -> {
				final List<Signature.Argument> newArgs =
					Stream.concat(s.getArguments().stream(), Stream.of(newArg)).collect(Collectors.toList());
				return Signature.of(newArgs);
			})
			.collect(Collectors.toList());
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
			constantSequence.equals(that.constantSequence) &&
			varyingValidator.equals(that.varyingValidator) &&
			Objects.equals(varyingArgumentName, that.varyingArgumentName);
	}

	@Override
	public int hashCode() {
		return Objects.hash(
			constantArgumentCount,
			constantSequence,
			varyingValidator,
			varyingArgumentName);
	}
}
