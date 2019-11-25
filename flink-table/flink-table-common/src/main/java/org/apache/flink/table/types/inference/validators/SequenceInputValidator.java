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
import org.apache.flink.table.types.DataType;
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
 * Validator that checks for a sequence of {@link ArgumentTypeValidator}s for validating an entire
 * function signature like {@code f(STRING, NUMERIC)} or {@code f(s STRING, n NUMERIC)}.
 */
@Internal
public final class SequenceInputValidator implements InputTypeValidator {

	private final List<? extends ArgumentTypeValidator> validators;

	private final @Nullable List<String> argumentNames;

	public SequenceInputValidator(
			List<? extends ArgumentTypeValidator> validators,
			@Nullable List<String> argumentNames) {
		Preconditions.checkArgument(validators.size() > 0);
		Preconditions.checkArgument(argumentNames == null || argumentNames.size() == validators.size());
		this.validators = validators;
		this.argumentNames = argumentNames;
	}

	@Override
	public ArgumentCount getArgumentCount() {
		return ConstantArgumentCount.of(validators.size());
	}

	@Override
	public boolean validate(CallContext callContext, boolean throwOnFailure) {
		final List<DataType> dataTypes = callContext.getArgumentDataTypes();
		if (dataTypes.size() != validators.size()) {
			return false;
		}
		for (int i = 0; i < validators.size(); i++) {
			final ArgumentTypeValidator validator = validators.get(i);
			if (!validator.validateArgument(callContext, i, throwOnFailure)) {
				return false;
			}
		}
		return true;
	}

	@Override
	public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
		final List<Signature.Argument> arguments = new ArrayList<>();
		for (int i = 0; i < validators.size(); i++) {
			if (argumentNames == null) {
				arguments.add(validators.get(i).getExpectedArgument(definition, i));
			} else {
				arguments.add(Signature.Argument.of(
					argumentNames.get(i),
					validators.get(i).getExpectedArgument(definition, i).getType()));
			}
		}

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
		SequenceInputValidator that = (SequenceInputValidator) o;
		return Objects.equals(validators, that.validators) &&
			Objects.equals(argumentNames, that.argumentNames);
	}

	@Override
	public int hashCode() {
		return Objects.hash(validators, argumentNames);
	}
}
