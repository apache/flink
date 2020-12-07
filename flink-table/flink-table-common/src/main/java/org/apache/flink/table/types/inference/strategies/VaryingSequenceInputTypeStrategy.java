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
import org.apache.flink.table.types.inference.ArgumentCount;
import org.apache.flink.table.types.inference.ArgumentTypeStrategy;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.ConstantArgumentCount;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Strategy for inferring and validating a varying function signature like {@code f(INT, STRING, NUMERIC...)}
 * or {@code f(i INT, str STRING, num NUMERIC...)} using a sequence of {@link ArgumentTypeStrategy}s. The
 * first n - 1 arguments must be constant. The n-th argument can occur 0, 1, or more times.
 */
@Internal
public final class VaryingSequenceInputTypeStrategy implements InputTypeStrategy {

	private final int constantArgumentCount;

	private final List<ArgumentTypeStrategy> constantArgumentStrategies;

	private final ArgumentTypeStrategy varyingArgumentStrategy;

	private final @Nullable List<String> argumentNames;

	public VaryingSequenceInputTypeStrategy(
			List<ArgumentTypeStrategy> argumentStrategies,
			@Nullable List<String> argumentNames) {
		Preconditions.checkArgument(argumentStrategies.size() > 0);
		Preconditions.checkArgument(argumentNames == null || argumentNames.size() == argumentStrategies.size());
		constantArgumentCount = argumentStrategies.size() - 1;
		constantArgumentStrategies = argumentStrategies.subList(0, constantArgumentCount);
		varyingArgumentStrategy = argumentStrategies.get(constantArgumentCount);
		this.argumentNames = argumentNames;
	}

	@Override
	public ArgumentCount getArgumentCount() {
		return ConstantArgumentCount.from(constantArgumentCount);
	}

	@Override
	public Optional<List<DataType>> inferInputTypes(CallContext callContext, boolean throwOnFailure) {
		final List<DataType> dataTypes = callContext.getArgumentDataTypes();
		if (dataTypes.size() < constantArgumentCount) {
			return Optional.empty();
		}
		final List<DataType> inferredDataTypes = new ArrayList<>(dataTypes.size());
		for (int i = 0; i < callContext.getArgumentDataTypes().size(); i++) {
			final ArgumentTypeStrategy argumentTypeStrategy;
			if (i < constantArgumentCount) {
				argumentTypeStrategy = constantArgumentStrategies.get(i);
			} else {
				argumentTypeStrategy = varyingArgumentStrategy;
			}
			final Optional<DataType> inferredDataType = argumentTypeStrategy.inferArgumentType(
				callContext,
				i,
				throwOnFailure);
			if (!inferredDataType.isPresent()) {
				return Optional.empty();
			}
			inferredDataTypes.add(inferredDataType.get());
		}
		return Optional.of(inferredDataTypes);
	}

	@Override
	public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
		final Signature.Argument varyingArgument = varyingArgumentStrategy.getExpectedArgument(
			definition,
			constantArgumentCount);
		final Signature.Argument newArg;
		final String type = varyingArgument.getType();
		if (argumentNames == null) {
			newArg = Signature.Argument.of(type + "...");
		} else {
			newArg = Signature.Argument.of(argumentNames.get(constantArgumentCount), type + "...");
		}

		final List<Signature.Argument> arguments = new ArrayList<>();
		for (int i = 0; i < constantArgumentCount; i++) {
			if (argumentNames == null) {
				arguments.add(constantArgumentStrategies.get(i).getExpectedArgument(definition, i));
			} else {
				arguments.add(Signature.Argument.of(
					argumentNames.get(i),
					constantArgumentStrategies.get(i).getExpectedArgument(definition, i).getType()));
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
		VaryingSequenceInputTypeStrategy that = (VaryingSequenceInputTypeStrategy) o;
		return constantArgumentCount == that.constantArgumentCount &&
			Objects.equals(constantArgumentStrategies, that.constantArgumentStrategies) &&
			Objects.equals(varyingArgumentStrategy, that.varyingArgumentStrategy) &&
			Objects.equals(argumentNames, that.argumentNames);
	}

	@Override
	public int hashCode() {
		return Objects.hash(
			constantArgumentCount,
			constantArgumentStrategies,
			varyingArgumentStrategy,
			argumentNames);
	}
}
