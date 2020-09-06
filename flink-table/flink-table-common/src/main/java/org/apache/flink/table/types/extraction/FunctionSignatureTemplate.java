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

package org.apache.flink.table.types.extraction;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.types.inference.ArgumentTypeStrategy;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.InputTypeStrategy;

import javax.annotation.Nullable;

import java.lang.reflect.Array;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.table.types.extraction.ExtractionUtils.extractionError;

/**
 * Template of a function signature with argument types and argument names.
 */
@Internal
final class FunctionSignatureTemplate {

	final List<FunctionArgumentTemplate> argumentTemplates;

	final boolean isVarArgs;

	final @Nullable String[] argumentNames;

	private FunctionSignatureTemplate(
			List<FunctionArgumentTemplate> argumentTemplates,
			boolean isVarArgs,
			@Nullable String[] argumentNames) {
		this.argumentTemplates = argumentTemplates;
		this.isVarArgs = isVarArgs;
		this.argumentNames = argumentNames;
	}

	static FunctionSignatureTemplate of(
			List<FunctionArgumentTemplate> argumentTemplates,
			boolean isVarArgs,
			@Nullable String[] argumentNames) {
		if (argumentNames != null && argumentNames.length != argumentTemplates.size()) {
			throw extractionError(
				"Mismatch between number of argument names '%s' and argument types '%s'.",
				argumentNames.length,
				argumentTemplates.size());
		}
		return new FunctionSignatureTemplate(argumentTemplates, isVarArgs, argumentNames);
	}

	InputTypeStrategy toInputTypeStrategy() {
		final ArgumentTypeStrategy[] argumentStrategies = argumentTemplates.stream()
			.map(FunctionArgumentTemplate::toArgumentTypeStrategy)
			.toArray(ArgumentTypeStrategy[]::new);

		final InputTypeStrategy strategy;
		if (isVarArgs) {
			if (argumentNames == null) {
				strategy = InputTypeStrategies.varyingSequence(argumentStrategies);
			} else {
				strategy = InputTypeStrategies.varyingSequence(
					argumentNames,
					argumentStrategies);
			}
		} else {
			if (argumentNames == null) {
				strategy = InputTypeStrategies.sequence(argumentStrategies);
			} else {
				strategy = InputTypeStrategies.sequence(argumentNames, argumentStrategies);
			}
		}
		return strategy;
	}

	List<Class<?>> toClass() {
		return IntStream.range(0, argumentTemplates.size())
			.mapToObj(i -> {
				final Class<?> clazz = argumentTemplates.get(i).toConversionClass();
				if (i == argumentTemplates.size() - 1 && isVarArgs) {
					return Array.newInstance(clazz, 0).getClass();
				}
				return clazz;
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
		FunctionSignatureTemplate that = (FunctionSignatureTemplate) o;
		// argument names are not part of equality
		return isVarArgs == that.isVarArgs &&
			argumentTemplates.equals(that.argumentTemplates);
	}

	@Override
	public int hashCode() {
		// argument names are not part of equality
		return Objects.hash(argumentTemplates, isVarArgs);
	}
}
