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
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.logical.LegacyTypeInformationType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeGeneralization;
import org.apache.flink.table.types.utils.TypeConversions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * An {@link InputTypeStrategy} that expects that all arguments have a common type.
 */
@Internal
public final class SameArgumentsInputTypeStrategy implements InputTypeStrategy {
	private static final Signature.Argument COMMON_ARGUMENT = Signature.Argument.of("<COMMON>");
	private final ArgumentCount argumentCount;

	public SameArgumentsInputTypeStrategy(ArgumentCount argumentCount) {
		this.argumentCount = argumentCount;
	}

	@Override
	public ArgumentCount getArgumentCount() {
		return argumentCount;
	}

	@Override
	public Optional<List<DataType>> inferInputTypes(
			CallContext callContext,
			boolean throwOnFailure) {
		List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();

		if (callContext.getArgumentDataTypes()
				.stream()
				.anyMatch(dataType -> dataType.getLogicalType() instanceof LegacyTypeInformationType)) {
			return Optional.of(callContext.getArgumentDataTypes());
		}

		Optional<LogicalType> commonType = LogicalTypeGeneralization.findCommonType(
			argumentDataTypes
				.stream()
				.map(DataType::getLogicalType)
				.collect(Collectors.toList())
		);

		if (!commonType.isPresent()) {
			throw callContext.newValidationError("Could not find a common type for arguments: %s", argumentDataTypes);
		}

		return commonType.map(type -> Collections.nCopies(
			argumentDataTypes.size(),
			TypeConversions.fromLogicalToDataType(type)));
	}

	@Override
	public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
		Optional<Integer> minCount = argumentCount.getMinCount();
		Optional<Integer> maxCount = argumentCount.getMaxCount();

		int numberOfMandatoryArguments = 0;
		if (minCount.isPresent()) {
			numberOfMandatoryArguments = minCount.get();
		}

		if (maxCount.isPresent()) {
			List<Signature> signatures = new ArrayList<>();
			IntStream.range(numberOfMandatoryArguments, maxCount.get() + 1)
				.forEach(count -> {
						signatures.add(Signature.of(Collections.nCopies(count, COMMON_ARGUMENT)));
					}
				);
			return signatures;
		}

		List<Signature.Argument> arguments = new ArrayList<>(
			Collections.nCopies(
				numberOfMandatoryArguments,
				COMMON_ARGUMENT));
		arguments.add(Signature.Argument.of("<COMMON>..."));
		return Collections.singletonList(Signature.of(arguments));
	}
}
