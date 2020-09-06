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
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentCount;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeMerging;
import org.apache.flink.table.types.utils.TypeConversions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * {@link InputTypeStrategy} specific for {@link BuiltInFunctionDefinitions#MAP}.
 *
 * <p>It expects at least two arguments. There must be even number of arguments.
 * All the keys and values must have a common super type respectively.
 */
@Internal
public final class MapInputTypeStrategy implements InputTypeStrategy {

	private static final ArgumentCount AT_LEAST_TWO_EVEN = new ArgumentCount() {
		@Override
		public boolean isValidCount(int count) {
			return count % 2 == 0;
		}

		@Override
		public Optional<Integer> getMinCount() {
			return Optional.of(2);
		}

		@Override
		public Optional<Integer> getMaxCount() {
			return Optional.empty();
		}
	};

	@Override
	public ArgumentCount getArgumentCount() {
		return AT_LEAST_TWO_EVEN;
	}

	@Override
	public Optional<List<DataType>> inferInputTypes(CallContext callContext, boolean throwOnFailure) {
		List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();
		if (argumentDataTypes.size() == 0) {
			return Optional.empty();
		}

		List<LogicalType> keyTypes = new ArrayList<>();
		List<LogicalType> valueTypes = new ArrayList<>();

		for (int i = 0; i < argumentDataTypes.size(); i++) {
			LogicalType logicalType = argumentDataTypes.get(i).getLogicalType();
			if (i % 2 == 0) {
				keyTypes.add(logicalType);
			} else {
				valueTypes.add(logicalType);
			}
		}
		Optional<LogicalType> commonKeyType = LogicalTypeMerging.findCommonType(keyTypes);
		Optional<LogicalType> commonValueType = LogicalTypeMerging.findCommonType(valueTypes);

		if (!commonKeyType.isPresent() || !commonValueType.isPresent()) {
			return Optional.empty();
		}

		DataType keyType = TypeConversions.fromLogicalToDataType(commonKeyType.get());
		DataType valueType = TypeConversions.fromLogicalToDataType(commonValueType.get());
		return Optional.of(IntStream.range(0, argumentDataTypes.size())
			.mapToObj(idx -> {
				if (idx % 2 == 0) {
					return keyType;
				} else {
					return valueType;
				}
			})
			.collect(Collectors.toList()));
	}

	@Override
	public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
		return Collections.singletonList(Signature.of(Signature.Argument.of("*")));
	}
}
