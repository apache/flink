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
import org.apache.flink.table.types.inference.ConstantArgumentCount;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeGeneralization;
import org.apache.flink.table.types.utils.TypeConversions;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * {@link InputTypeStrategy} specific for {@link org.apache.flink.table.functions.BuiltInFunctionDefinitions#ARRAY}.
 *
 * <p>It expects at least one argument. All the arguments must have a common super type.
 */
@Internal
public class ArrayInputTypeStrategy implements InputTypeStrategy {
	@Override
	public ArgumentCount getArgumentCount() {
		return ConstantArgumentCount.from(1);
	}

	@Override
	public Optional<List<DataType>> inferInputTypes(
		CallContext callContext,
		boolean throwOnFailure) {
		List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();
		if (argumentDataTypes.size() == 0) {
			return Optional.empty();
		}

		Optional<LogicalType> commonType = LogicalTypeGeneralization.findCommonType(
			argumentDataTypes
				.stream()
				.map(DataType::getLogicalType)
				.collect(Collectors.toList())
		);

		return commonType.map(type -> Collections.nCopies(
			argumentDataTypes.size(),
			TypeConversions.fromLogicalToDataType(type)));
	}

	@Override
	public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
		return Collections.singletonList(Signature.of(Signature.Argument.of("*")));
	}
}
