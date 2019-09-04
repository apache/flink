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
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.InputTypeValidator;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A validator that passes if the arguments are of given {@link LogicalTypeRoot}.
 */
@Internal
public class TypeFamilyTypeValidator implements InputTypeValidator {

	private final List<LogicalTypeFamily> expectedTypeFamilies;

	public TypeFamilyTypeValidator(List<LogicalTypeFamily> expectedTypeFamilies) {
		this.expectedTypeFamilies = expectedTypeFamilies;
	}

	@Override
	public ArgumentCount getArgumentCount() {
		return ArgumentCount.exact(expectedTypeFamilies.size());
	}

	@Override
	public boolean validate(CallContext callContext, boolean throwOnFailure) {
		List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();
		for (int i = 0; i < expectedTypeFamilies.size(); i++) {
			LogicalType actualType = argumentDataTypes.get(i).getLogicalType();
			LogicalTypeFamily expectedTypeFamily = expectedTypeFamilies.get(i);
			if (!LogicalTypeChecks.hasFamily(actualType, expectedTypeFamily)) {
				return false;
			}
		}

		return true;
	}

	@Override
	public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
		return Collections.singletonList(Signature.of(
				expectedTypeFamilies.stream()
						.map(arg -> Signature.Argument.of(arg.toString()))
						.collect(Collectors.toList())));
	}
}
