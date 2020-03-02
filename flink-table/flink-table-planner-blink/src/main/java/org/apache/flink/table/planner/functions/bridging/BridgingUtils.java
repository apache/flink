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

package org.apache.flink.table.planner.functions.bridging;

import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.functions.inference.TypeInferenceOperandChecker;
import org.apache.flink.table.planner.functions.inference.TypeInferenceOperandInference;
import org.apache.flink.table.planner.functions.inference.TypeInferenceReturnInference;
import org.apache.flink.table.types.inference.TypeInference;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;

import javax.annotation.Nullable;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Utilities for bridging {@link FunctionDefinition} with Calcite's representation of functions.
 */
final class BridgingUtils {

	static String createName(FunctionIdentifier identifier) {
		if (identifier.getSimpleName().isPresent()) {
			return identifier.getSimpleName().get();
		}
		return identifier.getIdentifier()
			.map(ObjectIdentifier::getObjectName)
			.orElseThrow(IllegalStateException::new);
	}

	static @Nullable SqlIdentifier createSqlIdentifier(FunctionIdentifier identifier) {
		return identifier.getIdentifier()
			.map(i -> new SqlIdentifier(i.toList(), SqlParserPos.ZERO))
			.orElse(null); // null indicates a built-in system function
	}

	static SqlReturnTypeInference createSqlReturnTypeInference(
			DataTypeFactory dataTypeFactory,
			FunctionDefinition definition,
			TypeInference typeInference) {
		return new TypeInferenceReturnInference(dataTypeFactory, definition, typeInference);
	}

	static SqlOperandTypeInference createSqlOperandTypeInference(
			DataTypeFactory dataTypeFactory,
			FunctionDefinition definition,
			TypeInference typeInference) {
		return new TypeInferenceOperandInference(dataTypeFactory, definition, typeInference);
	}

	static SqlOperandTypeChecker createSqlOperandTypeChecker(
			DataTypeFactory dataTypeFactory,
			FunctionDefinition definition,
			TypeInference typeInference) {
		return new TypeInferenceOperandChecker(dataTypeFactory, definition, typeInference);
	}

	static @Nullable List<RelDataType> createParamTypes(
			FlinkTypeFactory typeFactory,
			TypeInference typeInference) {
		return typeInference.getTypedArguments()
			.map(dataTypes ->
				dataTypes.stream()
					.map(dataType -> typeFactory.createFieldTypeFromLogicalType(dataType.getLogicalType()))
					.collect(Collectors.toList()))
			.orElse(null);
	}

	static SqlFunctionCategory createSqlFunctionCategory(FunctionIdentifier identifier) {
		if (identifier.getSimpleName().isPresent()) {
			return SqlFunctionCategory.SYSTEM;
		}
		return SqlFunctionCategory.USER_DEFINED_FUNCTION;
	}

	private BridgingUtils() {
		// no instantiation
	}
}
