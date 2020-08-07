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

package org.apache.flink.table.operations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.functions.TableFunction;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Describes a relational operation that was created from applying a {@link TableFunction}.
 */
@Internal
public class CalculatedQueryOperation implements QueryOperation {

	private final FunctionDefinition functionDefinition;
	private final @Nullable FunctionIdentifier functionIdentifier;
	private final List<ResolvedExpression> arguments;
	private final TableSchema tableSchema;

	public CalculatedQueryOperation(
			FunctionDefinition functionDefinition,
			@Nullable FunctionIdentifier functionIdentifier,
			List<ResolvedExpression> arguments,
			TableSchema tableSchema) {
		this.functionDefinition = functionDefinition;
		this.functionIdentifier = functionIdentifier;
		this.arguments = arguments;
		this.tableSchema = tableSchema;
	}

	public FunctionDefinition getFunctionDefinition() {
		return functionDefinition;
	}

	public Optional<FunctionIdentifier> getFunctionIdentifier() {
		return Optional.ofNullable(functionIdentifier);
	}

	public List<ResolvedExpression> getArguments() {
		return arguments;
	}

	@Override
	public TableSchema getTableSchema() {
		return tableSchema;
	}

	@Override
	public String asSummaryString() {
		Map<String, Object> args = new LinkedHashMap<>();
		if (functionIdentifier != null) {
			args.put("function", functionIdentifier);
		} else {
			args.put("function", functionDefinition.toString());
		}
		args.put("arguments", arguments);

		return OperationUtils.formatWithChildren("CalculatedTable", args, getChildren(), Operation::asSummaryString);
	}

	@Override
	public List<QueryOperation> getChildren() {
		return Collections.emptyList();
	}

	@Override
	public <U> U accept(QueryOperationVisitor<U> visitor) {
		return visitor.visit(this);
	}
}
