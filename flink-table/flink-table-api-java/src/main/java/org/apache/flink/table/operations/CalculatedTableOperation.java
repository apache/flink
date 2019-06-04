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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.functions.TableFunction;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Describes a relational operation that was created from applying a {@link TableFunction}.
 */
@Internal
public class CalculatedTableOperation<T> extends TableOperation {

	private final TableFunction<T> tableFunction;
	private final List<Expression> parameters;
	private final TypeInformation<T> resultType;
	private final TableSchema tableSchema;

	public CalculatedTableOperation(
			TableFunction<T> tableFunction,
			List<Expression> parameters,
			TypeInformation<T> resultType,
			TableSchema tableSchema) {
		this.tableFunction = tableFunction;
		this.parameters = parameters;
		this.resultType = resultType;
		this.tableSchema = tableSchema;
	}

	public TableFunction<T> getTableFunction() {
		return tableFunction;
	}

	public List<Expression> getParameters() {
		return parameters;
	}

	public TypeInformation<T> getResultType() {
		return resultType;
	}

	@Override
	public TableSchema getTableSchema() {
		return tableSchema;
	}

	@Override
	public String asSummaryString() {
		Map<String, Object> args = new LinkedHashMap<>();
		args.put("function", tableFunction);
		args.put("parameters", parameters);

		return formatWithChildren("CalculatedTable", args);
	}

	@Override
	public List<TableOperation> getChildren() {
		return Collections.emptyList();
	}

	@Override
	public <U> U accept(TableOperationVisitor<U> visitor) {
		return visitor.visitCalculatedTable(this);
	}
}
