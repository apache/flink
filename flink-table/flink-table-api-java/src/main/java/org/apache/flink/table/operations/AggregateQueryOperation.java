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
import org.apache.flink.table.expressions.Expression;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Relational operation that performs computations on top of subsets of input rows grouped by
 * key.
 */
@Internal
public class AggregateQueryOperation implements QueryOperation {

	private final List<Expression> groupingExpressions;
	private final List<Expression> aggregateExpressions;
	private final QueryOperation child;
	private final TableSchema tableSchema;

	public AggregateQueryOperation(
			List<Expression> groupingExpressions,
			List<Expression> aggregateExpressions,
			QueryOperation child,
			TableSchema tableSchema) {
		this.groupingExpressions = groupingExpressions;
		this.aggregateExpressions = aggregateExpressions;
		this.child = child;
		this.tableSchema = tableSchema;
	}

	@Override
	public TableSchema getTableSchema() {
		return tableSchema;
	}

	@Override
	public String asSummaryString() {
		Map<String, Object> args = new LinkedHashMap<>();
		args.put("group", groupingExpressions);
		args.put("agg", aggregateExpressions);

		return OperationUtils.formatWithChildren("Aggregate", args, getChildren(), Operation::asSummaryString);
	}

	public List<Expression> getGroupingExpressions() {
		return groupingExpressions;
	}

	public List<Expression> getAggregateExpressions() {
		return aggregateExpressions;
	}

	@Override
	public List<QueryOperation> getChildren() {
		return Collections.singletonList(child);
	}

	@Override
	public <T> T accept(QueryOperationVisitor<T> visitor) {
		return visitor.visit(this);
	}
}
