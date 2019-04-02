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
import java.util.List;

/**
 * Relational operation that performs computations on top of subsets of input rows grouped by
 * key.
 */
@Internal
public class AggregateTableOperation implements TableOperation {

	private final List<Expression> groupingExpressions;
	private final List<Expression> aggregateExpressions;
	private final TableOperation child;
	private final TableSchema tableSchema;

	public AggregateTableOperation(
			List<Expression> groupingExpressions,
			List<Expression> aggregateExpressions,
			TableOperation child,
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

	public List<Expression> getGroupingExpressions() {
		return groupingExpressions;
	}

	public List<Expression> getAggregateExpressions() {
		return aggregateExpressions;
	}

	@Override
	public List<TableOperation> getChildren() {
		return Collections.singletonList(child);
	}

	@Override
	public <T> T accept(TableOperationVisitor<T> visitor) {
		return visitor.visitAggregate(this);
	}
}
