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

import java.util.Arrays;
import java.util.List;

/**
 * Table operation that joins two relational operations based on given condition.
 */
@Internal
public class JoinTableOperation implements TableOperation {
	private final TableOperation left;
	private final TableOperation right;
	private final JoinType joinType;
	private final Expression condition;
	private final boolean correlated;
	private final TableSchema tableSchema;

	/**
	 * Specifies how the two Tables should be joined.
	 */
	public enum JoinType {
		INNER,
		LEFT_OUTER,
		RIGHT_OUTER,
		FULL_OUTER
	}

	public JoinTableOperation(
			TableOperation left,
			TableOperation right,
			JoinType joinType,
			Expression condition,
			boolean correlated) {
		this.left = left;
		this.right = right;
		this.joinType = joinType;
		this.condition = condition;
		this.correlated = correlated;

		this.tableSchema = calculateResultingSchema(left, right);
	}

	private TableSchema calculateResultingSchema(TableOperation left, TableOperation right) {
		TableSchema leftSchema = left.getTableSchema();
		TableSchema rightSchema = right.getTableSchema();
		int resultingSchemaSize = leftSchema.getFieldCount() + rightSchema.getFieldCount();
		String[] newFieldNames = new String[resultingSchemaSize];
		System.arraycopy(leftSchema.getFieldNames(), 0, newFieldNames, 0, leftSchema.getFieldCount());
		System.arraycopy(
			rightSchema.getFieldNames(),
			0,
			newFieldNames,
			leftSchema.getFieldCount(),
			rightSchema.getFieldCount());

		TypeInformation[] newFieldTypes = new TypeInformation[resultingSchemaSize];

		System.arraycopy(leftSchema.getFieldTypes(), 0, newFieldTypes, 0, leftSchema.getFieldCount());
		System.arraycopy(
			rightSchema.getFieldTypes(),
			0,
			newFieldTypes,
			leftSchema.getFieldCount(),
			rightSchema.getFieldCount());
		return new TableSchema(newFieldNames, newFieldTypes);
	}

	public JoinType getJoinType() {
		return joinType;
	}

	public Expression getCondition() {
		return condition;
	}

	public boolean isCorrelated() {
		return correlated;
	}

	@Override
	public TableSchema getTableSchema() {
		return tableSchema;
	}

	@Override
	public List<TableOperation> getChildren() {
		return Arrays.asList(left, right);
	}

	@Override
	public <T> T accept(TableOperationVisitor<T> visitor) {
		return visitor.visitJoin(this);
	}
}
