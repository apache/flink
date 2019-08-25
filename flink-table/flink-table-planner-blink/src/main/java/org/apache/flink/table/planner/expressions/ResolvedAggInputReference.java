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

package org.apache.flink.table.planner.expressions;

import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionVisitor;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.List;

/**
 * Normally we should use {@link FieldReferenceExpression} to represent an input field.
 * {@link FieldReferenceExpression} uses name to locate the field, in aggregate case, we want to use
 * field index.
 */
public class ResolvedAggInputReference implements Expression {

	private final String name;
	private final int index;
	private final LogicalType resultType;

	public ResolvedAggInputReference(String name, int index, LogicalType resultType) {
		this.name = Preconditions.checkNotNull(name);
		this.index = index;
		this.resultType = resultType;
	}

	public String getName() {
		return name;
	}

	public int getIndex() {
		return index;
	}

	public LogicalType getResultType() {
		return resultType;
	}

	@Override
	public String asSummaryString() {
		return name;
	}

	@Override
	public List<Expression> getChildren() {
		return Collections.emptyList();
	}

	@Override
	public <R> R accept(ExpressionVisitor<R> visitor) {
		return visitor.visit(this);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		ResolvedAggInputReference that = (ResolvedAggInputReference) o;
		return index == that.index && name.equals(that.name) && resultType.equals(that.resultType);
	}

	@Override
	public int hashCode() {
		int result = name.hashCode();
		result = 31 * result + index;
		result = 31 * result + resultType.hashCode();
		return result;
	}

	@Override
	public String toString() {
		return asSummaryString();
	}
}
