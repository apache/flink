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
import org.apache.flink.table.types.logical.LogicalType;

import java.util.Collections;
import java.util.List;

/**
 * Special reference which represent a local filed, such as aggregate buffers or constants.
 * We are stored as class members, so the field can be referenced directly.
 * We should use an unique name to locate the field.
 *
 * <p>See {@link org.apache.flink.table.planner.codegen.ExprCodeGenerator#visitLocalRef}.
 */
public class ResolvedAggLocalReference implements Expression {

	private final String fieldTerm;
	private final String nullTerm;
	private final LogicalType resultType;

	public ResolvedAggLocalReference(String fieldTerm, String nullTerm, LogicalType resultType) {
		this.fieldTerm = fieldTerm;
		this.nullTerm = nullTerm;
		this.resultType = resultType;
	}

	public String getFieldTerm() {
		return fieldTerm;
	}

	public String getNullTerm() {
		return nullTerm;
	}

	public LogicalType getResultType() {
		return resultType;
	}

	@Override
	public String asSummaryString() {
		return fieldTerm;
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

		ResolvedAggLocalReference that = (ResolvedAggLocalReference) o;

		return fieldTerm.equals(that.fieldTerm) && nullTerm.equals(that.nullTerm) && resultType.equals(that.resultType);
	}

	@Override
	public int hashCode() {
		int result = fieldTerm.hashCode();
		result = 31 * result + nullTerm.hashCode();
		result = 31 * result + resultType.hashCode();
		return result;
	}

	@Override
	public String toString() {
		return asSummaryString();
	}
}
