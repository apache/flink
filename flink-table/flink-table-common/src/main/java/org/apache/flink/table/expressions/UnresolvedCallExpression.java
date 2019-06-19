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

package org.apache.flink.table.expressions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Unresolved call expression for calling a function identified by a {@link FunctionDefinition}.
 *
 * <p>This is a purely API facing expression with unvalidated arguments and unknown output data type.
 */
@PublicEvolving
public final class UnresolvedCallExpression implements Expression {

	private final FunctionDefinition functionDefinition;

	private final List<Expression> args;

	public UnresolvedCallExpression(FunctionDefinition functionDefinition, List<Expression> args) {
		this.functionDefinition = Preconditions.checkNotNull(functionDefinition);
		this.args = Collections.unmodifiableList(new ArrayList<>(Preconditions.checkNotNull(args)));
	}

	public FunctionDefinition getFunctionDefinition() {
		return functionDefinition;
	}

	@Override
	public String asSummaryString() {
		final String argList = args.stream()
			.map(Expression::asSummaryString)
			.collect(Collectors.joining(", ", "(", ")"));
		return functionDefinition.toString() + argList;
	}

	@Override
	public List<Expression> getChildren() {
		return this.args;
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
		UnresolvedCallExpression that = (UnresolvedCallExpression) o;
		return Objects.equals(functionDefinition, that.functionDefinition) &&
			Objects.equals(args, that.args);
	}

	@Override
	public int hashCode() {
		return Objects.hash(functionDefinition, args);
	}

	@Override
	public String toString() {
		return asSummaryString();
	}
}
