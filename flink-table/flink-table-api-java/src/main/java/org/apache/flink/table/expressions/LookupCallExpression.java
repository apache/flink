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

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A call expression where the target function has not been resolved yet.
 *
 * <p>Instead of a {@link FunctionDefinition}, the call is identified by the function's name and needs to be lookup in
 * a catalog
 */
@PublicEvolving
public final class LookupCallExpression implements Expression {

	private final String unresolvedName;

	private final List<Expression> args;

	LookupCallExpression(String unresolvedFunction, List<Expression> args) {
		this.unresolvedName = Preconditions.checkNotNull(unresolvedFunction);
		this.args = Collections.unmodifiableList(Preconditions.checkNotNull(args));
	}

	public String getUnresolvedName() {
		return unresolvedName;
	}

	@Override
	public String asSummaryString() {
		final List<String> argList = args.stream().map(Object::toString).collect(Collectors.toList());
		return unresolvedName + "(" + String.join(", ", argList) + ")";
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
		LookupCallExpression that = (LookupCallExpression) o;
		return Objects.equals(unresolvedName, that.unresolvedName) &&
			Objects.equals(args, that.args);
	}

	@Override
	public int hashCode() {
		return Objects.hash(unresolvedName, args);
	}

	@Override
	public String toString() {
		return asSummaryString();
	}
}
