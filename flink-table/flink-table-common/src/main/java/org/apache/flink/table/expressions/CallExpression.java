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

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * General expression for unresolved function calls. The function can be a built-in function
 * or a user-defined function.
 */
@PublicEvolving
public class CallExpression implements Expression {

	private final FunctionDefinition functionDefinition;

	private final List<Expression> args = new ArrayList<>();

	public CallExpression(FunctionDefinition functionDefinition, List<Expression> args) {
		this.functionDefinition = functionDefinition;
		this.args.addAll(args);
	}

	@Override
	public List<Expression> getChildren() {
		return this.args;
	}

	public FunctionDefinition getFunctionDefinition() {
		return functionDefinition;
	}

	@Override
	public <R> R accept(ExpressionVisitor<R> visitor) {
		return visitor.visitCall(this);
	}

	@Override
	public String toString() {
		return functionDefinition.getName() + "(" + StringUtils.join(args, ", ") + ")";
	}
}
