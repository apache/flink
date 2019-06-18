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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.FunctionLookup;
import org.apache.flink.table.functions.FunctionDefinition;

import java.util.List;

import static org.apache.flink.table.expressions.ApiExpressionUtils.call;

/**
 * Resolves calls with function names to calls with actual function definitions.
 */
@Internal
public class LookupCallResolver extends ApiExpressionDefaultVisitor<Expression> {

	private final FunctionLookup functionLookup;

	public LookupCallResolver(FunctionLookup functionLookup) {
		this.functionLookup = functionLookup;
	}

	public Expression visit(LookupCallExpression lookupCall) {
		final FunctionLookup.Result result = functionLookup.lookupFunction(lookupCall.getUnresolvedName())
			.orElseThrow(() -> new ValidationException("Undefined function: " + lookupCall.getUnresolvedName()));

		return createResolvedCall(result.getFunctionDefinition(), lookupCall.getChildren());
	}

	public Expression visit(CallExpression call) {
		return createResolvedCall(call.getFunctionDefinition(), call.getChildren());
	}

	private Expression createResolvedCall(FunctionDefinition functionDefinition, List<Expression> unresolvedChildren) {
		final Expression[] resolvedChildren = unresolvedChildren
			.stream()
			.map(child -> child.accept(this))
			.toArray(Expression[]::new);

		return call(functionDefinition, resolvedChildren);
	}

	@Override
	protected Expression defaultMethod(Expression expression) {
		return expression;
	}
}
