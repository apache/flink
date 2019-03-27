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
import org.apache.flink.table.expressions.catalog.FunctionDefinitionCatalog;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Resolves calls with function names to calls with actual function definitions.
 */
@Internal
public class UnresolvedCallResolver extends ApiExpressionDefaultVisitor<Expression> {

	private final FunctionDefinitionCatalog functionCatalog;

	public UnresolvedCallResolver(FunctionDefinitionCatalog functionCatalog) {
		this.functionCatalog = functionCatalog;
	}

	public Expression visitUnresolvedCall(UnresolvedCallExpression unresolvedCall) {
		FunctionDefinition functionDefinition = functionCatalog.lookupFunction(unresolvedCall.getUnresolvedName());
		return createResolvedCall(functionDefinition, unresolvedCall.getChildren());
	}

	public Expression visitCall(CallExpression call) {
		return createResolvedCall(call.getFunctionDefinition(), call.getChildren());
	}

	private Expression createResolvedCall(FunctionDefinition functionDefinition, List<Expression> unresolvedChildren) {
		List<Expression> resolvedChildren = unresolvedChildren
			.stream()
			.map(child -> child.accept(this))
			.collect(Collectors.toList());

		return new CallExpression(functionDefinition, resolvedChildren);
	}

	@Override
	protected Expression defaultMethod(Expression expression) {
		return expression;
	}
}
