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

/**
 * A utility {@link ApiExpressionVisitor} that calls {@link ApiExpressionDefaultVisitor#defaultMethod(Expression)} by
 * default, unless other methods are overridden explicitly.
 */
@Internal
public abstract class ApiExpressionDefaultVisitor<T> extends ApiExpressionVisitor<T> {
	@Override
	public T visitCall(CallExpression call) {
		return defaultMethod(call);
	}

	@Override
	public T visitSymbol(SymbolExpression symbolExpression) {
		return defaultMethod(symbolExpression);
	}

	@Override
	public T visitValueLiteral(ValueLiteralExpression valueLiteralExpression) {
		return defaultMethod(valueLiteralExpression);
	}

	@Override
	public T visitFieldReference(FieldReferenceExpression fieldReference) {
		return defaultMethod(fieldReference);
	}

	@Override
	public T visitUnresolvedReference(UnresolvedReferenceExpression unresolvedReference) {
		return defaultMethod(unresolvedReference);
	}

	@Override
	public T visitTypeLiteral(TypeLiteralExpression typeLiteral) {
		return defaultMethod(typeLiteral);
	}

	@Override
	public T visitTableReference(TableReferenceExpression tableReference) {
		return defaultMethod(tableReference);
	}

	@Override
	public T visitLookupCall(LookupCallExpression lookupCall) {
		return defaultMethod(lookupCall);
	}

	@Override
	public T visitNonApiExpression(Expression other) {
		return defaultMethod(other);
	}

	protected abstract T defaultMethod(Expression expression);
}
