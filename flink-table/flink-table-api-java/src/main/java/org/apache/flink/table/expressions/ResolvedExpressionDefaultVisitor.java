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
 * A utility {@link ResolvedExpressionVisitor} that calls {@link #defaultMethod(ResolvedExpression)}
 * by default, unless other methods are overridden explicitly.
 */
@Internal
public abstract class ResolvedExpressionDefaultVisitor<T> extends ResolvedExpressionVisitor<T> {

	@Override
	public T visit(TableReferenceExpression tableReference) {
		return defaultMethod(tableReference);
	}

	@Override
	public T visit(LocalReferenceExpression localReference) {
		return defaultMethod(localReference);
	}

	@Override
	public T visit(CallExpression call) {
		return defaultMethod(call);
	}

	@Override
	public T visit(ValueLiteralExpression valueLiteral) {
		return defaultMethod(valueLiteral);
	}

	@Override
	public T visit(FieldReferenceExpression fieldReference) {
		return defaultMethod(fieldReference);
	}

	@Override
	public T visit(TypeLiteralExpression typeLiteral) {
		return defaultMethod(typeLiteral);
	}

	protected abstract T defaultMethod(ResolvedExpression expression);
}
