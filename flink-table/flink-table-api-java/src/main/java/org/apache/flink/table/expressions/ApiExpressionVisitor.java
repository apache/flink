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
 * A visitor for all API-specific {@link Expression}s.
 */
@Internal
public abstract class ApiExpressionVisitor<R> implements ExpressionVisitor<R> {

	public abstract R visitTableReference(TableReferenceExpression tableReference);

	public abstract R visitLocalReference(LocalReferenceExpression localReference);

	public abstract R visitLookupCall(LookupCallExpression lookupCall);

	public abstract R visitUnresolvedReference(UnresolvedReferenceExpression unresolvedReference);

	public final R visit(Expression other) {
		if (other instanceof TableReferenceExpression) {
			return visitTableReference((TableReferenceExpression) other);
		} else if (other instanceof LocalReferenceExpression) {
			return visitLocalReference((LocalReferenceExpression) other);
		} else if (other instanceof LookupCallExpression) {
			return visitLookupCall((LookupCallExpression) other);
		} else if (other instanceof UnresolvedReferenceExpression) {
			return visitUnresolvedReference((UnresolvedReferenceExpression) other);
		}
		return visitNonApiExpression(other);
	}

	public abstract R visitNonApiExpression(Expression other);
}
