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
 * A visitor for all {@link Expression}s that might be created during API translation.
 */
@Internal
public abstract class ApiExpressionVisitor<R> implements ExpressionVisitor<R> {

	public final R visit(Expression other) {
		if (other instanceof UnresolvedReferenceExpression) {
			return visit((UnresolvedReferenceExpression) other);
		} else if (other instanceof TableReferenceExpression) {
			return visit((TableReferenceExpression) other);
		} else if (other instanceof LocalReferenceExpression) {
			return visit((LocalReferenceExpression) other);
		} else if (other instanceof LookupCallExpression) {
			return visit((LookupCallExpression) other);
		} else if (other instanceof UnresolvedCallExpression) {
			return visit((UnresolvedCallExpression) other);
		}
		return visitNonApiExpression(other);
	}

	// --------------------------------------------------------------------------------------------
	// resolved API expressions
	// --------------------------------------------------------------------------------------------

	public abstract R visit(TableReferenceExpression tableReference);

	public abstract R visit(LocalReferenceExpression localReference);

	// --------------------------------------------------------------------------------------------
	// unresolved API expressions
	// --------------------------------------------------------------------------------------------

	public abstract R visit(UnresolvedReferenceExpression unresolvedReference);

	public abstract R visit(LookupCallExpression lookupCall);

	public abstract R visit(UnresolvedCallExpression unresolvedCallExpression);

	// --------------------------------------------------------------------------------------------
	// other expressions
	// --------------------------------------------------------------------------------------------

	public abstract R visitNonApiExpression(Expression other);
}
