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
public interface ApiExpressionVisitor<R> extends ExpressionVisitor<R> {

	R visitTableReference(TableReferenceExpression tableReference);

	R visitUnresolvedCall(UnresolvedCallExpression unresolvedCall);

	R visitUnresolvedField(UnresolvedFieldReferenceExpression unresolvedField);

	default R visit(Expression other) {
		if (other instanceof TableReferenceExpression) {
			return visitTableReference((TableReferenceExpression) other);
		} else if (other instanceof UnresolvedCallExpression) {
			return visitUnresolvedCall((UnresolvedCallExpression) other);
		} else if (other instanceof UnresolvedFieldReferenceExpression) {
			return visitUnresolvedField((UnresolvedFieldReferenceExpression) other);
		}
		return visitNonApiExpression(other);
	}

	R visitNonApiExpression(Expression other);
}
