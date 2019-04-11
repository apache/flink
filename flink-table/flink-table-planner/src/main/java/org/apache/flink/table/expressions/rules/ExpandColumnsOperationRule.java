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

package org.apache.flink.table.expressions.rules;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.expressions.ColumnsOperationExpander;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.UnresolvedReferenceExpression;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Replaces columns operations with all available {@link org.apache.flink.table.expressions.UnresolvedReferenceExpression}s
 * from underlying inputs.
 */
@Internal
final class ExpandColumnsOperationRule implements ResolverRule {
	@Override
	public List<Expression> apply(List<Expression> expression, ResolutionContext context) {
		ColumnsOperationExpander columnsOperationExpander =
			new ColumnsOperationExpander(
				context.referenceLookup().getAllInputFields().stream()
					.map(p -> new UnresolvedReferenceExpression(p.getName()))
					.collect(Collectors.toList())
			);
		return expression.stream()
			.flatMap(expr -> expr.accept(columnsOperationExpander).stream())
			.collect(Collectors.toList());
	}
}
