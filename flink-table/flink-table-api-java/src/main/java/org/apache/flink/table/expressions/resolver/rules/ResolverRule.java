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

package org.apache.flink.table.expressions.resolver.rules;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.FunctionLookup;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.LocalReferenceExpression;
import org.apache.flink.table.expressions.resolver.ExpressionResolver;
import org.apache.flink.table.expressions.resolver.LocalOverWindow;
import org.apache.flink.table.expressions.resolver.lookups.FieldReferenceLookup;
import org.apache.flink.table.expressions.resolver.lookups.TableReferenceLookup;
import org.apache.flink.table.functions.FunctionDefinition;

import java.util.List;
import java.util.Optional;

/**
 * Rule that can be applied during resolution of {@link Expression}. Rules are applied to a collection of expressions
 * at once, e.g. all expressions in a projection. One must consider order in which rules are applied. Some rules
 * might e.g. require that references to fields have to be already resolved.
 */
@Internal
public interface ResolverRule {

	List<Expression> apply(List<Expression> expression, ResolutionContext context);

	/**
	 * Contextual information that can be used during application of the rule. E.g. one can access fields in inputs by
	 * name etc.
	 */
	interface ResolutionContext {

		/**
		 * Access to available {@link org.apache.flink.table.expressions.FieldReferenceExpression} in inputs.
		 */
		FieldReferenceLookup referenceLookup();

		/**
		 * Access to available {@link org.apache.flink.table.expressions.TableReferenceExpression}.
		 */
		TableReferenceLookup tableLookup();

		/**
		 * Access to available {@link FunctionDefinition}s.
		 */
		FunctionLookup functionLookup();

		/**
		 * Enables the creation of resolved expressions for transformations after the actual resolution.
		 */
		ExpressionResolver.PostResolverFactory postResolutionFactory();

		/**
		 * Access to available local references.
		 */
		Optional<LocalReferenceExpression> getLocalReference(String alias);

		/**
		 * Access to available local over windows.
		 */
		Optional<LocalOverWindow> getOverWindow(Expression alias);
	}
}
