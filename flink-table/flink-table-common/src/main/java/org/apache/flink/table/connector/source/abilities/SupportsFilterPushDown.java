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

package org.apache.flink.table.connector.source.abilities;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.expressions.ExpressionVisitor;
import org.apache.flink.table.expressions.ResolvedExpression;

import java.util.List;

/**
 * Enables to push down filters into a {@link ScanTableSource}.
 *
 * <p>Given the following SQL:
 * <pre>{@code
 *   SELECT * FROM t WHERE (a = '1' OR a = '2') AND b IS NOT NULL;
 * }</pre>
 *
 * <p>In the example above, {@code [a = '1' OR a = '2']} and {@code [b IS NOT NULL]} are acceptable
 * filters.
 *
 * <p>By default, if this interface is not implemented, filters are applied in a subsequent operation
 * after the source.
 *
 * <p>For efficiency, a source can push filters further down in order to be close to the actual data
 * generation. The passed filters are translated into conjunctive form. A source can pick filters and
 * return the accepted and remaining filters.
 *
 * <p>Accepted filters are filters that are consumed by the source but may be applied on a best effort
 * basis. The information about accepted filters helps the planner to adjust the cost estimation for
 * the current plan. A subsequent filter operation will still take place by the runtime depending on
 * the remaining filters.
 *
 * <p>Remaining filters are filters that cannot be fully applied by the source. The remaining filters
 * decide if a subsequent filter operation will still take place by the runtime.
 *
 * <p>By the above definition, accepted filters and remaining filters must not be disjunctive lists. A
 * filter can occur in both list. However, all given filters must be present in at least one list.
 *
 * <p>Note: A source is not allowed to change the given expressions in the returned {@link Result}.
 *
 * <p>Use {@link ExpressionVisitor} to traverse filter expressions.
 */
@PublicEvolving
public interface SupportsFilterPushDown {

	/**
	 * Provides a list of filters in conjunctive form. A source can pick filters and return the accepted
	 * and remaining filters.
	 *
	 * <p>See the documentation of {@link SupportsFilterPushDown} for more information.
	 */
	Result applyFilters(List<ResolvedExpression> filters);

	/**
	 * Result of a filter push down. It represents the communication of the source to the planner during
	 * optimization.
	 */
	final class Result {
		private final List<ResolvedExpression> acceptedFilters;
		private final List<ResolvedExpression> remainingFilters;

		private Result(
				List<ResolvedExpression> acceptedFilters,
				List<ResolvedExpression> remainingFilters) {
			this.acceptedFilters = acceptedFilters;
			this.remainingFilters = remainingFilters;
		}

		/**
		 * Constructs a filter push-down result.
		 *
		 * <p>See the documentation of {@link SupportsFilterPushDown} for more information.
		 *
		 * @param acceptedFilters filters that are consumed by the source but may be applied on a best
		 *                        effort basis
		 * @param remainingFilters filters that a subsequent filter operation still needs to perform
		 *                         during runtime
		 */
		public static Result of(
				List<ResolvedExpression> acceptedFilters,
				List<ResolvedExpression> remainingFilters) {
			return new Result(acceptedFilters, remainingFilters);
		}

		public List<ResolvedExpression> getAcceptedFilters() {
			return acceptedFilters;
		}

		public List<ResolvedExpression> getRemainingFilters() {
			return remainingFilters;
		}
	}
}
