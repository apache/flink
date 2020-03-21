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

package org.apache.flink.table.api;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.expressions.ApiExpressionUtils;
import org.apache.flink.table.expressions.Expression;

/**
 * A group window specification.
 *
 * <p>Group windows group rows based on time or row-count intervals and is therefore essentially a
 * special type of groupBy. Just like groupBy, group windows allow to compute aggregates
 * on groups of elements.
 *
 * <p>Infinite streaming tables can only be grouped into time or row intervals. Hence window
 * grouping is required to apply aggregations on streaming tables.
 *
 * <p>For finite batch tables, group windows provide shortcuts for time-based groupBy.
 */
@PublicEvolving
public abstract class GroupWindow {

	/** Alias name for the group window. */
	private final Expression alias;
	private final Expression timeField;

	GroupWindow(Expression alias, Expression timeField) {
		this.alias = ApiExpressionUtils.unwrapFromApi(alias);
		this.timeField = ApiExpressionUtils.unwrapFromApi(timeField);
	}

	public Expression getAlias() {
		return alias;
	}

	public Expression getTimeField() {
		return timeField;
	}
}
