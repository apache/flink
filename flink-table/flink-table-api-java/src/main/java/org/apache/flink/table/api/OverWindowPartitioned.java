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
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionParser;

import java.util.List;

/**
 * Partially defined over window with partitioning.
 */
@PublicEvolving
public final class OverWindowPartitioned {

	/** Defines a partitioning of the input on one or more attributes. */
	private final List<Expression> partitionBy;

	OverWindowPartitioned(List<Expression> partitionBy) {
		this.partitionBy = partitionBy;
	}

	/**
	 * Specifies the time attribute on which rows are ordered.
	 *
	 * <p>For streaming tables, reference a rowtime or proctime time attribute here
	 * to specify the time mode.
	 *
	 * <p>For batch tables, refer to a timestamp or long attribute.
	 *
	 * @param orderBy field reference
	 * @return an over window with defined order
	 * @deprecated use {@link #orderBy(Expression)}
	 */
	@Deprecated
	public OverWindowPartitionedOrdered orderBy(String orderBy) {
		return this.orderBy(ExpressionParser.parseExpression(orderBy));
	}

	/**
	 * Specifies the time attribute on which rows are ordered.
	 *
	 * <p>For streaming tables, reference a rowtime or proctime time attribute here
	 * to specify the time mode.
	 *
	 * <p>For batch tables, refer to a timestamp or long attribute.
	 *
	 * @param orderBy field reference
	 * @return an over window with defined order
	 */
	public OverWindowPartitionedOrdered orderBy(Expression orderBy) {
		return new OverWindowPartitionedOrdered(partitionBy, orderBy);
	}
}
