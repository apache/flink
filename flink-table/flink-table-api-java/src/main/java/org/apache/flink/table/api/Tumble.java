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

/**
 * Helper class for creating a tumbling window. Tumbling windows are consecutive, non-overlapping
 * windows of a specified fixed length. For example, a tumbling window of 5 minutes size groups
 * elements in 5 minutes intervals.
 *
 * <p>Java Example:
 *
 * <pre>
 * {@code
 *    Tumble.over("10.minutes").on("rowtime").as("w")
 * }
 * </pre>
 *
 * <p>Scala Example:
 *
 * <pre>
 * {@code
 *    Tumble over 5.minutes on 'rowtime as 'w
 * }
 * </pre>
 */
@PublicEvolving
public final class Tumble {

	/**
	 * Creates a tumbling window. Tumbling windows are fixed-size, consecutive, non-overlapping
	 * windows of a specified fixed length. For example, a tumbling window of 5 minutes size groups
	 * elements in 5 minutes intervals.
	 *
	 * @param size the size of the window as time or row-count interval.
	 * @return a partially defined tumbling window
	 * @deprecated use {@link #over(Expression)}
	 */
	@Deprecated
	public static TumbleWithSize over(String size) {
		return over(ExpressionParser.parseExpression(size));
	}

	/**
	 * Creates a tumbling window. Tumbling windows are fixed-size, consecutive, non-overlapping
	 * windows of a specified fixed length. For example, a tumbling window of 5 minutes size groups
	 * elements in 5 minutes intervals.
	 *
	 * @param size the size of the window as time or row-count interval.
	 * @return a partially defined tumbling window
	 */
	public static TumbleWithSize over(Expression size) {
		return new TumbleWithSize(size);
	}
}
