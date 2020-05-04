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

/**
 * A table that performs flatAggregate on a {@link Table}, a {@link GroupedTable} or a
 * {@link WindowGroupedTable}.
 */
@PublicEvolving
public interface FlatAggregateTable {

	/**
	 * Performs a selection operation on a FlatAggregateTable. Similar to a SQL SELECT
	 * statement. The field expressions can contain complex expressions.
	 *
	 * <p><b>Note</b>: You have to close the flatAggregate with a select statement. And the select
	 * statement does not support aggregate functions.
	 *
	 * <p>Example:
	 *
	 * <pre>
	 * {@code
	 *   TableAggregateFunction tableAggFunc = new MyTableAggregateFunction();
	 *   tableEnv.registerFunction("tableAggFunc", tableAggFunc);
	 *   tab.groupBy("key")
	 *     .flatAggregate("tableAggFunc(a, b) as (x, y, z)")
	 *     .select("key, x, y, z")
	 * }
	 * </pre>
	 * @deprecated use {@link #select(Expression...)}
	 */
	@Deprecated
	Table select(String fields);

	/**
	 * Performs a selection operation on a FlatAggregateTable table. Similar to a SQL SELECT
	 * statement. The field expressions can contain complex expressions.
	 *
	 * <p><b>Note</b>: You have to close the flatAggregate with a select statement. And the select
	 * statement does not support aggregate functions.
	 *
	 * <p>Example:
	 *
	 * <pre>
	 * {@code
	 *   TableAggregateFunction tableAggFunc = new MyTableAggregateFunction();
	 *   tableEnv.registerFunction("tableAggFunc", tableAggFunc);
	 *   tab.groupBy($("key"))
	 *     .flatAggregate(call("tableAggFunc", $("a"), $("b")).as("x", "y", "z"))
	 *     .select($("key"), $("x"), $("y"), $("z"));
	 * }
	 * </pre>
	 *
	 * <p>Scala Example:
	 *
	 * <pre>
	 * {@code
	 *   val tableAggFunc: TableAggregateFunction = new MyTableAggregateFunction
	 *   tab.groupBy($"key")
	 *     .flatAggregate(tableAggFunc($"a", $"b") as ("x", "y", "z"))
	 *     .select($"key", $"x", $"y", $"z")
	 * }
	 * </pre>
	 */
	Table select(Expression... fields);
}
