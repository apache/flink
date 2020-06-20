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
 * A table that has been performed on the aggregate function.
 */
@PublicEvolving
public interface AggregatedTable {

	/**
	 * Performs a selection operation after an aggregate operation. The field expressions
	 * cannot contain table functions and aggregations.
	 *
	 * <p>Example:
	 *
	 * <pre>
	 * {@code
	 *   AggregateFunction aggFunc = new MyAggregateFunction();
	 *   tableEnv.registerFunction("aggFunc", aggFunc);
	 *   table.groupBy("key")
	 *     .aggregate("aggFunc(a, b) as (f0, f1, f2)")
	 *     .select("key, f0, f1");
	 * }
	 * </pre>
	 * @deprecated use {@link #select(Expression...)}
	 */
	@Deprecated
	Table select(String fields);

	/**
	 * Performs a selection operation after an aggregate operation. The field expressions
	 * cannot contain table functions and aggregations.
	 *
	 * <p>Example:
	 *
	 * <pre>
	 * {@code
	 *   AggregateFunction aggFunc = new MyAggregateFunction();
	 *   tableEnv.registerFunction("aggFunc", aggFunc);
	 *   table.groupBy($("key"))
	 *     .aggregate(call("aggFunc", $("a"), $("b")).as("f0", "f1", "f2"))
	 *     .select($("key"), $("f0"), $("f1"));
	 * }
	 * </pre>
	 *
	 * <p>Scala Example:
	 *
	 * <pre>
	 * {@code
	 *   val aggFunc = new MyAggregateFunction
	 *   table.groupBy($"key")
	 *     .aggregate(aggFunc($"a", $"b") as ("f0", "f1", "f2"))
	 *     .select($"key", $"f0", $"f1")
	 * }
	 * </pre>
	 */
	Table select(Expression... fields);
}
