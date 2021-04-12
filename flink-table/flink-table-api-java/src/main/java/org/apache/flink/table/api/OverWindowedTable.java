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
 * A table that has been windowed for {@link OverWindow}s.
 *
 * <p>Unlike group windows, which are specified in the {@code GROUP BY} clause, over windows do not
 * collapse rows. Instead over window aggregates compute an aggregate for each input row over a
 * range of its neighboring rows.
 */
@PublicEvolving
public interface OverWindowedTable {

    /**
     * Performs a selection operation on a over windowed table. Similar to an SQL SELECT statement.
     * The field expressions can contain complex expressions and aggregations.
     *
     * <p>Example:
     *
     * <pre>{@code
     * overWindowedTable.select("c, b.count over ow, e.sum over ow")
     * }</pre>
     *
     * @deprecated use {@link #select(Expression...)}
     */
    @Deprecated
    Table select(String fields);

    /**
     * Performs a selection operation on a over windowed table. Similar to an SQL SELECT statement.
     * The field expressions can contain complex expressions and aggregations.
     *
     * <p>Example:
     *
     * <pre>{@code
     * overWindowedTable.select(
     *    $("c"),
     *    $("b").count().over($("ow")),
     *    $("e").sum().over($("ow"))
     * );
     * }</pre>
     *
     * <p>Scala Example:
     *
     * <pre>{@code
     * overWindowedTable.select('c, 'b.count over 'ow, 'e.sum over 'ow)
     * }</pre>
     */
    Table select(Expression... fields);
}
