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

package org.apache.flink.connector.jdbc.utils;

import java.util.List;
import java.util.function.Function;

/** SQL filter clauses. */
public enum FilterClause {
    EQ(list -> String.format("%s = %s", list.get(0), list.get(1)), 2),

    NOT_EQ(list -> String.format("%s <> %s", list.get(0), list.get(1)), 2),

    GT(list -> String.format("%s > %s", list.get(0), list.get(1)), 2),

    GT_EQ(list -> String.format("%s >= %s", list.get(0), list.get(1)), 2),

    LT(list -> String.format("%s < %s", list.get(0), list.get(1)), 2),

    LT_EQ(list -> String.format("%s <= %s", list.get(0), list.get(1)), 2),

    IS_NULL(list -> String.format("%s IS NULL", list.get(0)), 1),

    IS_NOT_NULL(list -> String.format("%s IS NOT NULL", list.get(0)), 1),

    AND(list -> String.format("%s AND %s", list.get(0), list.get(1)), 2),

    OR(list -> String.format("%s OR %s", list.get(0), list.get(1)), 2),

    LIKE(list -> String.format("%s LIKE %s", list.get(0), list.get(1)), 2),

    NOT(list -> String.format("!(%s)", list.get(0)), 1);

    public final Function<List<String>, String> formatter;
    public final int argsNum;

    FilterClause(final Function<List<String>, String> function, int argsNum) {
        this.formatter = function;
        this.argsNum = argsNum;
    }
}
