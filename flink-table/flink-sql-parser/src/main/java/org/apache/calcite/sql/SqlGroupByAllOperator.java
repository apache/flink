/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.calcite.sql;

/**
 * Marker operator for a bare {@code GROUP BY ALL} clause.
 *
 * <p>The parser emits a call to this operator as a placeholder, because at parse time it cannot
 * know the table's columns or which SELECT expressions are aggregates. {@code
 * FlinkCalciteSqlValidator} rewrites the placeholder into the actual grouping expressions during
 * validation, so this operator never reaches type derivation or conversion.
 */
public class SqlGroupByAllOperator extends SqlSpecialOperator {
    public static final SqlGroupByAllOperator INSTANCE = new SqlGroupByAllOperator();

    private SqlGroupByAllOperator() {
        super("GROUP BY ALL", SqlKind.OTHER);
    }

    @Override
    public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        writer.keyword("ALL");
    }
}
