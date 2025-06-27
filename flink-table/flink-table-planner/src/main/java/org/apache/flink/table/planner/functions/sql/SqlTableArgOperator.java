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

package org.apache.flink.table.planner.functions.sql;

import org.apache.flink.table.planner.calcite.FlinkConvertletTable;
import org.apache.flink.table.planner.calcite.RexTableArgCall;
import org.apache.flink.table.types.inference.StaticArgument;
import org.apache.flink.table.types.inference.StaticArgumentTrait;

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlSpecialOperator;

/**
 * Marker for table arguments in a signature of {@link StaticArgument}s inserted by {@link
 * FlinkConvertletTable}.
 *
 * <p>The table argument describes a {@link StaticArgumentTrait#SET_SEMANTIC_TABLE} or {@link
 * StaticArgumentTrait#ROW_SEMANTIC_TABLE}. It is represented as {@link RexTableArgCall} going
 * forward.
 */
public class SqlTableArgOperator extends SqlSpecialOperator {

    public static final SqlTableArgOperator INSTANCE = new SqlTableArgOperator();

    public SqlTableArgOperator() {
        super("TABLE", SqlKind.OTHER);
    }
}
