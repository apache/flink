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

package org.apache.flink.table.planner.operations.converters;

import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;

import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlTableRef;

import java.util.EnumSet;
import java.util.Optional;

/**
 * A converter for query {@link SqlNode}, e.g., {@link SqlSelect}, {@link SqlOrderBy}, {@link
 * SqlTableRef}.
 */
public class SqlQueryConverter implements SqlNodeConverter<SqlNode> {

    @Override
    public Optional<EnumSet<SqlKind>> supportedSqlKinds() {
        return Optional.of(SqlKind.QUERY);
    }

    @Override
    public Operation convertSqlNode(SqlNode node, ConvertContext context) {
        // transform to a relational tree
        RelRoot relational = context.toRelRoot(node);
        return new PlannerQueryOperation(relational.project());
    }
}
