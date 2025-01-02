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

import org.apache.flink.sql.parser.ddl.SqlAlterMaterializedTableAsQuery;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.materializedtable.AlterMaterializedTableAsQueryOperation;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;

import org.apache.calcite.sql.SqlNode;

/** A converter for {@link SqlAlterMaterializedTableAsQuery}. */
public class SqlAlterMaterializedTableAsQueryConverter
        implements SqlNodeConverter<SqlAlterMaterializedTableAsQuery> {

    @Override
    public Operation convertSqlNode(
            SqlAlterMaterializedTableAsQuery sqlAlterMaterializedTableAsQuery,
            ConvertContext context) {
        UnresolvedIdentifier unresolvedIdentifier =
                UnresolvedIdentifier.of(sqlAlterMaterializedTableAsQuery.fullTableName());
        ObjectIdentifier identifier =
                context.getCatalogManager().qualifyIdentifier(unresolvedIdentifier);

        // get query schema and definition query
        SqlNode validateQuery =
                context.getSqlValidator().validate(sqlAlterMaterializedTableAsQuery.getAsQuery());
        PlannerQueryOperation queryOperation =
                new PlannerQueryOperation(
                        context.toRelRoot(validateQuery).project(),
                        () -> context.toQuotedSqlString(validateQuery));
        return new AlterMaterializedTableAsQueryOperation(identifier, queryOperation);
    }
}
