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

package org.apache.flink.table.planner.operations.converters.materializedtable;

import org.apache.flink.sql.parser.ddl.materializedtable.SqlAlterMaterializedTableAsQuery;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.materializedtable.AlterMaterializedTableAsQueryOperation;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.table.planner.utils.MaterializedTableUtils;

import org.apache.calcite.sql.SqlNode;

import java.util.List;

/** A converter for {@link SqlAlterMaterializedTableAsQuery}. */
public class SqlAlterMaterializedTableAsQueryConverter
        extends AbstractAlterMaterializedTableConverter<SqlAlterMaterializedTableAsQuery> {

    @Override
    protected Operation convertToOperation(
            SqlAlterMaterializedTableAsQuery sqlAlterTableAsQuery,
            ResolvedCatalogMaterializedTable oldTable,
            ConvertContext context) {
        ObjectIdentifier identifier = resolveIdentifier(sqlAlterTableAsQuery, context);

        // Validate and extract schema from query
        String originalQuery = context.toQuotedSqlString(sqlAlterTableAsQuery.getAsQuery());
        SqlNode validatedQuery =
                context.getSqlValidator().validate(sqlAlterTableAsQuery.getAsQuery());
        String definitionQuery = context.toQuotedSqlString(validatedQuery);
        PlannerQueryOperation queryOperation =
                new PlannerQueryOperation(
                        context.toRelRoot(validatedQuery).project(), () -> definitionQuery);
        ResolvedSchema oldSchema = oldTable.getResolvedSchema();
        ResolvedSchema newSchema = queryOperation.getResolvedSchema();

        List<TableChange> tableChanges =
                MaterializedTableUtils.buildSchemaTableChanges(oldSchema, newSchema);
        tableChanges.add(TableChange.modifyDefinitionQuery(originalQuery, definitionQuery));

        return new AlterMaterializedTableAsQueryOperation(identifier, tableChanges, oldTable);
    }
}
