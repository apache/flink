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

import org.apache.flink.sql.parser.ddl.materializedtable.SqlCreateOrAlterMaterializedTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.materializedtable.MaterializedTableOperationFactory;
import org.apache.flink.table.operations.materializedtable.MaterializedTableOperationFactory.Mode;
import org.apache.flink.table.planner.operations.converters.SqlNodeConverter;

/** A converter for {@link SqlCreateOrAlterMaterializedTable}. */
public class SqlCreateOrAlterMaterializedTableConverter
        implements SqlNodeConverter<SqlCreateOrAlterMaterializedTable> {

    @Override
    public Operation convertSqlNode(
            SqlCreateOrAlterMaterializedTable sqlCreateOrAlterMaterializedTable,
            ConvertContext context) {
        final ObjectIdentifier identifier =
                context.getCatalogManager()
                        .qualifyIdentifier(
                                UnresolvedIdentifier.of(
                                        sqlCreateOrAlterMaterializedTable.getFullName()));
        final Mode mode =
                sqlCreateOrAlterMaterializedTable.getOperator()
                                == SqlCreateOrAlterMaterializedTable.CREATE_OR_ALTER_OPERATOR
                        ? Mode.CREATE_OR_ALTER
                        : Mode.CREATE;
        return new MaterializedTableOperationFactory(
                        context.getCatalogManager(), context.getTableConfig())
                .build(
                        identifier,
                        mode,
                        new SqlMaterializedTableMergeContext(
                                sqlCreateOrAlterMaterializedTable, context));
    }
}
