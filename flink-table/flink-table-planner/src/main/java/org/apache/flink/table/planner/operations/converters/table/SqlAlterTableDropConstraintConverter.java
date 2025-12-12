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

package org.apache.flink.table.planner.operations.converters.table;

import org.apache.flink.sql.parser.ddl.table.SqlAlterTableDropConstraint;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.planner.operations.converters.SchemaReferencesManager;

import org.apache.calcite.sql.SqlIdentifier;

import java.util.List;
import java.util.Optional;

/** Convert ALTER TABLE DROP CONSTRAINT constraint_name to generate an updated {@link Schema}. */
public class SqlAlterTableDropConstraintConverter
        extends AbstractAlterTableConverter<SqlAlterTableDropConstraint> {
    @Override
    protected Operation convertToOperation(
            SqlAlterTableDropConstraint dropConstraint,
            ResolvedCatalogTable oldTable,
            ConvertContext context) {
        Optional<UniqueConstraint> pkConstraint = oldTable.getResolvedSchema().getPrimaryKey();
        if (pkConstraint.isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "%sThe base table does not define any primary key.", EX_MSG_PREFIX));
        }
        SqlIdentifier constraintIdentifier = dropConstraint.getConstraintName();
        String constraintName = pkConstraint.get().getName();
        if (constraintIdentifier != null
                && !constraintIdentifier.getSimple().equals(constraintName)) {
            throw new ValidationException(
                    String.format(
                            "%sThe base table does not define a primary key constraint named '%s'. "
                                    + "Available constraint name: ['%s'].",
                            EX_MSG_PREFIX, constraintIdentifier.getSimple(), constraintName));
        }

        Schema.Builder schemaBuilder = Schema.newBuilder();
        SchemaReferencesManager.buildUpdatedColumn(
                schemaBuilder, oldTable, (builder, column) -> builder.fromColumns(List.of(column)));
        SchemaReferencesManager.buildUpdatedWatermark(schemaBuilder, oldTable);

        return buildAlterTableChangeOperation(
                dropConstraint,
                List.of(TableChange.dropConstraint(constraintName)),
                schemaBuilder.build(),
                oldTable,
                context.getCatalogManager());
    }
}
