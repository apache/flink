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

import org.apache.flink.sql.parser.ddl.SqlAlterTableSchema;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.planner.operations.converters.SchemaConverter;

/**
 * Abstract class for converters to convert ALTER TABLE ADD | MODIFY (&lt;schema_component&gt; [,
 * &lt;schema_component&gt;, ...]) to generate an updated Schema.
 */
public abstract class SqlAlterTableSchemaConverter<T extends SqlAlterTableSchema>
        extends AbstractAlterTableConverter<T> {
    @Override
    protected Operation convertToOperation(
            T alterTableSchema, ResolvedCatalogTable oldTable, ConvertContext context) {
        SchemaConverter converter = createSchemaConverter(oldTable, context);
        converter.updateColumn(alterTableSchema.getColumnPositions().getList());
        alterTableSchema.getWatermark().ifPresent(converter::updateWatermark);
        alterTableSchema.getFullConstraint().ifPresent(converter::updatePrimaryKey);

        return buildAlterTableChangeOperation(
                alterTableSchema,
                converter.getChangesCollector(),
                converter.convert(),
                oldTable,
                context.getCatalogManager());
    }

    protected abstract SchemaConverter createSchemaConverter(
            ResolvedCatalogTable oldTable, ConvertContext context);
}
