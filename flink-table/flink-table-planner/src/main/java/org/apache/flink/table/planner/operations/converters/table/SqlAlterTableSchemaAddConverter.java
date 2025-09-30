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

import org.apache.flink.sql.parser.ddl.SqlAlterTableAdd;
import org.apache.flink.sql.parser.ddl.SqlAlterTableSchema;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.planner.operations.converters.SchemaAddConverter;
import org.apache.flink.table.planner.operations.converters.SchemaConverter;

/**
 * Convert ALTER TABLE ADD (&lt;schema_component&gt; [, &lt;schema_component&gt;, ...]) to generate
 * an updated Schema.
 */
public class SqlAlterTableSchemaAddConverter
        extends SqlAlterTableSchemaConverter<SqlAlterTableAdd> {
    @Override
    protected SchemaConverter createSchemaConverter(
            SqlAlterTableSchema alterTableSchema,
            ResolvedCatalogTable oldTable,
            ConvertContext context) {
        return new SchemaAddConverter(oldTable, context);
    }
}
