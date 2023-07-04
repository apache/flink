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

import org.apache.flink.sql.parser.ddl.SqlAlterTable;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ddl.AlterTableChangeOperation;
import org.apache.flink.table.operations.ddl.AlterTableOperation;

import org.apache.calcite.sql.SqlIdentifier;

import java.util.List;
import java.util.Optional;

/** A utility class for converting sql to {@link AlterTableOperation}. */
public final class AlterSchemaUtils {

    private AlterSchemaUtils() {}

    public static String getColumnName(SqlIdentifier identifier) {
        if (!identifier.isSimple()) {
            throw new UnsupportedOperationException(
                    String.format("Alter nested row type %s is not supported yet.", identifier));
        }
        return identifier.getSimple();
    }

    public static <T> T unwrap(Optional<T> value) {
        return value.orElseThrow(() -> new TableException("The value should never be empty."));
    }

    public static Operation buildAlterTableChangeOperation(
            ObjectIdentifier tableIdentifier,
            SqlAlterTable alterTable,
            List<TableChange> tableChanges,
            Schema newSchema,
            ResolvedCatalogTable oldTable) {
        return new AlterTableChangeOperation(
                tableIdentifier,
                tableChanges,
                CatalogTable.of(
                        newSchema,
                        oldTable.getComment(),
                        oldTable.getPartitionKeys(),
                        oldTable.getOptions()),
                alterTable.ifTableExists());
    }
}
