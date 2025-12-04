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

import org.apache.flink.sql.parser.ddl.table.SqlAlterTableReset;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ddl.AlterTableChangeOperation;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Convert ALTER TABLE RESET statement. */
public class SqlAlterTableResetConverter extends AbstractAlterTableConverter<SqlAlterTableReset> {
    @Override
    protected Operation convertToOperation(
            SqlAlterTableReset alterTableReset,
            ResolvedCatalogTable oldTable,
            ConvertContext context) {
        final ObjectIdentifier tableIdentifier = resolveIdentifier(alterTableReset, context);
        Map<String, String> newOptions = new HashMap<>(oldTable.getOptions());
        // reset empty or 'connector' key is not allowed
        Set<String> resetKeys = alterTableReset.getResetKeys();
        if (resetKeys.isEmpty() || resetKeys.contains(FactoryUtil.CONNECTOR.key())) {
            String exMsg =
                    resetKeys.isEmpty()
                            ? "ALTER TABLE RESET does not support empty key"
                            : "ALTER TABLE RESET does not support changing 'connector'";
            throw new ValidationException(exMsg);
        }
        // reset table option keys
        resetKeys.forEach(newOptions::remove);
        return new AlterTableChangeOperation(
                tableIdentifier,
                resetKeys.stream().map(TableChange::reset).collect(Collectors.toList()),
                oldTable.copy(newOptions),
                alterTableReset.ifTableExists());
    }
}
