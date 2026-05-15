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

import org.apache.flink.sql.parser.SqlParseUtils;
import org.apache.flink.sql.parser.ddl.SqlTableOption;
import org.apache.flink.sql.parser.ddl.materializedtable.SqlAlterMaterializedTableOptions;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.materializedtable.AlterMaterializedTableChangeOperation;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;

import java.util.List;
import java.util.function.Function;

/** A converter for {@link SqlAlterMaterializedTableOptions}. */
public class SqlAlterMaterializedTableOptionsConverter
        extends AbstractAlterMaterializedTableConverter<SqlAlterMaterializedTableOptions> {

    @Override
    protected Operation convertToOperation(
            SqlAlterMaterializedTableOptions sqlAlterTable,
            ResolvedCatalogMaterializedTable oldTable,
            ConvertContext context) {
        return new AlterMaterializedTableChangeOperation(
                resolveIdentifier(sqlAlterTable, context),
                gatherTableChanges(sqlAlterTable, context),
                oldTable);
    }

    @Override
    protected Function<ResolvedCatalogMaterializedTable, List<TableChange>> gatherTableChanges(
            SqlAlterMaterializedTableOptions sqlAlterTable, ConvertContext context) {
        final SqlNodeList propertyList = sqlAlterTable.getPropertyList();
        if (propertyList.getList().isEmpty()) {
            throw new ValidationException(
                    EX_MSG_PREFIX + "ALTER MATERIALIZED TABLE SET does not support empty options.");
        }
        final List<TableChange> changes =
                SqlParseUtils.extractList(
                        propertyList, SqlAlterMaterializedTableOptionsConverter::toSetOption);
        return oldTable -> changes;
    }

    private static TableChange.SetOption toSetOption(final SqlNode property) {
        final SqlTableOption option = (SqlTableOption) property;
        return TableChange.set(option.getKeyString(), option.getValueString());
    }
}
