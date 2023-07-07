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

package org.apache.flink.table.planner.operations;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ReplaceTableAsOperation;
import org.apache.flink.table.operations.ddl.CreateTableOperation;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.parse.CalciteParser;
import org.apache.flink.table.types.AbstractDataType;

import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test base for testing convert [CREATE OR] REPLACE TABLE AS statement to operation. */
public class SqlRTASNodeToOperationConverterTest extends SqlNodeToOperationConversionTestBase {

    @Test
    public void testReplaceTableAS() {
        String tableName = "replace_table";
        String tableComment = "test table comment 表描述";
        String sql =
                "REPLACE TABLE "
                        + tableName
                        + " COMMENT '"
                        + tableComment
                        + "' WITH ('k1' = 'v1', 'k2' = 'v2') as SELECT * FROM t1";
        testCommonReplaceTableAs(sql, tableName, tableComment);
    }

    @Test
    public void testCreateOrReplaceTableAS() {
        String tableName = "create_or_replace_table";
        String sql =
                "CREATE OR REPLACE TABLE "
                        + tableName
                        + " WITH ('k1' = 'v1', 'k2' = 'v2') as SELECT * FROM t1";
        testCommonReplaceTableAs(sql, tableName, null);
    }

    private void testCommonReplaceTableAs(
            String sql, String tableName, @Nullable String tableComment) {
        ObjectIdentifier expectedIdentifier = ObjectIdentifier.of("builtin", "default", tableName);
        Operation operation = parseAndConvert(sql);
        CatalogTable expectedCatalogTable =
                CatalogTable.of(
                        getDefaultTableSchema(),
                        tableComment,
                        Collections.emptyList(),
                        getDefaultTableOptions());
        verifyReplaceTableAsOperation(operation, expectedIdentifier, expectedCatalogTable);
    }

    private Operation parseAndConvert(String sql) {
        final FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
        final CalciteParser parser = getParserBySqlDialect(SqlDialect.DEFAULT);

        SqlNode node = parser.parse(sql);
        return SqlNodeToOperationConversion.convert(planner, catalogManager, node).get();
    }

    private void verifyReplaceTableAsOperation(
            Operation operation,
            ObjectIdentifier expectedTableIdentifier,
            CatalogTable expectedCatalogTable) {
        assertThat(operation).isInstanceOf(ReplaceTableAsOperation.class);
        ReplaceTableAsOperation replaceTableAsOperation = (ReplaceTableAsOperation) operation;
        CreateTableOperation createTableOperation =
                replaceTableAsOperation.getCreateTableOperation();
        // verify the createTableOperation
        assertThat(createTableOperation.isTemporary()).isFalse();
        assertThat(createTableOperation.isIgnoreIfExists()).isFalse();
        assertThat(createTableOperation.getTableIdentifier()).isEqualTo(expectedTableIdentifier);
        // verify the catalog table to be created
        verifyCatalogTable(expectedCatalogTable, createTableOperation.getCatalogTable());
    }

    private void verifyCatalogTable(
            CatalogTable expectedCatalogTable, CatalogTable actualCatalogTable) {
        assertThat(actualCatalogTable.getUnresolvedSchema())
                .isEqualTo(expectedCatalogTable.getUnresolvedSchema());
        assertThat(actualCatalogTable.getComment()).isEqualTo(expectedCatalogTable.getComment());
        assertThat(actualCatalogTable.getPartitionKeys())
                .isEqualTo(expectedCatalogTable.getPartitionKeys());
        assertThat(actualCatalogTable.getOptions()).isEqualTo(expectedCatalogTable.getOptions());
    }

    private Map<String, String> getDefaultTableOptions() {
        Map<String, String> expectedOptions = new HashMap<>();
        expectedOptions.put("k1", "v1");
        expectedOptions.put("k2", "v2");
        return expectedOptions;
    }

    private Schema getDefaultTableSchema() {
        return Schema.newBuilder()
                .fromFields(
                        new String[] {"a", "b", "c", "d"},
                        new AbstractDataType[] {
                            DataTypes.BIGINT(),
                            DataTypes.STRING(),
                            DataTypes.INT(),
                            DataTypes.STRING()
                        })
                .build();
    }
}
