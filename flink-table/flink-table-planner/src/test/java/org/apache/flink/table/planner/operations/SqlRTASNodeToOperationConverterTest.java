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
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.TableDistribution;
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
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test base for testing convert [CREATE OR] REPLACE TABLE AS statement to operation. */
class SqlRTASNodeToOperationConverterTest extends SqlNodeToOperationConversionTestBase {

    @Test
    void testReplaceTableAs() {
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
    void testReplaceTableAsWithOrderingColumns() {
        String tableName = "replace_table";
        String sql =
                "REPLACE TABLE "
                        + tableName
                        + " (a, b) WITH ('k1' = 'v1', 'k2' = 'v2') as SELECT b, a FROM t1";
        Schema tableSchema =
                Schema.newBuilder()
                        .column("a", DataTypes.BIGINT().notNull())
                        .column("b", DataTypes.STRING())
                        .build();

        testCommonReplaceTableAs(sql, tableName, null, tableSchema, null, Collections.emptyList());
    }

    @Test
    void testReplaceTableAsWithNotFoundColumnIdentifiers() {
        String tableName = "replace_table";
        String sql =
                "REPLACE TABLE "
                        + tableName
                        + " (a, d) WITH ('k1' = 'v1', 'k2' = 'v2') as SELECT b, a FROM t1";

        assertThatThrownBy(() -> parseAndConvert(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Column 'd' not found in the source schema.");
    }

    @Test
    void testReplaceTableAsWithMismatchIdentifiersLength() {
        String tableName = "replace_table";
        String sql =
                "REPLACE TABLE "
                        + tableName
                        + " (a) WITH ('k1' = 'v1', 'k2' = 'v2') as SELECT b, a FROM t1";

        assertThatThrownBy(() -> parseAndConvert(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "The number of columns in the column list "
                                + "must match the number of columns in the source schema.");
    }

    @Test
    void testCreateOrReplaceTableAs() {
        String tableName = "create_or_replace_table";
        String sql =
                "CREATE OR REPLACE TABLE "
                        + tableName
                        + " WITH ('k1' = 'v1', 'k2' = 'v2') as SELECT * FROM t1";
        testCommonReplaceTableAs(sql, tableName, null);
    }

    @Test
    void testCreateOrReplaceTableAsWithColumns() {
        String tableName = "create_or_replace_table";
        String sql =
                "CREATE OR REPLACE TABLE "
                        + tableName
                        + "(c0 int, c1 double metadata, c2 as c0 * a, c3 int metadata virtual) "
                        + " WITH ('k1' = 'v1', 'k2' = 'v2') as SELECT * FROM t1";
        Schema tableSchema =
                Schema.newBuilder()
                        .column("c0", DataTypes.INT())
                        .columnByMetadata("c1", DataTypes.DOUBLE())
                        .columnByExpression("c2", "`c0` * `a`")
                        .columnByMetadata("c3", DataTypes.INT(), true)
                        .fromSchema(getDefaultTableSchema())
                        .build();

        testCommonReplaceTableAs(sql, tableName, null, tableSchema, null, Collections.emptyList());
    }

    @Test
    void testCreateOrReplaceTableAsWithColumnsOverridden() {
        String tableName = "create_or_replace_table";
        String sql =
                "CREATE OR REPLACE TABLE "
                        + tableName
                        + "(c0 int, a double, bb string, c int metadata, dd string metadata) "
                        + " WITH ('k1' = 'v1', 'k2' = 'v2') "
                        + "as SELECT a, b as `bb`, c, d as `dd` FROM t1";
        Schema tableSchema =
                Schema.newBuilder()
                        .column("c0", DataTypes.INT())
                        .column("a", DataTypes.DOUBLE())
                        .column("bb", DataTypes.STRING())
                        .columnByMetadata("c", DataTypes.INT())
                        .columnByMetadata("dd", DataTypes.STRING())
                        .build();

        testCommonReplaceTableAs(sql, tableName, null, tableSchema, null, Collections.emptyList());
    }

    @Test
    void testCreateOrReplaceTableAsWithNotNullColumnsAreNotAllowed() {
        String tableName = "create_or_replace_table";
        String sql =
                "CREATE OR REPLACE TABLE "
                        + tableName
                        + "(c0 int not null) "
                        + " WITH ('k1' = 'v1', 'k2' = 'v2') as SELECT * FROM t1";

        assertThatThrownBy(() -> parseAndConvert(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Column 'c0' has no default value and does not allow NULLs.");
    }

    @Test
    void testCreateOrReplaceTableAsWithOverriddenVirtualMetadataColumnsNotAllowed() {
        String tableName = "create_or_replace_table";
        String sql =
                "CREATE OR REPLACE TABLE "
                        + tableName
                        + "(c int metadata virtual) "
                        + " WITH ('k1' = 'v1', 'k2' = 'v2') as SELECT * FROM t1";

        assertThatThrownBy(() -> parseAndConvert(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "A column named 'c' already exists in the source schema. "
                                + "Virtual metadata columns cannot overwrite columns from "
                                + "source.");
    }

    @Test
    void testCreateOrReplaceTableAsWithOverriddenComputedColumnsNotAllowed() {
        String tableName = "create_or_replace_table";
        String sql =
                "CREATE OR REPLACE TABLE "
                        + tableName
                        + "(c as 'f0 * 2') "
                        + " WITH ('k1' = 'v1', 'k2' = 'v2') as SELECT * FROM t1";

        assertThatThrownBy(() -> parseAndConvert(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "A column named 'c' already exists in the source schema. "
                                + "Computed columns cannot overwrite columns from source.");
    }

    @Test
    void testCreateOrReplaceTableAsWithIncompatibleImplicitCastTypes() {
        String tableName = "create_or_replace_table";
        String sql =
                "CREATE OR REPLACE TABLE "
                        + tableName
                        + "(a boolean) "
                        + " WITH ('k1' = 'v1', 'k2' = 'v2') as SELECT * FROM t1";

        assertThatThrownBy(() -> parseAndConvert(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Incompatible types for sink column 'a' at position 0. "
                                + "The source column has type 'BIGINT NOT NULL', while the target "
                                + "column has type 'BOOLEAN'.");
    }

    @Test
    void testCreateOrReplaceTableAsWithDistribution() {
        String tableName = "create_or_replace_table";
        String sql =
                "CREATE OR REPLACE TABLE "
                        + tableName
                        + " DISTRIBUTED BY HASH(b) INTO 2 BUCKETS "
                        + " WITH ('k1' = 'v1', 'k2' = 'v2') as SELECT * FROM t1";
        Schema tableSchema = Schema.newBuilder().fromSchema(getDefaultTableSchema()).build();

        testCommonReplaceTableAs(
                sql,
                tableName,
                null,
                tableSchema,
                TableDistribution.ofHash(Collections.singletonList("b"), 2),
                Collections.emptyList());
    }

    @Test
    void testCreateOrReplaceTableAsWithPrimaryKey() {
        String tableName = "create_or_replace_table";
        String sql =
                "CREATE OR REPLACE TABLE "
                        + tableName
                        + "(PRIMARY KEY (a) NOT ENFORCED) "
                        + " WITH ('k1' = 'v1', 'k2' = 'v2') as SELECT * FROM t1";
        Schema tableSchema =
                Schema.newBuilder()
                        .column("a", DataTypes.BIGINT().notNull())
                        .column("b", DataTypes.STRING())
                        .column("c", DataTypes.INT())
                        .column("d", DataTypes.STRING())
                        .primaryKey("a")
                        .build();

        testCommonReplaceTableAs(sql, tableName, null, tableSchema, null, Collections.emptyList());
    }

    @Test
    void testCreateOrReplaceTableAsWithWatermark() {
        String tableName = "create_or_replace_table";
        String sql =
                "CREATE OR REPLACE TABLE "
                        + tableName
                        + "(c0 TIMESTAMP(3), WATERMARK FOR c0 AS c0 - INTERVAL '3' SECOND)"
                        + " WITH ('k1' = 'v1', 'k2' = 'v2') as SELECT * FROM t1";
        Schema tableSchema =
                Schema.newBuilder()
                        .column("c0", DataTypes.TIMESTAMP(3))
                        .column("a", DataTypes.BIGINT().notNull())
                        .column("b", DataTypes.STRING())
                        .column("c", DataTypes.INT())
                        .column("d", DataTypes.STRING())
                        .watermark("c0", "`c0` - INTERVAL '3' SECOND")
                        .build();

        testCommonReplaceTableAs(sql, tableName, null, tableSchema, null, Collections.emptyList());
    }

    private void testCommonReplaceTableAs(
            String sql, String tableName, @Nullable String tableComment) {
        testCommonReplaceTableAs(
                sql,
                tableName,
                tableComment,
                getDefaultTableSchema(),
                null,
                Collections.emptyList());
    }

    private void testCommonReplaceTableAs(
            String sql,
            String tableName,
            @Nullable String tableComment,
            Schema tableSchema,
            @Nullable TableDistribution distribution,
            List<String> partitionKey) {
        ObjectIdentifier expectedIdentifier = ObjectIdentifier.of("builtin", "default", tableName);
        Operation operation = parseAndConvert(sql);
        CatalogTable expectedCatalogTable =
                CatalogTable.newBuilder()
                        .schema(tableSchema)
                        .comment(tableComment)
                        .distribution(distribution)
                        .options(getDefaultTableOptions())
                        .partitionKeys(partitionKey)
                        .build();
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
        assertThat(actualCatalogTable.getDistribution())
                .isEqualTo(expectedCatalogTable.getDistribution());
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
                            DataTypes.BIGINT().notNull(),
                            DataTypes.STRING(),
                            DataTypes.INT(),
                            DataTypes.STRING()
                        })
                .build();
    }
}
