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
import org.apache.flink.table.operations.CreateTableASOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.parse.CalciteParser;
import org.apache.flink.table.planner.utils.TestSimpleDynamicTableSourceFactory;

import org.apache.calcite.sql.SqlNode;
import org.assertj.core.api.HamcrestCondition;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.apache.flink.table.planner.utils.OperationMatchers.isCreateTableOperation;
import static org.apache.flink.table.planner.utils.OperationMatchers.partitionedBy;
import static org.apache.flink.table.planner.utils.OperationMatchers.withDistribution;
import static org.apache.flink.table.planner.utils.OperationMatchers.withNoDistribution;
import static org.apache.flink.table.planner.utils.OperationMatchers.withSchema;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test base for testing convert CREATE TABLE AS statement to operation. */
class SqlCTASNodeToOperationTest extends SqlNodeToOperationConversionTestBase {

    private static final Map<String, String> TABLE_OPTIONS =
            Map.of("connector", TestSimpleDynamicTableSourceFactory.IDENTIFIER());

    @Test
    void testCreateTableAsWithNotFoundColumnIdentifiers() {
        CatalogTable catalogTable =
                CatalogTable.newBuilder()
                        .schema(
                                Schema.newBuilder()
                                        .column("f0", DataTypes.INT().notNull())
                                        .column("f1", DataTypes.INT())
                                        .build())
                        .options(TABLE_OPTIONS)
                        .build();

        catalogManager.createTable(
                catalogTable, ObjectIdentifier.of("builtin", "default", "src1"), false);

        final String sql = "create table tbl1 (f1, f2) AS SELECT * FROM src1";

        assertThatThrownBy(() -> parseAndConvert(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Column 'f2' not found in the source schema.");
    }

    @Test
    void testCreateTableAsWithMismatchIdentifiersLength() {
        CatalogTable catalogTable =
                CatalogTable.newBuilder()
                        .schema(
                                Schema.newBuilder()
                                        .column("f0", DataTypes.INT().notNull())
                                        .column("f1", DataTypes.INT())
                                        .build())
                        .options(TABLE_OPTIONS)
                        .build();

        catalogManager.createTable(
                catalogTable, ObjectIdentifier.of("builtin", "default", "src1"), false);

        final String sql = "create table tbl1 (f1) AS SELECT * FROM src1";

        assertThatThrownBy(() -> parseAndConvert(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "The number of columns in the column list "
                                + "must match the number of columns in the source schema.");
    }

    @Test
    void testCreateTableAsWithColumns() {
        CatalogTable catalogTable =
                CatalogTable.newBuilder()
                        .schema(
                                Schema.newBuilder()
                                        .column("f0", DataTypes.INT().notNull())
                                        .column("f1", DataTypes.TIMESTAMP(3))
                                        .build())
                        .options(TABLE_OPTIONS)
                        .build();

        catalogManager.createTable(
                catalogTable, ObjectIdentifier.of("builtin", "default", "src1"), false);

        final String sql =
                "create table tbl1 (c0 int, c1 double metadata, c2 as c0 * f0, c3 timestamp(3), c4 int metadata virtual, "
                        + "watermark FOR c3 AS c3 - interval '3' second) "
                        + "AS SELECT * FROM src1";

        Operation ctas = parseAndConvert(sql);
        Operation operation = ((CreateTableASOperation) ctas).getCreateTableOperation();
        assertThat(operation)
                .is(
                        new HamcrestCondition<>(
                                isCreateTableOperation(
                                        withNoDistribution(),
                                        withSchema(
                                                Schema.newBuilder()
                                                        .column("c0", DataTypes.INT())
                                                        .columnByMetadata("c1", DataTypes.DOUBLE())
                                                        .columnByExpression("c2", "`c0` * `f0`")
                                                        .column("c3", DataTypes.TIMESTAMP(3))
                                                        .columnByMetadata(
                                                                "c4", DataTypes.INT(), true)
                                                        .column("f0", DataTypes.INT().notNull())
                                                        .column("f1", DataTypes.TIMESTAMP(3))
                                                        .watermark(
                                                                "c3", "`c3` - INTERVAL '3' SECOND")
                                                        .build()))));
    }

    @Test
    void testCreateTableAsWithColumnsOverridden() {
        CatalogTable catalogTable =
                CatalogTable.newBuilder()
                        .schema(
                                Schema.newBuilder()
                                        .column("f0", DataTypes.INT().notNull())
                                        .column("f1", DataTypes.INT())
                                        .column("f2", DataTypes.TIMESTAMP(3))
                                        .column("f3", DataTypes.STRING())
                                        .build())
                        .options(TABLE_OPTIONS)
                        .build();

        catalogManager.createTable(
                catalogTable, ObjectIdentifier.of("builtin", "default", "src1"), false);

        final String sql =
                "create table tbl1 (c0 int, f0 bigint not null, a1 double, f2 timestamp(3) metadata, a3 string metadata) "
                        + "AS SELECT f0, f1 as `a1`, f2, f3 as `a3` FROM src1";

        Operation ctas = parseAndConvert(sql);
        Operation operation = ((CreateTableASOperation) ctas).getCreateTableOperation();
        assertThat(operation)
                .is(
                        new HamcrestCondition<>(
                                isCreateTableOperation(
                                        withNoDistribution(),
                                        withSchema(
                                                Schema.newBuilder()
                                                        .column("c0", DataTypes.INT())
                                                        .column("f0", DataTypes.BIGINT().notNull())
                                                        .column("a1", DataTypes.DOUBLE())
                                                        .columnByMetadata(
                                                                "f2", DataTypes.TIMESTAMP(3))
                                                        .columnByMetadata("a3", DataTypes.STRING())
                                                        .build()))));
    }

    @Test
    void testCreateTableAsWithOverriddenVirtualMetadataColumnsNotAllowed() {
        CatalogTable catalogTable =
                CatalogTable.newBuilder()
                        .schema(
                                Schema.newBuilder()
                                        .column("f0", DataTypes.INT().notNull())
                                        .column("f1", DataTypes.BIGINT())
                                        .build())
                        .options(TABLE_OPTIONS)
                        .build();

        catalogManager.createTable(
                catalogTable, ObjectIdentifier.of("builtin", "default", "src1"), false);

        final String sql =
                "create table tbl1 (f1 bigint metadata virtual) " + "AS SELECT * FROM src1";

        assertThatThrownBy(() -> parseAndConvert(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "A column named 'f1' already exists in the source schema. "
                                + "Virtual metadata columns cannot overwrite columns from "
                                + "source.");
    }

    @Test
    void testCreateTableAsWithOverriddenComputedColumnsNotAllowed() {
        CatalogTable catalogTable =
                CatalogTable.newBuilder()
                        .schema(
                                Schema.newBuilder()
                                        .column("f0", DataTypes.INT().notNull())
                                        .column("f1", DataTypes.BIGINT())
                                        .build())
                        .options(TABLE_OPTIONS)
                        .build();

        catalogManager.createTable(
                catalogTable, ObjectIdentifier.of("builtin", "default", "src1"), false);

        final String sql = "create table tbl1 (f1 as 'f0 * 2') " + "AS SELECT * FROM src1";

        assertThatThrownBy(() -> parseAndConvert(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "A column named 'f1' already exists in the source schema. "
                                + "Computed columns cannot overwrite columns from source.");
    }

    @Test
    void testCreateTableAsWithPrimaryAndPartitionKey() {
        CatalogTable catalogTable =
                CatalogTable.newBuilder()
                        .schema(
                                Schema.newBuilder()
                                        .column("f0", DataTypes.INT().notNull())
                                        .column("f1", DataTypes.TIMESTAMP(3))
                                        .build())
                        .options(TABLE_OPTIONS)
                        .build();

        catalogManager.createTable(
                catalogTable, ObjectIdentifier.of("builtin", "default", "src1"), false);

        final String sql =
                "create table tbl1 (PRIMARY KEY (f0) NOT ENFORCED) "
                        + "PARTITIONED BY (f0) AS SELECT * FROM src1";

        Operation ctas = parseAndConvert(sql);
        Operation operation = ((CreateTableASOperation) ctas).getCreateTableOperation();
        assertThat(operation)
                .is(
                        new HamcrestCondition<>(
                                isCreateTableOperation(
                                        withNoDistribution(),
                                        partitionedBy("f0"),
                                        withSchema(
                                                Schema.newBuilder()
                                                        .column("f0", DataTypes.INT().notNull())
                                                        .column("f1", DataTypes.TIMESTAMP(3))
                                                        .primaryKey("f0")
                                                        .build()))));
    }

    @Test
    void testCreateTableAsWithWatermark() {
        CatalogTable catalogTable =
                CatalogTable.newBuilder()
                        .schema(
                                Schema.newBuilder()
                                        .column("f0", DataTypes.INT().notNull())
                                        .column("f1", DataTypes.TIMESTAMP(3))
                                        .build())
                        .options(TABLE_OPTIONS)
                        .build();

        catalogManager.createTable(
                catalogTable, ObjectIdentifier.of("builtin", "default", "src1"), false);

        final String sql =
                "create table tbl1 (WATERMARK FOR f1 AS f1 - INTERVAL '3' SECOND) "
                        + "AS SELECT * FROM src1";

        Operation ctas = parseAndConvert(sql);
        Operation operation = ((CreateTableASOperation) ctas).getCreateTableOperation();
        assertThat(operation)
                .is(
                        new HamcrestCondition<>(
                                isCreateTableOperation(
                                        withNoDistribution(),
                                        withSchema(
                                                Schema.newBuilder()
                                                        .column("f0", DataTypes.INT().notNull())
                                                        .column("f1", DataTypes.TIMESTAMP(3))
                                                        .watermark(
                                                                "f1", "`f1` - INTERVAL '3' SECOND")
                                                        .build()))));
    }

    @Test
    void testCreateTableAsWithNotNullColumnsAreNotAllowed() {
        CatalogTable catalogTable =
                CatalogTable.newBuilder()
                        .schema(
                                Schema.newBuilder()
                                        .column("f0", DataTypes.INT().notNull())
                                        .column("f1", DataTypes.INT())
                                        .build())
                        .options(TABLE_OPTIONS)
                        .build();

        catalogManager.createTable(
                catalogTable, ObjectIdentifier.of("builtin", "default", "src1"), false);

        final String sql = "create table tbl1 (c0 int not null) " + "AS SELECT * FROM src1";

        assertThatThrownBy(() -> parseAndConvert(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Column 'c0' has no default value and does not allow NULLs.");
    }

    @Test
    void testCreateTableAsWithIncompatibleImplicitCastTypes() {
        CatalogTable catalogTable =
                CatalogTable.newBuilder()
                        .schema(
                                Schema.newBuilder()
                                        .column("f0", DataTypes.INT().notNull())
                                        .column("f1", DataTypes.TIMESTAMP(3))
                                        .build())
                        .options(TABLE_OPTIONS)
                        .build();

        catalogManager.createTable(
                catalogTable, ObjectIdentifier.of("builtin", "default", "src1"), false);

        final String sql = "create table tbl1 (f0 boolean) AS SELECT * FROM src1";

        assertThatThrownBy(() -> parseAndConvert(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Incompatible types for sink column 'f0' at position 0. "
                                + "The source column has type 'INT NOT NULL', while the target "
                                + "column has type 'BOOLEAN'.");
    }

    @Test
    void testMergingCreateTableAsWithDistribution() {
        CatalogTable catalogTable =
                CatalogTable.newBuilder()
                        .schema(
                                Schema.newBuilder()
                                        .column("f0", DataTypes.INT().notNull())
                                        .column("f1", DataTypes.TIMESTAMP(3))
                                        .columnByExpression("f2", "`f0` + 12345")
                                        .watermark("f1", "`f1` - interval '1' second")
                                        .build())
                        .options(TABLE_OPTIONS)
                        .distribution(TableDistribution.ofHash(Collections.singletonList("f0"), 3))
                        .partitionKeys(Arrays.asList("f0", "f1"))
                        .build();

        catalogManager.createTable(
                catalogTable, ObjectIdentifier.of("builtin", "default", "sourceTable"), false);

        final String sql =
                "create table derivedTable DISTRIBUTED BY HASH(f0) INTO 2 BUCKETS "
                        + "AS SELECT * FROM sourceTable";

        Operation ctas = parseAndConvert(sql);
        Operation operation = ((CreateTableASOperation) ctas).getCreateTableOperation();
        assertThat(operation)
                .is(
                        new HamcrestCondition<>(
                                isCreateTableOperation(
                                        withDistribution(
                                                TableDistribution.ofHash(
                                                        Collections.singletonList("f0"), 2)),
                                        withSchema(
                                                Schema.newBuilder()
                                                        .column("f0", DataTypes.INT().notNull())
                                                        .column("f1", DataTypes.TIMESTAMP(3))
                                                        .column("f2", DataTypes.INT().notNull())
                                                        .build()))));
    }

    @Test
    void testMergingCreateTableAsWitEmptyDistribution() {
        CatalogTable catalogTable =
                CatalogTable.newBuilder()
                        .schema(
                                Schema.newBuilder()
                                        .column("f0", DataTypes.INT().notNull())
                                        .column("f1", DataTypes.TIMESTAMP(3))
                                        .columnByExpression("f2", "`f0` + 12345")
                                        .watermark("f1", "`f1` - interval '1' second")
                                        .build())
                        .distribution(TableDistribution.ofHash(Collections.singletonList("f0"), 3))
                        .partitionKeys(Arrays.asList("f0", "f1"))
                        .options(TABLE_OPTIONS)
                        .build();

        catalogManager.createTable(
                catalogTable, ObjectIdentifier.of("builtin", "default", "sourceTable"), false);

        final String sql = "create table derivedTable AS SELECT * FROM sourceTable";
        Operation ctas = parseAndConvert(sql);
        Operation operation = ((CreateTableASOperation) ctas).getCreateTableOperation();
        assertThat(operation)
                .is(
                        new HamcrestCondition<>(
                                isCreateTableOperation(
                                        withNoDistribution(),
                                        withSchema(
                                                Schema.newBuilder()
                                                        .column("f0", DataTypes.INT().notNull())
                                                        .column("f1", DataTypes.TIMESTAMP(3))
                                                        .column("f2", DataTypes.INT().notNull())
                                                        .build()))));
    }

    private Operation parseAndConvert(String sql) {
        final FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
        final CalciteParser parser = getParserBySqlDialect(SqlDialect.DEFAULT);

        SqlNode node = parser.parse(sql);
        return SqlNodeToOperationConversion.convert(planner, catalogManager, node).get();
    }
}
