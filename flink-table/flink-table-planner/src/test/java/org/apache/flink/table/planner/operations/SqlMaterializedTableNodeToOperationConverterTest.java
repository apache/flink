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

import org.apache.flink.sql.parser.error.SqlValidateException;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.FunctionDescriptor;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.CatalogMaterializedTable.LogicalRefreshMode;
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshMode;
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshStatus;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.IntervalFreshness;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.catalog.WatermarkSpec;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.materializedtable.AlterMaterializedTableAsQueryOperation;
import org.apache.flink.table.operations.materializedtable.AlterMaterializedTableChangeOperation;
import org.apache.flink.table.operations.materializedtable.AlterMaterializedTableRefreshOperation;
import org.apache.flink.table.operations.materializedtable.AlterMaterializedTableResumeOperation;
import org.apache.flink.table.operations.materializedtable.AlterMaterializedTableSuspendOperation;
import org.apache.flink.table.operations.materializedtable.CreateMaterializedTableOperation;
import org.apache.flink.table.operations.materializedtable.DropMaterializedTableOperation;
import org.apache.flink.table.planner.utils.TableFunc0;

import org.apache.flink.shaded.guava33.com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for the materialized table statements for {@link SqlNodeToOperationConversion}. */
class SqlMaterializedTableNodeToOperationConverterTest
        extends SqlNodeToOperationConversionTestBase {

    private static final String CREATE_OPERATION = "CREATE ";
    private static final String CREATE_OR_ALTER_OPERATION = "CREATE OR ALTER ";

    @BeforeEach
    void before() throws TableAlreadyExistException, DatabaseNotExistException {
        super.before();
        final ObjectPath path3 = new ObjectPath(catalogManager.getCurrentDatabase(), "t3");
        final ResolvedSchema t3TableSchema =
                ResolvedSchema.of(
                        Column.physical("a", DataTypes.BIGINT().notNull()),
                        Column.physical("b", DataTypes.VARCHAR(Integer.MAX_VALUE)),
                        Column.physical("c", DataTypes.INT()),
                        Column.physical("d", DataTypes.VARCHAR(Integer.MAX_VALUE)));
        final Schema tableSchema = Schema.newBuilder().fromResolvedSchema(t3TableSchema).build();
        Map<String, String> options = new HashMap<>();
        options.put("connector", "COLLECTION");
        final CatalogTable catalogTable =
                CatalogTable.newBuilder()
                        .schema(tableSchema)
                        .comment("")
                        .partitionKeys(Arrays.asList("b", "c"))
                        .options(options)
                        .build();
        catalog.createTable(path3, catalogTable, true);

        // create materialized table
        final String sqlWithConstraint =
                "CREATE MATERIALIZED TABLE base_mtbl (\n"
                        + "   CONSTRAINT ct1 PRIMARY KEY(a) NOT ENFORCED"
                        + ")\n"
                        + "COMMENT 'materialized table comment'\n"
                        + "PARTITIONED BY (a, d)\n"
                        + "WITH (\n"
                        + "  'connector' = 'filesystem', \n"
                        + "  'format' = 'json'\n"
                        + ")\n"
                        + "FRESHNESS = INTERVAL '30' SECOND\n"
                        + "REFRESH_MODE = FULL\n"
                        + "AS SELECT * FROM t1";
        createMaterializedTableInCatalog(sqlWithConstraint, "base_mtbl");

        // MATERIALIZED TABLE with WATERMARK
        final String sqlWithWatermark =
                "CREATE MATERIALIZED TABLE base_mtbl_with_watermark (\n"
                        + "   t AS current_timestamp,"
                        + "   CONSTRAINT ct1 PRIMARY KEY(a) NOT ENFORCED,"
                        + "   WATERMARK FOR t as current_timestamp - INTERVAL '5' SECOND"
                        + ")\n"
                        + "COMMENT 'materialized table comment'\n"
                        + "PARTITIONED BY (a, d)\n"
                        + "WITH (\n"
                        + "  'connector' = 'filesystem', \n"
                        + "  'format' = 'json'\n"
                        + ")\n"
                        + "FRESHNESS = INTERVAL '30' SECOND\n"
                        + "REFRESH_MODE = FULL\n"
                        + "AS SELECT t1.* FROM t1";
        createMaterializedTableInCatalog(sqlWithWatermark, "base_mtbl_with_watermark");

        // MATERIALIZED TABLE without constraint
        final String sqlWithoutConstraint =
                "CREATE MATERIALIZED TABLE base_mtbl_without_constraint "
                        + "COMMENT 'materialized table comment'\n"
                        + "PARTITIONED BY (a, d)\n"
                        + "WITH (\n"
                        + "  'connector' = 'filesystem', \n"
                        + "  'format' = 'json'\n"
                        + ")\n"
                        + "FRESHNESS = INTERVAL '30' SECOND\n"
                        + "REFRESH_MODE = FULL\n"
                        + "AS SELECT t1.* FROM t1";

        createMaterializedTableInCatalog(sqlWithoutConstraint, "base_mtbl_without_constraint");
    }

    @Test
    void testCreateMaterializedTable() {
        final String sql =
                "CREATE MATERIALIZED TABLE mtbl1 (\n"
                        + "   CONSTRAINT ct1 PRIMARY KEY(a) NOT ENFORCED"
                        + ")\n"
                        + "COMMENT 'materialized table comment'\n"
                        + "PARTITIONED BY (a, d)\n"
                        + "WITH (\n"
                        + "  'connector' = 'filesystem', \n"
                        + "  'format' = 'json'\n"
                        + ")\n"
                        + "FRESHNESS = INTERVAL '30' SECOND\n"
                        + "REFRESH_MODE = FULL\n"
                        + "AS SELECT * FROM t1";

        ResolvedCatalogMaterializedTable materializedTable =
                createResolvedCatalogMaterializedTable(sql);

        final IntervalFreshness resolvedFreshness = materializedTable.getDefinitionFreshness();
        assertThat(resolvedFreshness).isEqualTo(IntervalFreshness.ofSecond("30"));

        final RefreshMode resolvedRefreshMode = materializedTable.getRefreshMode();
        assertThat(resolvedRefreshMode).isSameAs(RefreshMode.FULL);

        final CatalogMaterializedTable expected =
                getDefaultMaterializedTableBuilder()
                        .freshness(IntervalFreshness.ofSecond("30"))
                        .logicalRefreshMode(LogicalRefreshMode.FULL)
                        .refreshMode(RefreshMode.FULL)
                        .refreshStatus(RefreshStatus.INITIALIZING)
                        .build();
        assertThat(materializedTable.getOrigin()).isEqualTo(expected);
    }

    @Test
    void testCreateMaterializedTableWithoutFreshness() {
        final String sql =
                "CREATE MATERIALIZED TABLE mtbl1 (\n"
                        + "   CONSTRAINT ct1 PRIMARY KEY(a) NOT ENFORCED"
                        + ")\n"
                        + "COMMENT 'materialized table comment'\n"
                        + "PARTITIONED BY (a, d)\n"
                        + "WITH (\n"
                        + "  'connector' = 'filesystem', \n"
                        + "  'format' = 'json'\n"
                        + ")\n"
                        + "REFRESH_MODE = FULL\n"
                        + "AS SELECT * FROM t1";
        ResolvedCatalogMaterializedTable materializedTable =
                createResolvedCatalogMaterializedTable(sql);

        // The resolved freshness should default to 1 minute
        final IntervalFreshness resolvedFreshness = materializedTable.getDefinitionFreshness();
        assertThat(resolvedFreshness).isEqualTo(IntervalFreshness.ofHour("1"));

        final RefreshMode resolvedRefreshMode = materializedTable.getRefreshMode();
        assertThat(resolvedRefreshMode).isSameAs(RefreshMode.FULL);

        final CatalogMaterializedTable expected =
                getDefaultMaterializedTableBuilder()
                        .logicalRefreshMode(LogicalRefreshMode.FULL)
                        .refreshMode(RefreshMode.FULL)
                        .refreshStatus(RefreshStatus.INITIALIZING)
                        .build();
        assertThat(materializedTable.getOrigin()).isEqualTo(expected);
    }

    @Test
    void testCreateMaterializedTableWithoutFreshnessAndRefreshMode() {
        final String sql =
                "CREATE MATERIALIZED TABLE mtbl1 (\n"
                        + "   CONSTRAINT ct1 PRIMARY KEY(a) NOT ENFORCED"
                        + ")\n"
                        + "COMMENT 'materialized table comment'\n"
                        + "PARTITIONED BY (a, d)\n"
                        + "WITH (\n"
                        + "  'connector' = 'filesystem', \n"
                        + "  'format' = 'json'\n"
                        + ")\n"
                        + "AS SELECT * FROM t1";
        ResolvedCatalogMaterializedTable materializedTable =
                createResolvedCatalogMaterializedTable(sql);

        final IntervalFreshness resolvedFreshness = materializedTable.getDefinitionFreshness();
        assertThat(resolvedFreshness).isEqualTo(IntervalFreshness.ofMinute("3"));
        final CatalogMaterializedTable expected =
                getDefaultMaterializedTableBuilder()
                        .logicalRefreshMode(LogicalRefreshMode.AUTOMATIC)
                        .refreshStatus(RefreshStatus.INITIALIZING)
                        .build();
        assertThat(materializedTable.getOrigin()).isEqualTo(expected);
    }

    @Test
    void testCreateMaterializedTableWithUDTFQuery() {
        functionCatalog.registerCatalogFunction(
                UnresolvedIdentifier.of(
                        ObjectIdentifier.of(
                                catalogManager.getCurrentCatalog(), "default", "myFunc")),
                FunctionDescriptor.forFunctionClass(TableFunc0.class).build(),
                true);

        final String sql =
                "CREATE MATERIALIZED TABLE mtbl1 (\n"
                        + "   CONSTRAINT ct1 PRIMARY KEY(a) NOT ENFORCED"
                        + ")\n"
                        + "COMMENT 'materialized table comment'\n"
                        + "PARTITIONED BY (a)\n"
                        + "WITH (\n"
                        + "  'connector' = 'filesystem', \n"
                        + "  'format' = 'json'\n"
                        + ")\n"
                        + "FRESHNESS = INTERVAL '30' SECOND\n"
                        + "REFRESH_MODE = FULL\n"
                        + "AS SELECT a, f1, f2 FROM t1, LATERAL TABLE(myFunc(b)) as T(f1, f2)";

        CreateMaterializedTableOperation createOperation = createMaterializedTableOperation(sql);

        assertThat(createOperation.getCatalogMaterializedTable().getExpandedQuery())
                .isEqualTo(
                        "SELECT `t1`.`a`, `T`.`f1`, `T`.`f2`\n"
                                + "FROM `builtin`.`default`.`t1` AS `t1`,\n"
                                + "LATERAL TABLE(`builtin`.`default`.`myFunc`(`b`)) AS `T` (`f1`, `f2`)");
    }

    @Test
    void testCreateMaterializedTableWithUDTFQueryWithoutAlias() {
        functionCatalog.registerCatalogFunction(
                UnresolvedIdentifier.of(
                        ObjectIdentifier.of(
                                catalogManager.getCurrentCatalog(), "default", "myFunc")),
                FunctionDescriptor.forFunctionClass(TableFunc0.class).build(),
                true);

        final String sql =
                "CREATE MATERIALIZED TABLE mtbl1 \n"
                        + "AS SELECT * FROM t1, LATERAL TABLE(myFunc(b))";
        CreateMaterializedTableOperation createOperation = createMaterializedTableOperation(sql);

        assertThat(createOperation.getCatalogMaterializedTable().getExpandedQuery())
                .isEqualTo(
                        "SELECT `t1`.`a`, `t1`.`b`, `t1`.`c`, `t1`.`d`, `EXPR$0`.`name`, `EXPR$0`.`age`\n"
                                + "FROM `builtin`.`default`.`t1` AS `t1`,\n"
                                + "LATERAL TABLE(`builtin`.`default`.`myFunc`(`b`)) AS `EXPR$0`");
    }

    @Test
    void testContinuousRefreshMode() {
        // test continuous mode derived by specify freshness automatically
        final String sql =
                "CREATE MATERIALIZED TABLE mtbl1\n"
                        + "FRESHNESS = INTERVAL '30' SECOND\n"
                        + "AS SELECT * FROM t1";
        ResolvedCatalogMaterializedTable materializedTable =
                createResolvedCatalogMaterializedTable(sql);

        assertThat(materializedTable.getLogicalRefreshMode())
                .isSameAs(LogicalRefreshMode.AUTOMATIC);
        assertThat(materializedTable.getRefreshMode()).isSameAs(RefreshMode.CONTINUOUS);

        // test continuous mode by manual specify
        final String sql2 =
                "CREATE MATERIALIZED TABLE mtbl1\n"
                        + "FRESHNESS = INTERVAL '30' DAY\n"
                        + "REFRESH_MODE = CONTINUOUS\n"
                        + "AS SELECT * FROM t1";
        ResolvedCatalogMaterializedTable materializedTable2 =
                createResolvedCatalogMaterializedTable(sql2);

        assertThat(materializedTable2.getLogicalRefreshMode())
                .isSameAs(LogicalRefreshMode.CONTINUOUS);
        assertThat(materializedTable2.getRefreshMode()).isSameAs(RefreshMode.CONTINUOUS);
    }

    @Test
    void testFullRefreshMode() {
        // test full mode derived by specify freshness automatically
        final String sql =
                "CREATE MATERIALIZED TABLE mtbl1\n"
                        + "FRESHNESS = INTERVAL '1' DAY\n"
                        + "AS SELECT * FROM t1";
        ResolvedCatalogMaterializedTable materializedTable =
                createResolvedCatalogMaterializedTable(sql);

        assertThat(materializedTable.getLogicalRefreshMode())
                .isSameAs(LogicalRefreshMode.AUTOMATIC);
        assertThat(materializedTable.getRefreshMode()).isSameAs(RefreshMode.FULL);

        // test full mode by manual specify
        final String sql2 =
                "CREATE MATERIALIZED TABLE mtbl1\n"
                        + "FRESHNESS = INTERVAL '30' SECOND\n"
                        + "REFRESH_MODE = FULL\n"
                        + "AS SELECT * FROM t1";
        ResolvedCatalogMaterializedTable materializedTable2 =
                createResolvedCatalogMaterializedTable(sql2);

        assertThat(materializedTable2.getLogicalRefreshMode()).isSameAs(LogicalRefreshMode.FULL);
        assertThat(materializedTable2.getRefreshMode()).isSameAs(RefreshMode.FULL);
    }

    @ParameterizedTest
    @MethodSource("testDataForCreateMaterializedTableFailedCase")
    void createMaterializedTableFailedCase(TestSpec spec) {
        assertThatThrownBy(() -> parse(spec.sql))
                .as(spec.sql)
                .isInstanceOf(spec.expectedException)
                .hasMessageContaining(spec.errMessage);
    }

    @ParameterizedTest
    @MethodSource("testDataWithDifferentSchemasSuccessCase")
    void createMaterializedTableSuccessCase(String sql, ResolvedSchema expected) {
        CreateMaterializedTableOperation operation = (CreateMaterializedTableOperation) parse(sql);
        assertThat(operation.getCatalogMaterializedTable().getResolvedSchema()).isEqualTo(expected);
    }

    @ParameterizedTest
    @MethodSource("alterSuccessCase")
    void createAlterTableSuccessCase(TestSpec testSpec) {
        AlterMaterializedTableChangeOperation operation =
                (AlterMaterializedTableChangeOperation) parse(testSpec.sql);
        CatalogMaterializedTable catalogMaterializedTable = operation.getCatalogMaterializedTable();
        assertThat(catalogMaterializedTable.getUnresolvedSchema())
                .hasToString(testSpec.expectedSchema);
    }

    @Test
    void createMaterializedTableWithWatermark() {
        final String sql =
                "CREATE MATERIALIZED TABLE users_shops (watermark for ts as ts - interval '2' second)"
                        + " FRESHNESS = INTERVAL '30' SECOND"
                        + " AS SELECT 1 as shop_id, 2 as user_id, cast(current_timestamp as timestamp(3)) ts";
        CreateMaterializedTableOperation operation = (CreateMaterializedTableOperation) parse(sql);
        ResolvedSchema schema = operation.getCatalogMaterializedTable().getResolvedSchema();
        assertThat(schema.getColumns())
                .containsExactly(
                        Column.physical("shop_id", DataTypes.INT().notNull()),
                        Column.physical("user_id", DataTypes.INT().notNull()),
                        Column.physical("ts", DataTypes.TIMESTAMP(3).notNull()));

        assertThat(schema.getWatermarkSpecs()).hasSize(1);
        WatermarkSpec watermarkSpec = schema.getWatermarkSpecs().get(0);
        assertThat(watermarkSpec.getWatermarkExpression())
                .hasToString("`ts` - INTERVAL '2' SECOND");
        assertThat(schema.getPrimaryKey()).isEmpty();
        assertThat(schema.getIndexes()).isEmpty();
    }

    @Test
    void createMaterializedTableWithWatermarkUsingColumnFromCreatePart() {
        final String sql =
                "CREATE MATERIALIZED TABLE users_shops (ts TIMESTAMP(3), watermark for ts as ts - interval '2' second)"
                        + " FRESHNESS = INTERVAL '30' SECOND"
                        + " AS SELECT current_timestamp() as ts, 1 as shop_id, 2 as user_id";
        CreateMaterializedTableOperation operation = (CreateMaterializedTableOperation) parse(sql);
        ResolvedSchema schema = operation.getCatalogMaterializedTable().getResolvedSchema();
        assertThat(schema.getColumns())
                .containsExactly(
                        Column.physical("ts", DataTypes.TIMESTAMP(3)),
                        Column.physical("shop_id", DataTypes.INT().notNull()),
                        Column.physical("user_id", DataTypes.INT().notNull()));

        assertThat(schema.getWatermarkSpecs()).hasSize(1);
        WatermarkSpec watermarkSpec = schema.getWatermarkSpecs().get(0);
        assertThat(watermarkSpec.getWatermarkExpression())
                .hasToString("`ts` - INTERVAL '2' SECOND");
        assertThat(schema.getPrimaryKey()).isEmpty();
        assertThat(schema.getIndexes()).isEmpty();
    }

    @Test
    void testAlterMaterializedTableRefreshOperationWithPartitionSpec() {
        final String sql =
                "ALTER MATERIALIZED TABLE mtbl1 REFRESH PARTITION (ds1 = '1', ds2 = '2')";

        Operation operation = parse(sql);
        assertThat(operation).isInstanceOf(AlterMaterializedTableRefreshOperation.class);

        AlterMaterializedTableRefreshOperation op =
                (AlterMaterializedTableRefreshOperation) operation;
        assertThat(op.getTableIdentifier().toString()).isEqualTo("`builtin`.`default`.`mtbl1`");
        assertThat(op.getPartitionSpec()).isEqualTo(ImmutableMap.of("ds1", "1", "ds2", "2"));
    }

    @Test
    void testAlterMaterializedTableRefreshOperationWithoutPartitionSpec() {
        final String sql = "ALTER MATERIALIZED TABLE mtbl1 REFRESH";

        Operation operation = parse(sql);
        assertThat(operation).isInstanceOf(AlterMaterializedTableRefreshOperation.class);

        AlterMaterializedTableRefreshOperation op =
                (AlterMaterializedTableRefreshOperation) operation;
        assertThat(op.getTableIdentifier().toString()).isEqualTo("`builtin`.`default`.`mtbl1`");
        assertThat(op.getPartitionSpec()).isEmpty();
    }

    @Test
    void testAlterMaterializedTableSuspend() {
        final String sql = "ALTER MATERIALIZED TABLE mtbl1 SUSPEND";
        Operation operation = parse(sql);
        assertThat(operation).isInstanceOf(AlterMaterializedTableSuspendOperation.class);
    }

    @Test
    void testAlterMaterializedTableResume() {
        final String sql1 = "ALTER MATERIALIZED TABLE mtbl1 RESUME";
        Operation operation = parse(sql1);
        assertThat(operation).isInstanceOf(AlterMaterializedTableResumeOperation.class);
        assertThat(operation.asSummaryString())
                .isEqualTo("ALTER MATERIALIZED TABLE builtin.default.mtbl1 RESUME");

        final String sql2 = "ALTER MATERIALIZED TABLE mtbl1 RESUME WITH ('k1' = 'v1')";
        Operation operation2 = parse(sql2);
        assertThat(operation2).isInstanceOf(AlterMaterializedTableResumeOperation.class);
        assertThat(((AlterMaterializedTableResumeOperation) operation2).getDynamicOptions())
                .containsEntry("k1", "v1");
        assertThat(operation2.asSummaryString())
                .isEqualTo("ALTER MATERIALIZED TABLE builtin.default.mtbl1 RESUME WITH (k1: [v1])");
    }

    @Test
    void testAlterMaterializedTableAsQuery() throws TableNotExistException {
        String sql =
                "ALTER MATERIALIZED TABLE base_mtbl AS SELECT a, b, c, d, d as e, cast('123' as string) as f FROM t3";
        Operation operation = parse(sql);

        assertThat(operation).isInstanceOf(AlterMaterializedTableAsQueryOperation.class);

        AlterMaterializedTableAsQueryOperation op =
                (AlterMaterializedTableAsQueryOperation) operation;
        assertThat(op.getTableChanges())
                .isEqualTo(
                        Arrays.asList(
                                TableChange.add(
                                        Column.physical("e", DataTypes.VARCHAR(Integer.MAX_VALUE))),
                                TableChange.add(
                                        Column.physical("f", DataTypes.VARCHAR(Integer.MAX_VALUE))),
                                TableChange.modifyDefinitionQuery(
                                        "SELECT `t3`.`a`, `t3`.`b`, `t3`.`c`, `t3`.`d`, `t3`.`d` AS `e`, CAST('123' AS STRING) AS `f`\n"
                                                + "FROM `builtin`.`default`.`t3` AS `t3`")));
        assertThat(operation.asSummaryString())
                .isEqualTo(
                        "ALTER MATERIALIZED TABLE builtin.default.base_mtbl AS SELECT `t3`.`a`, `t3`.`b`, `t3`.`c`, `t3`.`d`, `t3`.`d` AS `e`, CAST('123' AS STRING) AS `f`\n"
                                + "FROM `builtin`.`default`.`t3` AS `t3`");

        // new table only difference schema & definition query with old table.
        CatalogMaterializedTable oldTable =
                (CatalogMaterializedTable)
                        catalog.getTable(
                                new ObjectPath(catalogManager.getCurrentDatabase(), "base_mtbl"));
        CatalogMaterializedTable newTable = op.getCatalogMaterializedTable();

        assertThat(oldTable.getUnresolvedSchema()).isNotEqualTo(newTable.getUnresolvedSchema());
        assertThat(oldTable.getUnresolvedSchema().getPrimaryKey())
                .isEqualTo(newTable.getUnresolvedSchema().getPrimaryKey());
        assertThat(oldTable.getUnresolvedSchema().getWatermarkSpecs())
                .isEqualTo(newTable.getUnresolvedSchema().getWatermarkSpecs());
        assertThat(oldTable.getOriginalQuery()).isNotEqualTo(newTable.getOriginalQuery());
        assertThat(oldTable.getExpandedQuery()).isNotEqualTo(newTable.getExpandedQuery());
        assertThat(oldTable.getDefinitionFreshness()).isEqualTo(newTable.getDefinitionFreshness());
        assertThat(oldTable.getRefreshMode()).isEqualTo(newTable.getRefreshMode());
        assertThat(oldTable.getRefreshStatus()).isEqualTo(newTable.getRefreshStatus());
        assertThat(oldTable.getSerializedRefreshHandler())
                .isEqualTo(newTable.getSerializedRefreshHandler());

        List<Schema.UnresolvedColumn> addedColumn =
                newTable.getUnresolvedSchema().getColumns().stream()
                        .filter(
                                column ->
                                        !oldTable.getUnresolvedSchema()
                                                .getColumns()
                                                .contains(column))
                        .collect(Collectors.toList());
        // added column should be a nullable column.
        assertThat(addedColumn)
                .isEqualTo(
                        Arrays.asList(
                                new Schema.UnresolvedPhysicalColumn(
                                        "e", DataTypes.VARCHAR(Integer.MAX_VALUE)),
                                new Schema.UnresolvedPhysicalColumn(
                                        "f", DataTypes.VARCHAR(Integer.MAX_VALUE))));
    }

    @Test
    void testAlterMaterializedTableAsQueryWithConflictColumnName() {
        String sql5 = "ALTER MATERIALIZED TABLE base_mtbl AS SELECT a, b, c, d, c as a FROM t3";
        AlterMaterializedTableAsQueryOperation sqlAlterMaterializedTableAsQuery =
                (AlterMaterializedTableAsQueryOperation) parse(sql5);

        assertThat(sqlAlterMaterializedTableAsQuery.getTableChanges())
                .isEqualTo(
                        Arrays.asList(
                                TableChange.add(Column.physical("a0", DataTypes.INT())),
                                TableChange.modifyDefinitionQuery(
                                        "SELECT `t3`.`a`, `t3`.`b`, `t3`.`c`, `t3`.`d`, `t3`.`c` AS `a`\n"
                                                + "FROM `builtin`.`default`.`t3` AS `t3`")));
    }

    @Test
    void testDropMaterializedTable() {
        final String sql = "DROP MATERIALIZED TABLE mtbl1";
        Operation operation = parse(sql);
        assertThat(operation).isInstanceOf(DropMaterializedTableOperation.class);
        assertThat(((DropMaterializedTableOperation) operation).isIfExists()).isFalse();
        assertThat(operation.asSummaryString())
                .isEqualTo(
                        "DROP MATERIALIZED TABLE: (identifier: [`builtin`.`default`.`mtbl1`], IfExists: [false])");

        final String sql2 = "DROP MATERIALIZED TABLE IF EXISTS mtbl1";
        Operation operation2 = parse(sql2);
        assertThat(operation2).isInstanceOf(DropMaterializedTableOperation.class);
        assertThat(((DropMaterializedTableOperation) operation2).isIfExists()).isTrue();

        assertThat(operation2.asSummaryString())
                .isEqualTo(
                        "DROP MATERIALIZED TABLE: (identifier: [`builtin`.`default`.`mtbl1`], IfExists: [true])");
    }

    @Test
    void testCreateOrAlterMaterializedTable() {
        final String sql =
                "CREATE OR ALTER MATERIALIZED TABLE mtbl1 (\n"
                        + "   CONSTRAINT ct1 PRIMARY KEY(a) NOT ENFORCED"
                        + ")\n"
                        + "COMMENT 'materialized table comment'\n"
                        + "PARTITIONED BY (a, d)\n"
                        + "WITH (\n"
                        + "  'connector' = 'filesystem', \n"
                        + "  'format' = 'json'\n"
                        + ")\n"
                        + "FRESHNESS = INTERVAL '30' SECOND\n"
                        + "REFRESH_MODE = FULL\n"
                        + "AS SELECT * FROM t1";
        Operation operation = parse(sql);
        assertThat(operation).isInstanceOf(CreateMaterializedTableOperation.class);

        CreateMaterializedTableOperation op = (CreateMaterializedTableOperation) operation;
        ResolvedCatalogMaterializedTable materializedTable = op.getCatalogMaterializedTable();
        assertThat(materializedTable).isInstanceOf(ResolvedCatalogMaterializedTable.class);

        Map<String, String> options = new HashMap<>();
        options.put("connector", "filesystem");
        options.put("format", "json");
        final CatalogMaterializedTable expected =
                getDefaultMaterializedTableBuilder()
                        .freshness(IntervalFreshness.ofSecond("30"))
                        .logicalRefreshMode(LogicalRefreshMode.FULL)
                        .refreshMode(RefreshMode.FULL)
                        .refreshStatus(RefreshStatus.INITIALIZING)
                        .build();

        assertThat(materializedTable.getOrigin()).isEqualTo(expected);
    }

    @Test
    void testCreateOrAlterMaterializedTableForExistingTable() throws TableNotExistException {
        final String sql =
                "CREATE OR ALTER MATERIALIZED TABLE base_mtbl (\n"
                        + "   CONSTRAINT ct1 PRIMARY KEY(a) NOT ENFORCED"
                        + ")\n"
                        + "COMMENT 'materialized table comment'\n"
                        + "PARTITIONED BY (a, d)\n"
                        + "WITH (\n"
                        + "  'connector' = 'filesystem', \n"
                        + "  'format' = 'json'\n"
                        + ")\n"
                        + "FRESHNESS = INTERVAL '30' SECOND\n"
                        + "REFRESH_MODE = FULL\n"
                        + "AS SELECT a, b, c, d, d as e, cast('123' as string) as f FROM t3";
        Operation operation = parse(sql);

        assertThat(operation).isInstanceOf(AlterMaterializedTableAsQueryOperation.class);

        AlterMaterializedTableAsQueryOperation op =
                (AlterMaterializedTableAsQueryOperation) operation;
        assertThat(op.getTableChanges())
                .isEqualTo(
                        Arrays.asList(
                                TableChange.add(
                                        Column.physical("e", DataTypes.VARCHAR(Integer.MAX_VALUE))),
                                TableChange.add(
                                        Column.physical("f", DataTypes.VARCHAR(Integer.MAX_VALUE))),
                                TableChange.modifyDefinitionQuery(
                                        "SELECT `t3`.`a`, `t3`.`b`, `t3`.`c`, `t3`.`d`, `t3`.`d` AS `e`, CAST('123' AS STRING) AS `f`\n"
                                                + "FROM `builtin`.`default`.`t3` AS `t3`")));
        assertThat(operation.asSummaryString())
                .isEqualTo(
                        "ALTER MATERIALIZED TABLE builtin.default.base_mtbl AS SELECT `t3`.`a`, `t3`.`b`, `t3`.`c`, `t3`.`d`, `t3`.`d` AS `e`, CAST('123' AS STRING) AS `f`\n"
                                + "FROM `builtin`.`default`.`t3` AS `t3`");

        // new table only difference schema & definition query with old table.
        CatalogMaterializedTable oldTable =
                (CatalogMaterializedTable)
                        catalog.getTable(
                                new ObjectPath(catalogManager.getCurrentDatabase(), "base_mtbl"));
        CatalogMaterializedTable newTable = op.getCatalogMaterializedTable();

        assertThat(oldTable.getUnresolvedSchema()).isNotEqualTo(newTable.getUnresolvedSchema());
        assertThat(oldTable.getUnresolvedSchema().getPrimaryKey())
                .isEqualTo(newTable.getUnresolvedSchema().getPrimaryKey());
        assertThat(oldTable.getUnresolvedSchema().getWatermarkSpecs())
                .isEqualTo(newTable.getUnresolvedSchema().getWatermarkSpecs());
        assertThat(oldTable.getOriginalQuery()).isNotEqualTo(newTable.getOriginalQuery());
        assertThat(oldTable.getExpandedQuery()).isNotEqualTo(newTable.getExpandedQuery());
        assertThat(oldTable.getDefinitionFreshness()).isEqualTo(newTable.getDefinitionFreshness());
        assertThat(oldTable.getRefreshMode()).isEqualTo(newTable.getRefreshMode());
        assertThat(oldTable.getRefreshStatus()).isEqualTo(newTable.getRefreshStatus());
        assertThat(oldTable.getSerializedRefreshHandler())
                .isEqualTo(newTable.getSerializedRefreshHandler());

        List<Schema.UnresolvedColumn> addedColumn =
                newTable.getUnresolvedSchema().getColumns().stream()
                        .filter(
                                column ->
                                        !oldTable.getUnresolvedSchema()
                                                .getColumns()
                                                .contains(column))
                        .collect(Collectors.toList());
        // added column should be a nullable column.
        assertThat(addedColumn)
                .isEqualTo(
                        Arrays.asList(
                                new Schema.UnresolvedPhysicalColumn(
                                        "e", DataTypes.VARCHAR(Integer.MAX_VALUE)),
                                new Schema.UnresolvedPhysicalColumn(
                                        "f", DataTypes.VARCHAR(Integer.MAX_VALUE))));
    }

    private static Collection<TestSpec> testDataForCreateMaterializedTableFailedCase() {
        final Collection<TestSpec> list = new ArrayList<>();
        list.addAll(createWithInvalidSchema());
        list.addAll(createWithInvalidFreshness());
        list.addAll(createWithInvalidPartitions());
        list.addAll(alterWithInvalidSchema());
        list.addAll(alterQuery());
        return list;
    }

    private static Collection<TestSpec> alterQuery() {
        return List.of(
                TestSpec.of(
                        "ALTER MATERIALIZED TABLE base_mtbl AS SELECT a, b FROM t3",
                        "Failed to modify query because drop column is unsupported. When modifying "
                                + "a query, you can only append new columns at the end of original "
                                + "schema. The original schema has 4 columns, but the newly derived "
                                + "schema from the query has 2 columns."),
                TestSpec.of(
                        "ALTER MATERIALIZED TABLE base_mtbl AS SELECT a, b, d, c FROM t3",
                        "When modifying the query of a materialized table, currently only support "
                                + "appending columns at the end of original schema, dropping, "
                                + "renaming, and reordering columns are not supported.\n"
                                + "Column mismatch at position 2: Original column is [`c` INT], "
                                + "but new column is [`d` STRING]."),
                TestSpec.of(
                        "ALTER MATERIALIZED TABLE base_mtbl AS SELECT a, b, c, CAST(d AS INT) AS d FROM t3",
                        "When modifying the query of a materialized table, currently only support "
                                + "appending columns at the end of original schema, dropping, "
                                + "renaming, and reordering columns are not supported.\n"
                                + "Column mismatch at position 3: Original column is [`d` STRING], "
                                + "but new column is [`d` INT]."),
                TestSpec.of(
                        "ALTER MATERIALIZED TABLE base_mtbl AS SELECT a, b, c, CAST('d' AS STRING) AS d FROM t3",
                        "When modifying the query of a materialized table, currently only support "
                                + "appending columns at the end of original schema, dropping, "
                                + "renaming, and reordering columns are not supported.\n"
                                + "Column mismatch at position 3: Original column is [`d` STRING], "
                                + "but new column is [`d` STRING NOT NULL]."),
                TestSpec.of(
                        "ALTER MATERIALIZED TABLE t1 AS SELECT * FROM t1",
                        "ALTER MATERIALIZED TABLE for a table is not allowed"));
    }

    private static List<TestSpec> alterWithInvalidSchema() {
        return List.of(
                TestSpec.of(
                        "ALTER MATERIALIZED TABLE base_mtbl ADD WATERMARK for invalid_column as invalid_column",
                        "Failed to execute ALTER MATERIALIZED TABLE statement.\n"
                                + "Invalid column name 'invalid_column' for rowtime attribute in watermark declaration. "
                                + "Available columns are: [a, b, c, d]"),
                TestSpec.of(
                        "ALTER MATERIALIZED TABLE base_mtbl_with_watermark ADD WATERMARK for t as current_timestamp - INTERVAL '2' SECOND",
                        "Failed to execute ALTER MATERIALIZED TABLE statement.\n"
                                + "The current materialized table has already defined the watermark strategy "
                                + "`t` AS CURRENT_TIMESTAMP - INTERVAL '5' SECOND. You might want to drop it before adding a new one."),
                TestSpec.of(
                        "ALTER MATERIALIZED TABLE base_mtbl ADD `physical_not_used_in_query` BIGINT NOT NULL",
                        "Failed to execute ALTER MATERIALIZED TABLE statement.\n"
                                + "Invalid schema change. All persisted (physical and metadata) "
                                + "columns in the schema part need to be present in the query part.\n"
                                + "However, physical column `physical_not_used_in_query` could not be found in the query."),
                TestSpec.of(
                        "ALTER MATERIALIZED TABLE base_mtbl ADD `a` BIGINT NOT NULL",
                        "Failed to execute ALTER MATERIALIZED TABLE statement.\n"
                                + "Column `a` already exists in the materialized table."),
                TestSpec.of(
                        "ALTER MATERIALIZED TABLE base_mtbl ADD `q` AS `non_existing_column` + 2",
                        "Failed to execute ALTER MATERIALIZED TABLE statement.\n"
                                + "Invalid expression for computed column 'q'."),
                TestSpec.of(
                        "ALTER MATERIALIZED TABLE base_mtbl ADD PRIMARY KEY(c) NOT ENFORCED",
                        "Failed to execute ALTER MATERIALIZED TABLE statement.\n"
                                + "The current materialized table has already defined the primary key constraint [`a`]. "
                                + "You might want to drop it before adding a new one."),
                TestSpec.of(
                        "ALTER MATERIALIZED TABLE base_mtbl ADD (`q` AS current_timestamp AFTER `q2`, `q2` AS current_timestamp AFTER `q`)",
                        "Failed to execute ALTER MATERIALIZED TABLE statement.\n"
                                + "Referenced column `q2` by 'AFTER' does not exist in the table."),
                TestSpec.of(
                        "ALTER MATERIALIZED TABLE base_mtbl ADD `m1` INT METADATA",
                        "Failed to execute ALTER MATERIALIZED TABLE statement.\n"
                                + "Invalid schema change. All persisted (physical and metadata) "
                                + "columns in the schema part need to be present in the query part.\n"
                                + "However, metadata persisted column `m1` could not be found in the query."));
    }

    private static List<TestSpec> createWithInvalidSchema() {
        return List.of(
                TestSpec.of(
                        "CREATE MATERIALIZED TABLE users_shops (shop_id)"
                                + " FRESHNESS = INTERVAL '30' SECOND"
                                + " AS SELECT 1 AS shop_id, 2 AS user_id",
                        "The number of columns in the column list must match the number of columns in the source schema."),
                TestSpec.of(
                        "CREATE MATERIALIZED TABLE users_shops (id, name, address)"
                                + " FRESHNESS = INTERVAL '30' SECOND"
                                + " AS SELECT 1 AS shop_id, 2 AS user_id",
                        "The number of columns in the column list must match the number of columns in the source schema."),
                TestSpec.of(
                        "CREATE MATERIALIZED TABLE users_shops (id, name)"
                                + " FRESHNESS = INTERVAL '30' SECOND"
                                + " AS SELECT 1 AS shop_id, 2 AS user_id",
                        "Column 'id' not found in the source schema."),
                TestSpec.of(
                        "CREATE MATERIALIZED TABLE users_shops (shop_id STRING, user_id STRING)"
                                + " FRESHNESS = INTERVAL '30' SECOND"
                                + " AS SELECT 1 AS shop_id, 2 AS user_id",
                        "Incompatible types for sink column 'shop_id' at position 0. The source column has type 'INT NOT NULL', "
                                + "while the target column has type 'STRING'."),
                TestSpec.of(
                        "CREATE MATERIALIZED TABLE users_shops (shop_id INT, WATERMARK FOR ts AS `ts` - INTERVAL '5' SECOND)"
                                + " FRESHNESS = INTERVAL '30' SECOND"
                                + " AS SELECT 1 AS shop_id, 2 AS user_id",
                        "The rowtime attribute field 'ts' is not defined in the table schema, at line 1, column 67\n"
                                + "Available fields: ['shop_id', 'user_id']"),
                TestSpec.of(
                        "CREATE MATERIALIZED TABLE users_shops (shop_id INT, user_id INT, PRIMARY KEY(id) NOT ENFORCED)"
                                + " FRESHNESS = INTERVAL '30' SECOND"
                                + " AS SELECT 1 AS shop_id, 2 AS user_id",
                        "Primary key column 'id' is not defined in the schema at line 1, column 78"),
                TestSpec.of(
                        "CREATE MATERIALIZED TABLE users_shops (WATERMARK FOR ts AS ts - INTERVAL '2' SECOND)"
                                + " FRESHNESS = INTERVAL '30' SECOND"
                                + " AS SELECT 1 AS shop_id, 2 AS user_id",
                        "The rowtime attribute field 'ts' is not defined in the table schema, at line 1, column 54\n"
                                + "Available fields: ['shop_id', 'user_id']"),
                TestSpec.of(
                        "CREATE MATERIALIZED TABLE users_shops (a INT, b INT)"
                                + " FRESHNESS = INTERVAL '30' SECOND"
                                + " AS SELECT 1 AS shop_id, 2 AS user_id",
                        "Invalid as physical column 'a' is defined in the DDL, but is not used in a query column."),
                TestSpec.of(
                        "CREATE MATERIALIZED TABLE users_shops (shop_id INT, b INT)"
                                + " FRESHNESS = INTERVAL '30' SECOND"
                                + " AS SELECT 1 AS shop_id, 2 AS user_id",
                        "Invalid as physical column 'b' is defined in the DDL, but is not used in a query column."),
                // test unsupported constraint
                TestSpec.of(
                        "CREATE MATERIALIZED TABLE mtbl1 (\n"
                                + "   CONSTRAINT ct1 UNIQUE(a) NOT ENFORCED"
                                + ")\n"
                                + "FRESHNESS = INTERVAL '30' SECOND\n"
                                + "AS SELECT * FROM t1",
                        SqlValidateException.class,
                        "UNIQUE constraint is not supported yet"),
                // test primary key not defined in source table
                TestSpec.of(
                        "CREATE MATERIALIZED TABLE mtbl1 (\n"
                                + "   CONSTRAINT ct1 PRIMARY KEY(e) NOT ENFORCED"
                                + ")\n"
                                + "FRESHNESS = INTERVAL '30' SECOND\n"
                                + "AS SELECT * FROM t1",
                        "Primary key column 'e' is not defined in the schema at line 2, column 31"),
                // test primary key with nullable source column
                TestSpec.of(
                        "CREATE MATERIALIZED TABLE mtbl1 (\n"
                                + "   CONSTRAINT ct1 PRIMARY KEY(d) NOT ENFORCED"
                                + ")\n"
                                + "FRESHNESS = INTERVAL '30' SECOND\n"
                                + "AS SELECT * FROM t1",
                        "Invalid primary key 'ct1'. Column 'd' is nullable."));
    }

    private static List<TestSpec> createWithInvalidPartitions() {
        return List.of(
                TestSpec.of(
                        "CREATE MATERIALIZED TABLE mtbl1\n"
                                + "PARTITIONED BY (a, e)\n"
                                + "FRESHNESS = INTERVAL '30' SECOND\n"
                                + "REFRESH_MODE = FULL\n"
                                + "AS SELECT * FROM t1",
                        "Partition column 'e' not defined in the query's schema. Available columns: ['a', 'b', 'c', 'd']."),
                TestSpec.of(
                        "CREATE MATERIALIZED TABLE mtbl1\n"
                                + "PARTITIONED BY (b, c)\n"
                                + "WITH (\n"
                                + " 'partition.fields.ds.date-formatter' = 'yyyy-MM-dd'\n"
                                + ")\n"
                                + "FRESHNESS = INTERVAL '30' SECOND\n"
                                + "REFRESH_MODE = FULL\n"
                                + "AS SELECT * FROM t3",
                        "Column 'ds' referenced by materialized table option 'partition.fields.ds.date-formatter' isn't a partition column. Available partition columns: ['b', 'c']."),
                TestSpec.of(
                        "CREATE MATERIALIZED TABLE mtbl1\n"
                                + "WITH (\n"
                                + " 'partition.fields.c.date-formatter' = 'yyyy-MM-dd'\n"
                                + ")\n"
                                + "FRESHNESS = INTERVAL '30' SECOND\n"
                                + "REFRESH_MODE = FULL\n"
                                + "AS SELECT * FROM t3",
                        "Column 'c' referenced by materialized table option 'partition.fields.c.date-formatter' isn't a partition column. Available partition columns: ['']."),
                TestSpec.of(
                        "CREATE MATERIALIZED TABLE mtbl1\n"
                                + "PARTITIONED BY (b, c)\n"
                                + "WITH (\n"
                                + " 'partition.fields.c.date-formatter' = 'yyyy-MM-dd'\n"
                                + ")\n"
                                + "FRESHNESS = INTERVAL '30' SECOND\n"
                                + "REFRESH_MODE = FULL\n"
                                + "AS SELECT * FROM t3",
                        "Materialized table option 'partition.fields.c.date-formatter' only supports referring to char, varchar and string type partition column. Column `c` type is INT."));
    }

    private static List<TestSpec> createWithInvalidFreshness() {
        return List.of(
                TestSpec.of(
                        "CREATE MATERIALIZED TABLE mtbl1\n"
                                + "FRESHNESS = INTERVAL '40' MINUTE\n"
                                + "REFRESH_MODE = FULL\n"
                                + "AS SELECT * FROM t1",
                        "In full refresh mode, only freshness that are factors of 60 are currently supported when the time unit is MINUTE."),
                TestSpec.of(
                        "CREATE MATERIALIZED TABLE mtbl1\n"
                                + "FRESHNESS = INTERVAL '40' MINUTE\n"
                                + "AS SELECT * FROM t1",
                        "In full refresh mode, only freshness that are factors of 60 are currently supported when the time unit is MINUTE."),
                TestSpec.of(
                        "CREATE MATERIALIZED TABLE mtbl1\n"
                                + "FRESHNESS = INTERVAL -'30' SECOND\n"
                                + "REFRESH_MODE = FULL\n"
                                + "AS SELECT * FROM t1",
                        "Materialized table freshness doesn't support negative value."),
                TestSpec.of(
                        "CREATE MATERIALIZED TABLE mtbl1\n"
                                + "FRESHNESS = INTERVAL '30' YEAR\n"
                                + "AS SELECT * FROM t1",
                        "Materialized table freshness only support SECOND, MINUTE, HOUR, DAY as the time unit."),
                TestSpec.of(
                        "CREATE MATERIALIZED TABLE mtbl1\n"
                                + "FRESHNESS = INTERVAL '30' DAY TO HOUR\n"
                                + "AS SELECT * FROM t1",
                        "Materialized table freshness only support SECOND, MINUTE, HOUR, DAY as the time unit."));
    }

    private static Collection<TestSpec> alterSuccessCase() {
        final Collection<TestSpec> list = new ArrayList<>();
        list.add(
                TestSpec.withExpectedSchema(
                        "ALTER MATERIALIZED TABLE base_mtbl ADD (`q` AS current_timestamp AFTER `b`, WATERMARK FOR `q` AS `q` - INTERVAL '1' SECOND)",
                        "(\n"
                                + "  `a` BIGINT NOT NULL,\n"
                                + "  `b` STRING,\n"
                                + "  `q` AS [CURRENT_TIMESTAMP],\n"
                                + "  `c` INT,\n"
                                + "  `d` STRING,\n"
                                + "  WATERMARK FOR `q` AS [`q` - INTERVAL '1' SECOND],\n"
                                + "  CONSTRAINT `ct1` PRIMARY KEY (`a`) NOT ENFORCED\n"
                                + ")"));
        list.add(
                TestSpec.withExpectedSchema(
                        "ALTER MATERIALIZED TABLE base_mtbl ADD (`q` AS current_timestamp FIRST, `q2` AS current_time FIRST)",
                        "(\n"
                                + "  `q2` AS [CURRENT_TIME],\n"
                                + "  `q` AS [CURRENT_TIMESTAMP],\n"
                                + "  `a` BIGINT NOT NULL,\n"
                                + "  `b` STRING,\n"
                                + "  `c` INT,\n"
                                + "  `d` STRING,\n"
                                + "  CONSTRAINT `ct1` PRIMARY KEY (`a`) NOT ENFORCED\n"
                                + ")"));
        list.add(
                TestSpec.withExpectedSchema(
                        "ALTER MATERIALIZED TABLE base_mtbl_without_constraint ADD ("
                                + "    `c1` AS current_timestamp FIRST, "
                                + "    `topic` STRING METADATA VIRTUAL COMMENT 'kafka topic' AFTER `b`, "
                                + "    WATERMARK FOR `c1` AS `c1` - INTERVAL '1' SECOND, "
                                + "    PRIMARY KEY(`a`) NOT ENFORCED)",
                        "(\n"
                                + "  `c1` AS [CURRENT_TIMESTAMP],\n"
                                + "  `a` BIGINT NOT NULL,\n"
                                + "  `b` STRING,\n"
                                + "  `topic` METADATA VIRTUAL COMMENT 'kafka topic',\n"
                                + "  `c` INT,\n"
                                + "  `d` STRING,\n"
                                + "  WATERMARK FOR `c1` AS [`c1` - INTERVAL '1' SECOND],\n"
                                + "  CONSTRAINT `PK_a` PRIMARY KEY (`a`) NOT ENFORCED\n"
                                + ")"));
        return list;
    }

    private static Collection<Arguments> testDataWithDifferentSchemasSuccessCase() {
        final Collection<Arguments> list = new ArrayList<>();
        list.addAll(createOrAlter(CREATE_OPERATION));
        list.addAll(createOrAlter(CREATE_OR_ALTER_OPERATION));
        return list;
    }

    private static List<Arguments> createOrAlter(final String operation) {
        return List.of(
                Arguments.of(
                        operation
                                + "MATERIALIZED TABLE users_shops (shop_id, user_id)"
                                + " FRESHNESS = INTERVAL '30' SECOND"
                                + " AS SELECT 1 AS shop_id, 2 AS user_id",
                        ResolvedSchema.of(
                                Column.physical("shop_id", DataTypes.INT().notNull()),
                                Column.physical("user_id", DataTypes.INT().notNull()))),
                Arguments.of(
                        operation
                                + "MATERIALIZED TABLE users_shops"
                                + " FRESHNESS = INTERVAL '30' SECOND"
                                + " AS SELECT CAST(1 AS DOUBLE) AS shop_id, CAST(2 AS STRING) AS user_id",
                        ResolvedSchema.of(
                                Column.physical("shop_id", DataTypes.DOUBLE().notNull()),
                                Column.physical("user_id", DataTypes.STRING().notNull()))),
                Arguments.of(
                        operation
                                + "MATERIALIZED TABLE users_shops (user_id, shop_id)"
                                + " FRESHNESS = INTERVAL '30' SECOND"
                                + " AS SELECT 1 AS shop_id, 2 AS user_id",
                        ResolvedSchema.of(
                                Column.physical("user_id", DataTypes.INT().notNull()),
                                Column.physical("shop_id", DataTypes.INT().notNull()))),
                Arguments.of(
                        operation
                                + "MATERIALIZED TABLE users_shops (user_id INT, shop_id BIGINT)"
                                + " FRESHNESS = INTERVAL '30' SECOND"
                                + " AS SELECT 1 AS shop_id, 2 AS user_id",
                        ResolvedSchema.of(
                                Column.physical("shop_id", DataTypes.BIGINT()),
                                Column.physical("user_id", DataTypes.INT()))),
                Arguments.of(
                        operation
                                + "MATERIALIZED TABLE users_shops (user_id INT, shop_id BIGINT, PRIMARY KEY(user_id) NOT ENFORCED)"
                                + " FRESHNESS = INTERVAL '30' SECOND"
                                + " AS SELECT 1 AS shop_id, 2 AS user_id",
                        new ResolvedSchema(
                                List.of(
                                        Column.physical("shop_id", DataTypes.BIGINT()),
                                        Column.physical("user_id", DataTypes.INT().notNull())),
                                List.of(),
                                org.apache.flink.table.catalog.UniqueConstraint.primaryKey(
                                        "PK_user_id", List.of("user_id")),
                                List.of())),
                Arguments.of(
                        operation
                                + "MATERIALIZED TABLE users_shops (PRIMARY KEY(user_id) NOT ENFORCED)"
                                + " FRESHNESS = INTERVAL '30' SECOND"
                                + " AS SELECT 1 AS shop_id, 2 AS user_id",
                        new ResolvedSchema(
                                List.of(
                                        Column.physical("shop_id", DataTypes.INT().notNull()),
                                        Column.physical("user_id", DataTypes.INT().notNull())),
                                List.of(),
                                org.apache.flink.table.catalog.UniqueConstraint.primaryKey(
                                        "PK_user_id", List.of("user_id")),
                                List.of())));
    }

    /** Boilerplate CatalogMaterializedTable builder for tests. */
    private CatalogMaterializedTable.Builder getDefaultMaterializedTableBuilder() {
        return CatalogMaterializedTable.newBuilder()
                .schema(
                        Schema.newBuilder()
                                .column("a", DataTypes.BIGINT().notNull())
                                .column("b", DataTypes.VARCHAR(Integer.MAX_VALUE))
                                .column("c", DataTypes.INT())
                                .column("d", DataTypes.VARCHAR(Integer.MAX_VALUE))
                                .primaryKeyNamed("ct1", Collections.singletonList("a"))
                                .build())
                .comment("materialized table comment")
                .options(Map.of("connector", "filesystem", "format", "json"))
                .partitionKeys(Arrays.asList("a", "d"))
                .originalQuery("SELECT *\nFROM `t1`")
                .expandedQuery(
                        "SELECT `t1`.`a`, `t1`.`b`, `t1`.`c`, `t1`.`d`\n"
                                + "FROM `builtin`.`default`.`t1` AS `t1`");
    }

    private void createMaterializedTableInCatalog(String sql, String materializedTableName)
            throws TableAlreadyExistException, DatabaseNotExistException {
        final ObjectPath objectPath =
                new ObjectPath(catalogManager.getCurrentDatabase(), materializedTableName);
        final CreateMaterializedTableOperation operation = createMaterializedTableOperation(sql);
        catalog.createTable(objectPath, operation.getCatalogMaterializedTable(), true);
    }

    private ResolvedCatalogMaterializedTable createResolvedCatalogMaterializedTable(String sql) {
        return createMaterializedTableOperation(sql).getCatalogMaterializedTable();
    }

    private CreateMaterializedTableOperation createMaterializedTableOperation(String sql) {
        final Operation operation = parse(sql);
        assertThat(operation).isInstanceOf(CreateMaterializedTableOperation.class);
        return (CreateMaterializedTableOperation) operation;
    }

    private static class TestSpec {
        private final String sql;
        private final Class<?> expectedException;
        private final String errMessage;
        private final String expectedSchema;

        private TestSpec(String sql, Class<?> expectedException, String errMessage) {
            this.sql = sql;
            this.expectedException = expectedException;
            this.errMessage = errMessage;
            this.expectedSchema = null;
        }

        private TestSpec(String sql, String expectedSchema) {
            this.sql = sql;
            this.expectedException = null;
            this.errMessage = null;
            this.expectedSchema = expectedSchema;
        }

        public static TestSpec of(String sql, Class<?> expectedException, String errMessage) {
            return new TestSpec(sql, expectedException, errMessage);
        }

        public static TestSpec of(String sql, String errMessage) {
            return of(sql, ValidationException.class, errMessage);
        }

        public static TestSpec withExpectedSchema(String sql, String expectedSchema) {
            return new TestSpec(sql, expectedSchema);
        }

        @Override
        public String toString() {
            return sql;
        }
    }
}
